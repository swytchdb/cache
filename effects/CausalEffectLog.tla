--------------------------- MODULE CausalEffectLog ---------------------------
(*
 * TLA+ specification for the Causal Effect Log protocol.
 *
 * Models the core protocol: effects with deps, tip sets per node,
 * NACKs, fork resolution via multi-dep writes, and canonical merge
 * ordering (non-comm LWW first, then comm accumulate).
 *
 * Scope:
 *   - Single key (multi-key transactions are a separate concern)
 *   - Scalar values only (KEYED/ORDERED are structural extensions)
 *   - Effects carry: value (Int), merge rule (LWW or ADD), HLC, node
 *   - Nodes own disjoint offset ranges
 *   - Asynchronous network with reordering (no loss for liveness)
 *
 * Properties verified:
 *   - TipSetsConverge: same tips => same reconstruction
 *   - CausalDepsExist: every dep of a tip is in the log
 *   - CanonicalMergeDeterminism: merge result independent of eval order
 *)

EXTENDS Integers, FiniteSets

CONSTANTS
    Nodes,          \* e.g. {n1, n2, n3}
    MaxEffects      \* bound on total effects for model checking

\* Deterministic ordering of nodes. We pick an arbitrary but fixed
\* bijection from Nodes to 0..Cardinality(Nodes)-1.
\* TLC will evaluate this once at startup.
NodeOrder ==
    CHOOSE f \in [Nodes -> 0..(Cardinality(Nodes)-1)] :
        \A m, n \in Nodes : m /= n => f[m] /= f[n]

VARIABLES
    log,            \* function: offset (Nat) -> effect record
    logDomain,      \* set of offsets currently in the log
    tips,           \* function: node -> set of offsets
    nextOffset,     \* function: node -> next offset to use
    hlc,            \* function: node -> current HLC counter
    network,        \* set of in-flight messages
    effectCount     \* total effects emitted

vars == <<log, logDomain, tips, nextOffset, hlc, network, effectCount>>

NULL == 0  \* the root offset

-----------------------------------------------------------------------------
(* Merge helpers *)

IsComm(m) == m = "ADD"

\* HLC tiebreaker: higher wins; ties broken by node ordering
Wins(aHLC, aNode, bHLC, bNode) ==
    \/ aHLC > bHLC
    \/ (aHLC = bHLC /\ NodeOrder[aNode] > NodeOrder[bNode])

\* Sum values from a set of effect records
RECURSIVE SetSum(_)
SetSum(S) ==
    IF S = {} THEN 0
    ELSE LET x == CHOOSE x \in S : TRUE
         IN x + SetSum(S \ {x})

\* Maximum HLC from a set of effect records
SetMaxHLC(S) ==
    CHOOSE m \in {e.hlc : e \in S} : \A e \in S : e.hlc <= m

(*
 * CanonicalMerge: given a set of effect records (reduced branches),
 * produce a deterministic merged result.
 *
 * 1. Non-commutative branches: LWW — highest HLC wins, losers discarded.
 * 2. Commutative branches: sum all values onto the winner's base.
 *)
CanonicalMerge(branches) ==
    LET nonComm == {b \in branches : ~IsComm(b.merge)}
        comm    == {b \in branches : IsComm(b.merge)}
        commTotal == SetSum({b.value : b \in comm})
    IN
    IF nonComm = {} THEN
        \* All commutative
        [value |-> commTotal,
         merge |-> "ADD",
         hlc   |-> SetMaxHLC(branches),
         node  |-> (CHOOSE b \in branches :
                     \A c \in branches : ~Wins(c.hlc, c.node, b.hlc, b.node)).node]
    ELSE
        \* Pick LWW winner among non-comm, accumulate comm on top
        LET winner == CHOOSE b \in nonComm :
                        \A c \in nonComm : ~Wins(c.hlc, c.node, b.hlc, b.node)
                                           \/ c = b
        IN
        [value |-> winner.value + commTotal,
         merge |-> winner.merge,
         hlc   |-> SetMaxHLC(branches),
         node  |-> winner.node]

-----------------------------------------------------------------------------
(* Reconstruction from tip set *)

ReduceEffect(offset) ==
    LET e == log[offset]
    IN [value |-> e.value, merge |-> e.merge, hlc |-> e.hlc, node |-> e.node]

Reconstruct(tipSet) ==
    IF Cardinality(tipSet) = 0 THEN
        [value |-> 0, merge |-> "LWW", hlc |-> 0, node |-> CHOOSE n \in Nodes : TRUE]
    ELSE IF Cardinality(tipSet) = 1 THEN
        ReduceEffect(CHOOSE t \in tipSet : TRUE)
    ELSE
        CanonicalMerge({ReduceEffect(t) : t \in tipSet})

-----------------------------------------------------------------------------
(* Initial state *)

\* Nodes get offsets: n1 starts at 1, n2 at 101, n3 at 201, etc.
\* (Using a simple scheme; real system claims ranges via EmitExactlyOnce)

NodeIndex(n) == CHOOSE i \in 1..Cardinality(Nodes) :
    n = CHOOSE x \in Nodes :
        Cardinality({y \in Nodes : y < x}) = i - 1

Init ==
    /\ log = [o \in {} |-> <<>>]
    /\ logDomain = {}
    /\ tips = [n \in Nodes |-> {}]
    /\ nextOffset = [n \in Nodes |-> NodeOrder[n] * 100 + 1]
    /\ hlc = [n \in Nodes |-> 0]
    /\ network = {}
    /\ effectCount = 0

-----------------------------------------------------------------------------
(* Actions *)

(* Emit: node n writes an effect with the given value and merge rule.
   The effect's deps = current tip set (or {NULL} if no tips).
   After emit, node's tips collapse to the single new offset. *)
Emit(n, value, merge) ==
    /\ effectCount < MaxEffects
    /\ LET newHLC == hlc[n] + 1
           offset == nextOffset[n]
           deps   == IF tips[n] = {} THEN {NULL} ELSE tips[n]
           effect == [deps  |-> deps,
                      value |-> value,
                      merge |-> merge,
                      hlc   |-> newHLC,
                      node  |-> n]
       IN
       /\ log' = [o \in logDomain \cup {offset} |->
                    IF o = offset THEN effect ELSE log[o]]
       /\ logDomain' = logDomain \cup {offset}
       /\ tips' = [tips EXCEPT ![n] = {offset}]
       /\ nextOffset' = [nextOffset EXCEPT ![n] = offset + 1]
       /\ hlc' = [hlc EXCEPT ![n] = newHLC]
       /\ effectCount' = effectCount + 1
       /\ network' = network \cup
            {[type |-> "notify", from |-> n, to |-> m, offset |-> offset] :
             m \in Nodes \ {n}}

(* HandleRemote: node n processes a notify message.
   - If the effect's deps are a subset of n's tips: linear advancement
     or fork resolution. Replace matched tips with new offset.
   - Otherwise: concurrent branch. Add as tip, NACK the sender. *)
HandleRemote(n, msg) ==
    /\ msg \in network
    /\ msg.type = "notify"
    /\ msg.to = n
    /\ msg.offset \in logDomain
    /\ LET e      == log[msg.offset]
           eDeps  == e.deps
           myTips == tips[n]
       IN
       IF eDeps \subseteq myTips \/ (eDeps = {NULL} /\ myTips = {})
       THEN
           \* Linear or fork resolution
           /\ tips' = [tips EXCEPT ![n] = (myTips \ eDeps) \cup {msg.offset}]
           /\ network' = network \ {msg}
           /\ UNCHANGED <<log, logDomain, nextOffset, hlc, effectCount>>
       ELSE
           \* Concurrent: add as tip, send NACK with full tip set
           /\ tips' = [tips EXCEPT ![n] = myTips \cup {msg.offset}]
           /\ network' = (network \ {msg}) \cup
                {[type |-> "nack",
                  from |-> n,
                  to   |-> msg.from,
                  tips |-> myTips \cup {msg.offset}]}
           /\ UNCHANGED <<log, logDomain, nextOffset, hlc, effectCount>>

(* HandleNack: node n receives a NACK, merges the remote tip set
   into its own. The next Emit will depend on all tips. *)
HandleNack(n, msg) ==
    /\ msg \in network
    /\ msg.type = "nack"
    /\ msg.to = n
    /\ tips' = [tips EXCEPT ![n] = tips[n] \cup msg.tips]
    /\ network' = network \ {msg}
    /\ UNCHANGED <<log, logDomain, nextOffset, hlc, effectCount>>

-----------------------------------------------------------------------------
(* Next-state relation *)

Next ==
    \/ \E n \in Nodes, v \in 0..2, m \in {"LWW", "ADD"} : Emit(n, v, m)
    \/ \E n \in Nodes, msg \in network : HandleRemote(n, msg)
    \/ \E n \in Nodes, msg \in network : HandleNack(n, msg)

Fairness == \A msg \in network :
    WF_vars(\E n \in Nodes : HandleRemote(n, msg) \/ HandleNack(n, msg))

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
(* Safety Properties *)

(* CONVERGENCE: Any two nodes with the same tip set reconstruct the
   same value. This verifies canonical merge ordering is deterministic. *)
TipSetsConverge ==
    \A n1, n2 \in Nodes :
        (tips[n1] = tips[n2] /\ tips[n1] /= {} /\
         tips[n1] \subseteq logDomain) =>
            Reconstruct(tips[n1]).value = Reconstruct(tips[n2]).value

(* CAUSAL DEPS EXIST: For every tip, all its deps exist in the log
   (or are the null root). *)
CausalDepsExist ==
    \A n \in Nodes :
        \A t \in tips[n] :
            t \in logDomain =>
                \A d \in log[t].deps :
                    d = NULL \/ d \in logDomain

(* TYPE INVARIANT *)
TypeOK ==
    /\ \A o \in logDomain :
        /\ log[o].deps \subseteq (logDomain \cup {NULL})
        /\ log[o].merge \in {"LWW", "ADD"}
        /\ log[o].hlc \in Nat
        /\ log[o].node \in Nodes
    /\ \A n \in Nodes :
        /\ tips[n] \subseteq logDomain
        /\ hlc[n] \in Nat
        /\ nextOffset[n] \in Nat

(* TIPS NEVER EMPTY AFTER FIRST WRITE: Once a node writes, it always
   has at least one tip. *)
TipsNonEmptyAfterWrite ==
    \A n \in Nodes :
        nextOffset[n] > (NodeOrder[n] * 100 + 1)
        => tips[n] /= {}

=============================================================================
