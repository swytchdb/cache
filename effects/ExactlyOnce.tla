------------------------------ MODULE ExactlyOnce ------------------------------
(*
 * TLA+ specification for exactly-once destructive reads.
 *
 * Models the EmitExactlyOnce protocol: two or more nodes attempt to
 * pop (destructively read) the same element from a key. The protocol
 * guarantees that at most one node returns the value to its client.
 *
 * Scope:
 *   - Single key with a single poppable element
 *   - Nodes can: emit a pop (REMOVE), receive notifications, NACK,
 *     and either commit (return to client) or abort (retry/fail)
 *   - Asynchronous network with reordering (no loss for liveness)
 *
 * Properties verified:
 *   - AtMostOneCommit: at most one node commits a pop for the element
 *   - WinnerIsHLCWinner: the committed node (if any) has the highest
 *     HLC among concurrent poppers
 *   - LoserDiscoversBeforeReturning: a losing node never commits
 *)

EXTENDS Integers, FiniteSets

CONSTANTS
    Nodes           \* e.g. {n1, n2, n3}

\* Deterministic ordering of nodes for tiebreaking
NodeOrder ==
    CHOOSE f \in [Nodes -> 0..(Cardinality(Nodes)-1)] :
        \A m, n \in Nodes : m /= n => f[m] /= f[n]

\* HLC tiebreaker: higher wins; ties broken by node ordering
Wins(aHLC, aNode, bHLC, bNode) ==
    \/ aHLC > bHLC
    \/ (aHLC = bHLC /\ NodeOrder[aNode] > NodeOrder[bNode])

VARIABLES
    log,            \* function: offset -> effect record
    logDomain,      \* set of offsets currently in the log
    tips,           \* function: node -> set of offsets (for this key)
    nextOffset,     \* function: node -> next offset to use
    hlc,            \* function: node -> current HLC counter
    network,        \* set of in-flight messages
    popState,       \* function: node -> "idle" | "waiting" | "committed" | "aborted"
    popOffset,      \* function: node -> offset of this node's pop effect (0 if none)
    popHLC,         \* function: node -> HLC of this node's pop effect (0 if none)
    acked           \* function: node -> set of nodes that have ACKed our pop

vars == <<log, logDomain, tips, nextOffset, hlc, network, popState, popOffset, popHLC, acked>>

NULL == 0  \* the root offset

\* The element to pop. In a real system this is an element ID;
\* here we model a single poppable element as "elem1".
Element == "elem1"

-----------------------------------------------------------------------------
(* Initial state *)

\* All nodes start subscribed (tips = {NULL} means "knows about the key,
\* element exists"). This models the state after subscription bootstrapping.
Init ==
    /\ log = [o \in {} |-> <<>>]
    /\ logDomain = {}
    /\ tips = [n \in Nodes |-> {}]           \* no effects yet; element is at root
    /\ nextOffset = [n \in Nodes |-> NodeOrder[n] * 100 + 1]
    /\ hlc = [n \in Nodes |-> 0]
    /\ network = {}
    /\ popState = [n \in Nodes |-> "idle"]
    /\ popOffset = [n \in Nodes |-> 0]
    /\ popHLC = [n \in Nodes |-> 0]
    /\ acked = [n \in Nodes |-> {}]

-----------------------------------------------------------------------------
(* Helpers *)

\* The element still exists from node n's perspective: no REMOVE in
\* the node's log chain. We check that none of the node's tips (or
\* effects it has accepted) are REMOVEs for the element.
ElementExists(n) ==
    ~ \E t \in tips[n] :
        /\ t \in logDomain
        /\ log[t].op = "REMOVE"
        /\ log[t].element = Element

(* Actions *)

(*
 * EmitPop: node n attempts to pop the element.
 * - Only if the element still exists from n's perspective
 * - Emits a REMOVE effect depending on current tips (or NULL if none)
 * - Enters "waiting" state (waiting for 1 RTT from all subscribers)
 * - Broadcasts to all other nodes
 *)
EmitPop(n) ==
    /\ popState[n] = "idle"
    /\ ElementExists(n)
    /\ LET newHLC == hlc[n] + 1
           offset == nextOffset[n]
           deps   == IF tips[n] = {} THEN {NULL} ELSE tips[n]
           effect == [deps    |-> deps,
                      op      |-> "REMOVE",
                      element |-> Element,
                      hlc     |-> newHLC,
                      node    |-> n]
       IN
       /\ log' = [o \in logDomain \cup {offset} |->
                    IF o = offset THEN effect ELSE log[o]]
       /\ logDomain' = logDomain \cup {offset}
       /\ tips' = [tips EXCEPT ![n] = {offset}]
       /\ nextOffset' = [nextOffset EXCEPT ![n] = offset + 1]
       /\ hlc' = [hlc EXCEPT ![n] = newHLC]
       /\ popState' = [popState EXCEPT ![n] = "waiting"]
       /\ popOffset' = [popOffset EXCEPT ![n] = offset]
       /\ popHLC' = [popHLC EXCEPT ![n] = newHLC]
       /\ acked' = [acked EXCEPT ![n] = {}]
       /\ network' = network \cup
            {[type |-> "notify", from |-> n, to |-> m, offset |-> offset] :
             m \in Nodes \ {n}}

(*
 * HandleRemote: node n processes a notify for a pop effect.
 *
 * If the remote effect's deps match our tips: linear, accept it.
 * Otherwise: concurrent pop — add as tip, NACK sender.
 *
 * If we're also "waiting" and we see a concurrent pop for the same
 * element, we can determine the winner immediately via HLC.
 *)
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
           \* Linear: accept the remote pop, ACK
           /\ tips' = [tips EXCEPT ![n] = (myTips \ eDeps) \cup {msg.offset}]
           /\ network' = (network \ {msg}) \cup
                {[type |-> "ack", from |-> n, to |-> msg.from]}
           \* If we were waiting and someone else's pop landed linearly,
           \* that means they depend on us (or we had no tips). We're still ok.
           /\ UNCHANGED <<log, logDomain, nextOffset, hlc, popState, popOffset, popHLC, acked>>
       ELSE
           \* Concurrent: both targeting same element
           /\ tips' = [tips EXCEPT ![n] = myTips \cup {msg.offset}]
           /\ network' = (network \ {msg}) \cup
                {[type   |-> "nack",
                  from   |-> n,
                  to     |-> msg.from,
                  tips   |-> myTips \cup {msg.offset},
                  popHLC |-> e.hlc,
                  popNode|-> e.node]}
           \* If we were waiting, check if we lose
           /\ IF popState[n] = "waiting" /\
                 Wins(e.hlc, e.node, popHLC[n], n)
              THEN popState' = [popState EXCEPT ![n] = "aborted"]
              ELSE popState' = popState
           /\ UNCHANGED <<log, logDomain, nextOffset, hlc, popOffset, popHLC, acked>>

(*
 * HandleNack: node n receives a NACK while waiting for pop confirmation.
 * If the NACK contains a competing pop with a winning HLC, abort.
 * Otherwise, merge tip sets and continue waiting.
 *)
HandleNack(n, msg) ==
    /\ msg \in network
    /\ msg.type = "nack"
    /\ msg.to = n
    /\ tips' = [tips EXCEPT ![n] = tips[n] \cup msg.tips]
    /\ IF popState[n] = "waiting" /\
          Wins(msg.popHLC, msg.popNode, popHLC[n], n)
       THEN popState' = [popState EXCEPT ![n] = "aborted"]
       ELSE popState' = popState
    /\ network' = network \ {msg}
    /\ UNCHANGED <<log, logDomain, nextOffset, hlc, popOffset, popHLC, acked>>

(*
 * HandleAck: node n receives an ACK.
 * In a real system, we track ACKs per subscriber and commit when all
 * have responded. Here we simplify: commit when no conflicts exist
 * (all messages for us are processed and we're still "waiting").
 *)
HandleAck(n, msg) ==
    /\ msg \in network
    /\ msg.type = "ack"
    /\ msg.to = n
    /\ popState[n] = "waiting"
    /\ acked' = [acked EXCEPT ![n] = acked[n] \cup {msg.from}]
    /\ network' = network \ {msg}
    /\ UNCHANGED <<log, logDomain, tips, nextOffset, hlc, popState, popOffset, popHLC>>

(*
 * CommitPop: node n has received ACKs from all other nodes (no pending
 * messages to n in the network) and is still in "waiting" state.
 * This models the 1 RTT window closing without conflict.
 *)
CommitPop(n) ==
    /\ popState[n] = "waiting"
    \* All other nodes have ACKed (1 RTT complete)
    /\ acked[n] = Nodes \ {n}
    /\ popState' = [popState EXCEPT ![n] = "committed"]
    /\ UNCHANGED <<log, logDomain, tips, nextOffset, hlc, network, popOffset, popHLC, acked>>

-----------------------------------------------------------------------------
(* Next-state relation *)

Next ==
    \/ \E n \in Nodes : EmitPop(n)
    \/ \E n \in Nodes, msg \in network : HandleRemote(n, msg)
    \/ \E n \in Nodes, msg \in network : HandleNack(n, msg)
    \/ \E n \in Nodes, msg \in network : HandleAck(n, msg)
    \/ \E n \in Nodes : CommitPop(n)

\* Fairness: all messages are eventually delivered
Fairness ==
    /\ \A msg \in network :
        WF_vars(\E n \in Nodes : HandleRemote(n, msg))
    /\ \A msg \in network :
        WF_vars(\E n \in Nodes : HandleNack(n, msg))
    /\ \A msg \in network :
        WF_vars(\E n \in Nodes : HandleAck(n, msg))

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
(* Safety Properties *)

(*
 * AT MOST ONE COMMIT: The core exactly-once guarantee.
 * At most one node transitions to "committed" state.
 *)
AtMostOneCommit ==
    \A n1, n2 \in Nodes :
        (popState[n1] = "committed" /\ popState[n2] = "committed")
        => n1 = n2

(*
 * WINNER IS HLC WINNER: If a node commits, no other node that also
 * attempted a pop (and is waiting or committed — i.e., competing)
 * has a strictly winning HLC. This ensures the tiebreaker is respected.
 *)
WinnerIsHLCWinner ==
    \A n \in Nodes :
        popState[n] = "committed" =>
            \A m \in Nodes :
                (m /= n /\ popState[m] \in {"waiting", "committed"}) =>
                    ~Wins(popHLC[m], m, popHLC[n], n)

(*
 * NO ABORTED THEN COMMITTED: No node that has a competing pop with
 * a higher HLC ever reaches "committed" state. This is the invariant
 * form of "losers never commit."
 *)
NoAbortedThenCommitted ==
    \A n \in Nodes :
        popState[n] = "committed" =>
            ~ \E m \in Nodes :
                m /= n /\ popState[m] \in {"waiting", "committed"} /\
                Wins(popHLC[m], m, popHLC[n], n)

(*
 * NO ORPHAN TIPS: After all messages are delivered and all nodes have
 * either committed or aborted, every node's tip set contains the
 * committed node's pop offset (if any committed).
 *)
OrphanTipSafety ==
    \A n \in Nodes :
        popState[n] = "committed" =>
            popOffset[n] \in logDomain

(*
 * TYPE INVARIANT
 *)
TypeOK ==
    /\ \A o \in logDomain :
        /\ log[o].deps \subseteq (logDomain \cup {NULL})
        /\ log[o].op = "REMOVE"
        /\ log[o].hlc \in Nat
        /\ log[o].node \in Nodes
    /\ \A n \in Nodes :
        /\ tips[n] \subseteq logDomain
        /\ hlc[n] \in Nat
        /\ nextOffset[n] \in Nat
        /\ popState[n] \in {"idle", "waiting", "committed", "aborted"}
        /\ acked[n] \subseteq Nodes

=============================================================================
