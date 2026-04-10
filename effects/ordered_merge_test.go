/*
 * Copyright 2026 Swytch Labs BV
 *
 * This file is part of Swytch.
 *
 * Swytch is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * Swytch is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Swytch. If not, see <https://www.gnu.org/licenses/>.
 */

package effects

import (
	"testing"
	"time"

	pb "github.com/swytchdb/cache/cluster/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func oTs(nanos int64) *timestamppb.Timestamp {
	return timestamppb.New(time.Unix(0, nanos))
}

func oTip(nodeID, offset uint64) Tip {
	return Tip{nodeID, offset}
}

// orderedAppendEffect creates an ORDERED RPUSH-style effect with the given
// element ID, value, and fork-choice hash.
func orderedAppendEffect(offset Tip, hlcNanos int64, nodeID uint64, id string, value []byte, deps []*pb.EffectRef) *DAGNode {
	hlc := oTs(hlcNanos)
	return &DAGNode{
		Offset: offset,
		Effect: &pb.Effect{
			Key:            []byte("k"),
			Hlc:            hlc,
			NodeId:         nodeID,
			ForkChoiceHash: ComputeForkChoiceHash(pb.NodeID(nodeID), hlc),
			Deps:           deps,
			Kind: &pb.Effect_Data{Data: &pb.DataEffect{
				Op:         pb.EffectOp_INSERT_OP,
				Merge:      pb.MergeRule_LAST_WRITE_WINS,
				Collection: pb.CollectionKind_ORDERED,
				Placement:  pb.Placement_PLACE_TAIL,
				Id:         []byte(id),
				Value:      &pb.DataEffect_Raw{Raw: value},
			}},
		},
	}
}

// extractOrderedValues returns the element values from a ReducedEffect's
// OrderedElements in order, for test assertions.
func extractOrderedValues(r *pb.ReducedEffect) []string {
	if r == nil {
		return nil
	}
	var vals []string
	for _, elem := range r.OrderedElements {
		vals = append(vals, string(elem.Data.GetRaw()))
	}
	return vals
}

// sliceEqual returns true if two string slices are equal.
func sliceEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// TestResolveFull_OrderedLinearChain verifies that a simple linear chain
// of ORDERED appends produces elements in causal order.
func TestResolveFull_OrderedLinearChain(t *testing.T) {
	// Chain: A → B → C (three RPUSH appends)
	a := orderedAppendEffect(oTip(1, 100), 10, 1, "id-a", []byte("a"), nil)
	b := orderedAppendEffect(oTip(1, 200), 20, 1, "id-b", []byte("b"), toPbRefs([]Tip{a.Offset}))
	c := orderedAppendEffect(oTip(1, 300), 30, 1, "id-c", []byte("c"), toPbRefs([]Tip{b.Offset}))

	dag := BuildDAG([]*DAGNode{a, b, c})
	result := resolveFull(dag)

	got := extractOrderedValues(result)
	want := []string{"a", "b", "c"}
	if !sliceEqual(got, want) {
		t.Fatalf("linear chain: got %v, want %v", got, want)
	}
}

// TestResolveFull_OrderedForkMustPreserveCausalOrder verifies that adding
// a concurrent branch does NOT change the relative ordering of elements
// that are in the same causal chain.
//
// This is the bug from Jepsen: on node A with a linear chain [a, b, c, d],
// the order is [a, b, c, d]. On node B with an extra concurrent branch
// forking from b, the order became [d, e, a, b, c] instead of preserving
// a, b, c, d's relative order.
func TestResolveFull_OrderedForkMustPreserveCausalOrder(t *testing.T) {
	// Linear chain: A → B → C → D (four appends: a, b, c, d)
	a := orderedAppendEffect(oTip(1, 100), 10, 1, "id-a", []byte("a"), nil)
	b := orderedAppendEffect(oTip(1, 200), 20, 1, "id-b", []byte("b"), toPbRefs([]Tip{a.Offset}))
	c := orderedAppendEffect(oTip(1, 300), 30, 1, "id-c", []byte("c"), toPbRefs([]Tip{b.Offset}))
	d := orderedAppendEffect(oTip(2, 400), 40, 2, "id-d", []byte("d"), toPbRefs([]Tip{c.Offset}))

	// Verify linear: [a, b, c, d]
	linearDag := BuildDAG([]*DAGNode{a, b, c, d})
	linearResult := resolveFull(linearDag)
	linearGot := extractOrderedValues(linearResult)
	linearWant := []string{"a", "b", "c", "d"}
	if !sliceEqual(linearGot, linearWant) {
		t.Fatalf("linear: got %v, want %v", linearGot, linearWant)
	}

	// Now add a concurrent branch: B → E (fork at B, concurrent with C and D)
	e := orderedAppendEffect(oTip(3, 350), 35, 3, "id-e", []byte("e"), toPbRefs([]Tip{b.Offset}))

	// Forked DAG: tips are D and E
	//   A → B → C → D
	//        \→ E
	forkedDag := BuildDAG([]*DAGNode{a, b, c, d, e})
	forkedResult := resolveFull(forkedDag)
	forkedGot := extractOrderedValues(forkedResult)

	// The relative order of a, b, c, d must be preserved.
	// e can go anywhere (it's concurrent with c and d), but
	// a must be before b, b before c, c before d.
	if len(forkedGot) != 5 {
		t.Fatalf("forked: expected 5 elements, got %v", forkedGot)
	}

	// Check causal ordering is preserved
	posOf := make(map[string]int)
	for i, v := range forkedGot {
		posOf[v] = i
	}
	if posOf["a"] >= posOf["b"] {
		t.Errorf("forked: 'a' (pos %d) must be before 'b' (pos %d); got %v",
			posOf["a"], posOf["b"], forkedGot)
	}
	if posOf["b"] >= posOf["c"] {
		t.Errorf("forked: 'b' (pos %d) must be before 'c' (pos %d); got %v",
			posOf["b"], posOf["c"], forkedGot)
	}
	if posOf["c"] >= posOf["d"] {
		t.Errorf("forked: 'c' (pos %d) must be before 'd' (pos %d); got %v",
			posOf["c"], posOf["d"], forkedGot)
	}
}

// TestResolveFull_OrderedForkStableUnderGrowth verifies that the ordering
// produced by a smaller DAG is a prefix/subsequence of the ordering
// produced by the same DAG with additional concurrent effects.
//
// This directly models the Jepsen failure: n3 reads [a, b, c, d] and
// n5 reads [d, e, a, b, c] — the relative order of a, b, c flipped
// when d and e were added.
func TestResolveFull_OrderedForkStableUnderGrowth(t *testing.T) {
	// Chain: A → B → C
	a := orderedAppendEffect(oTip(1, 100), 10, 1, "id-a", []byte("a"), nil)
	b := orderedAppendEffect(oTip(2, 200), 20, 2, "id-b", []byte("b"), toPbRefs([]Tip{a.Offset}))
	c := orderedAppendEffect(oTip(1, 300), 30, 1, "id-c", []byte("c"), toPbRefs([]Tip{b.Offset}))

	// Initial DAG: linear [a, b, c]
	dag1 := BuildDAG([]*DAGNode{a, b, c})
	r1 := resolveFull(dag1)
	got1 := extractOrderedValues(r1)
	want1 := []string{"a", "b", "c"}
	if !sliceEqual(got1, want1) {
		t.Fatalf("initial: got %v, want %v", got1, want1)
	}

	// Add concurrent appends D (fork at A) and E (on D's chain)
	d := orderedAppendEffect(oTip(3, 150), 15, 3, "id-d", []byte("d"), toPbRefs([]Tip{a.Offset}))
	e := orderedAppendEffect(oTip(3, 250), 25, 3, "id-e", []byte("e"), toPbRefs([]Tip{d.Offset}))

	// Grown DAG:
	//   A → B → C
	//    \→ D → E
	dag2 := BuildDAG([]*DAGNode{a, b, c, d, e})
	r2 := resolveFull(dag2)
	got2 := extractOrderedValues(r2)

	if len(got2) != 5 {
		t.Fatalf("grown: expected 5 elements, got %v", got2)
	}

	// a, b, c must maintain their relative ordering from the initial read
	posOf := make(map[string]int)
	for i, v := range got2 {
		posOf[v] = i
	}
	if posOf["a"] >= posOf["b"] {
		t.Errorf("grown: 'a' (pos %d) must be before 'b' (pos %d); got %v",
			posOf["a"], posOf["b"], got2)
	}
	if posOf["b"] >= posOf["c"] {
		t.Errorf("grown: 'b' (pos %d) must be before 'c' (pos %d); got %v",
			posOf["b"], posOf["c"], got2)
	}
	// d, e must also be in order
	if posOf["d"] >= posOf["e"] {
		t.Errorf("grown: 'd' (pos %d) must be before 'e' (pos %d); got %v",
			posOf["d"], posOf["e"], got2)
	}
}

// TestResolveFull_OrderedDeterministicAcrossDAGViews verifies that two
// different "views" of the same set of effects produce the same ordering.
// This models the Jepsen scenario where n3 and n5 have different index
// tips but the same underlying effects.
func TestResolveFull_OrderedDeterministicAcrossDAGViews(t *testing.T) {
	// Shared effects: a linear chain with a fork
	//   A → B → C → D
	//        \→ E → F
	a := orderedAppendEffect(oTip(1, 100), 10, 1, "id-a", []byte("a"), nil)
	b := orderedAppendEffect(oTip(1, 200), 20, 1, "id-b", []byte("b"), toPbRefs([]Tip{a.Offset}))
	c := orderedAppendEffect(oTip(1, 300), 30, 1, "id-c", []byte("c"), toPbRefs([]Tip{b.Offset}))
	d := orderedAppendEffect(oTip(2, 400), 40, 2, "id-d", []byte("d"), toPbRefs([]Tip{c.Offset}))
	e := orderedAppendEffect(oTip(3, 350), 35, 3, "id-e", []byte("e"), toPbRefs([]Tip{b.Offset}))
	f := orderedAppendEffect(oTip(3, 450), 45, 3, "id-f", []byte("f"), toPbRefs([]Tip{e.Offset}))

	// View 1: all effects, tips = [D, F]
	dag1 := BuildDAG([]*DAGNode{a, b, c, d, e, f})
	r1 := resolveFull(dag1)
	got1 := extractOrderedValues(r1)

	// View 2: same effects but walked from different tips (e.g., with an
	// extra subscription effect that doesn't carry data)
	sub := &DAGNode{
		Offset: oTip(4, 500),
		Effect: &pb.Effect{
			Key:    []byte("k"),
			Hlc:    oTs(50),
			NodeId: 4,
			Deps:   toPbRefs([]Tip{d.Offset, f.Offset}),
			Kind:   &pb.Effect_Subscription{Subscription: &pb.SubscriptionEffect{SubscriberNodeId: 4}},
		},
	}
	dag2 := BuildDAG([]*DAGNode{a, b, c, d, e, f, sub})
	r2 := resolveFull(dag2)
	got2 := extractOrderedValues(r2)

	if !sliceEqual(got1, got2) {
		t.Fatalf("different views produced different orderings:\n  view1: %v\n  view2: %v", got1, got2)
	}
}

// TestComposeOrdered_NewElementsCausallyAfterBase verifies that
// composeOrdered doesn't reorder elements that are causally after
// base elements based on fork-choice hash.
func TestComposeOrdered_NewElementsCausallyAfterBase(t *testing.T) {
	// Base: list with elements [a, b]
	base := &pb.ReducedEffect{
		Op:         pb.EffectOp_INSERT_OP,
		Collection: pb.CollectionKind_ORDERED,
		OrderedElements: []*pb.ReducedElement{
			{Data: &pb.DataEffect{Id: []byte("id-a"), Value: &pb.DataEffect_Raw{Raw: []byte("a")}},
				ForkChoiceHash: ComputeForkChoiceHash(1, oTs(10))},
			{Data: &pb.DataEffect{Id: []byte("id-b"), Value: &pb.DataEffect_Raw{Raw: []byte("b")}},
				ForkChoiceHash: ComputeForkChoiceHash(1, oTs(20))},
		},
		ForkChoiceHash: ComputeForkChoiceHash(1, oTs(10)),
	}

	// Delta: new element c that should come AFTER a and b (it's from a
	// concurrent branch but causally we're composing sequentially)
	delta := &pb.ReducedEffect{
		Op:         pb.EffectOp_INSERT_OP,
		Collection: pb.CollectionKind_ORDERED,
		OrderedElements: []*pb.ReducedElement{
			// Give this a LOWER fork hash so it would be placed first by the
			// current buggy logic in composeOrdered line 254
			{Data: &pb.DataEffect{Id: []byte("id-c"), Value: &pb.DataEffect_Raw{Raw: []byte("c")}},
				ForkChoiceHash: ComputeForkChoiceHash(1, oTs(1))}, // very low HLC → low hash
		},
		ForkChoiceHash: ComputeForkChoiceHash(1, oTs(1)),
	}

	result := composeSequential(base, delta)
	got := extractOrderedValues(result)

	// composeSequential means delta happens AFTER base. So c must come
	// after a and b regardless of fork-choice hash.
	want := []string{"a", "b", "c"}
	if !sliceEqual(got, want) {
		t.Fatalf("composeOrdered: got %v, want %v\n"+
			"New element with lower fork hash was placed before base elements", got, want)
	}
}

// TestMergeOrdered_ConcurrentBranches_Deterministic verifies that
// merging two concurrent branches of ORDERED elements produces a
// deterministic result regardless of argument order.
func TestMergeOrdered_ConcurrentBranches_Deterministic(t *testing.T) {
	branchA := &pb.ReducedEffect{
		Op:         pb.EffectOp_INSERT_OP,
		Collection: pb.CollectionKind_ORDERED,
		Hlc:        oTs(30),
		NodeId:     1,
		OrderedElements: []*pb.ReducedElement{
			{Data: &pb.DataEffect{Id: []byte("id-a"), Value: &pb.DataEffect_Raw{Raw: []byte("a")}},
				ForkChoiceHash: ComputeForkChoiceHash(1, oTs(10))},
			{Data: &pb.DataEffect{Id: []byte("id-b"), Value: &pb.DataEffect_Raw{Raw: []byte("b")}},
				ForkChoiceHash: ComputeForkChoiceHash(1, oTs(20))},
		},
		ForkChoiceHash: ComputeForkChoiceHash(1, oTs(10)),
		Commutative:    true,
	}
	branchB := &pb.ReducedEffect{
		Op:         pb.EffectOp_INSERT_OP,
		Collection: pb.CollectionKind_ORDERED,
		Hlc:        oTs(35),
		NodeId:     2,
		OrderedElements: []*pb.ReducedElement{
			{Data: &pb.DataEffect{Id: []byte("id-c"), Value: &pb.DataEffect_Raw{Raw: []byte("c")}},
				ForkChoiceHash: ComputeForkChoiceHash(2, oTs(15))},
			{Data: &pb.DataEffect{Id: []byte("id-d"), Value: &pb.DataEffect_Raw{Raw: []byte("d")}},
				ForkChoiceHash: ComputeForkChoiceHash(2, oTs(25))},
		},
		ForkChoiceHash: ComputeForkChoiceHash(2, oTs(15)),
		Commutative:    true,
	}

	r1 := Merge2(branchA, branchB)
	r2 := Merge2(branchB, branchA)

	got1 := extractOrderedValues(r1)
	got2 := extractOrderedValues(r2)

	if !sliceEqual(got1, got2) {
		t.Fatalf("Merge2 not commutative for ordered:\n  Merge2(A,B): %v\n  Merge2(B,A): %v",
			got1, got2)
	}

	// Within each branch, relative order must be preserved
	posOf := make(map[string]int)
	for i, v := range got1 {
		posOf[v] = i
	}
	if posOf["a"] >= posOf["b"] {
		t.Errorf("branch A ordering broken: a@%d b@%d in %v", posOf["a"], posOf["b"], got1)
	}
	if posOf["c"] >= posOf["d"] {
		t.Errorf("branch B ordering broken: c@%d d@%d in %v", posOf["c"], posOf["d"], got1)
	}
}

// isContiguous returns true if all values from the given branch appear
// as a contiguous run in the result slice (no elements from other
// branches interleaved).
func isContiguous(result []string, branch []string) bool {
	if len(branch) == 0 {
		return true
	}
	branchSet := make(map[string]bool, len(branch))
	for _, v := range branch {
		branchSet[v] = true
	}
	// Find first occurrence
	start := -1
	for i, v := range result {
		if branchSet[v] {
			start = i
			break
		}
	}
	if start == -1 {
		return false
	}
	// All branch elements must occupy consecutive positions starting here
	idx := 0
	for i := start; i < len(result) && idx < len(branch); i++ {
		if branchSet[result[i]] {
			idx++
		} else {
			return false // non-branch element found within the run
		}
	}
	return idx == len(branch)
}

// TestResolveFull_OrderedBranchContiguity_Section2_3 validates whitepaper
// §2.3: "The hash determines which sequence appears first, but neither
// is interleaved." Concurrent sequences must remain contiguous after merge.
//
// This test constructs a fork with two 3-element branches and verifies
// that each branch's elements form a contiguous block in the result,
// regardless of which branch "wins" the hash comparison.
func TestResolveFull_OrderedBranchContiguity_Section2_3(t *testing.T) {
	// Shared root X
	x := orderedAppendEffect(oTip(1, 100), 10, 1, "id-x", []byte("x"), nil)

	// Branch A: X → a1 → a2 → a3 (Node 1)
	a1 := orderedAppendEffect(oTip(1, 200), 20, 1, "id-a1", []byte("a1"), toPbRefs([]Tip{x.Offset}))
	a2 := orderedAppendEffect(oTip(1, 300), 30, 1, "id-a2", []byte("a2"), toPbRefs([]Tip{a1.Offset}))
	a3 := orderedAppendEffect(oTip(1, 400), 40, 1, "id-a3", []byte("a3"), toPbRefs([]Tip{a2.Offset}))

	// Branch B: X → b1 → b2 → b3 (Node 2, concurrent with A)
	b1 := orderedAppendEffect(oTip(2, 210), 21, 2, "id-b1", []byte("b1"), toPbRefs([]Tip{x.Offset}))
	b2 := orderedAppendEffect(oTip(2, 310), 31, 2, "id-b2", []byte("b2"), toPbRefs([]Tip{b1.Offset}))
	b3 := orderedAppendEffect(oTip(2, 410), 41, 2, "id-b3", []byte("b3"), toPbRefs([]Tip{b2.Offset}))

	dag := BuildDAG([]*DAGNode{x, a1, a2, a3, b1, b2, b3})
	result := resolveFull(dag)
	got := extractOrderedValues(result)

	if len(got) != 7 {
		t.Fatalf("expected 7 elements, got %v", got)
	}

	// x must come first (causal root)
	if got[0] != "x" {
		t.Fatalf("expected 'x' first, got %v", got)
	}

	branchA := []string{"a1", "a2", "a3"}
	branchB := []string{"b1", "b2", "b3"}

	// §2.3: each branch must be contiguous — no interleaving
	if !isContiguous(got, branchA) {
		t.Errorf("branch A [a1,a2,a3] is not contiguous in %v", got)
	}
	if !isContiguous(got, branchB) {
		t.Errorf("branch B [b1,b2,b3] is not contiguous in %v", got)
	}

	// Intra-branch ordering must be preserved
	posOf := make(map[string]int)
	for i, v := range got {
		posOf[v] = i
	}
	if posOf["a1"] >= posOf["a2"] || posOf["a2"] >= posOf["a3"] {
		t.Errorf("branch A order broken: a1@%d a2@%d a3@%d in %v",
			posOf["a1"], posOf["a2"], posOf["a3"], got)
	}
	if posOf["b1"] >= posOf["b2"] || posOf["b2"] >= posOf["b3"] {
		t.Errorf("branch B order broken: b1@%d b2@%d b3@%d in %v",
			posOf["b1"], posOf["b2"], posOf["b3"], got)
	}
}

// TestResolveFull_OrderedBranchContiguity_MergeTip verifies contiguity
// is preserved even when a merge node (e.g., subscription or bind)
// collapses concurrent branches into a single tip.
func TestResolveFull_OrderedBranchContiguity_MergeTip(t *testing.T) {
	// Root X
	x := orderedAppendEffect(oTip(1, 100), 10, 1, "id-x", []byte("x"), nil)

	// Branch A: X → a1 → a2 (Node 1)
	a1 := orderedAppendEffect(oTip(1, 200), 20, 1, "id-a1", []byte("a1"), toPbRefs([]Tip{x.Offset}))
	a2 := orderedAppendEffect(oTip(1, 300), 30, 1, "id-a2", []byte("a2"), toPbRefs([]Tip{a1.Offset}))

	// Branch B: X → b1 → b2 (Node 2, concurrent with A)
	b1 := orderedAppendEffect(oTip(2, 210), 21, 2, "id-b1", []byte("b1"), toPbRefs([]Tip{x.Offset}))
	b2 := orderedAppendEffect(oTip(2, 310), 31, 2, "id-b2", []byte("b2"), toPbRefs([]Tip{b1.Offset}))

	// Merge node: subscription that depends on both branch tips
	merge := &DAGNode{
		Offset: oTip(3, 500),
		Effect: &pb.Effect{
			Key:    []byte("k"),
			Hlc:    oTs(50),
			NodeId: 3,
			Deps:   toPbRefs([]Tip{a2.Offset, b2.Offset}),
			Kind:   &pb.Effect_Subscription{Subscription: &pb.SubscriptionEffect{SubscriberNodeId: 3}},
		},
	}

	// With merge node: single tip, but branches must still be contiguous
	dag := BuildDAG([]*DAGNode{x, a1, a2, b1, b2, merge})
	result := resolveFull(dag)
	got := extractOrderedValues(result)

	if len(got) != 5 {
		t.Fatalf("expected 5 elements, got %v", got)
	}

	if got[0] != "x" {
		t.Fatalf("expected 'x' first, got %v", got)
	}

	branchA := []string{"a1", "a2"}
	branchB := []string{"b1", "b2"}

	if !isContiguous(got, branchA) {
		t.Errorf("branch A [a1,a2] is not contiguous in %v (merge tip present)", got)
	}
	if !isContiguous(got, branchB) {
		t.Errorf("branch B [b1,b2] is not contiguous in %v (merge tip present)", got)
	}

	// Same result without the merge node (multi-tip)
	dagNoMerge := BuildDAG([]*DAGNode{x, a1, a2, b1, b2})
	resultNoMerge := resolveFull(dagNoMerge)
	gotNoMerge := extractOrderedValues(resultNoMerge)

	if !sliceEqual(got, gotNoMerge) {
		t.Fatalf("merge tip changed ordering:\n  with merge:    %v\n  without merge: %v", got, gotNoMerge)
	}
}

// TestResolveFull_OrderedThreeBranches_Contiguous verifies contiguity
// with three concurrent branches merging at a single point.
func TestResolveFull_OrderedThreeBranches_Contiguous(t *testing.T) {
	// Root X
	x := orderedAppendEffect(oTip(1, 100), 10, 1, "id-x", []byte("x"), nil)

	// Branch A: X → a1 → a2
	a1 := orderedAppendEffect(oTip(1, 200), 20, 1, "id-a1", []byte("a1"), toPbRefs([]Tip{x.Offset}))
	a2 := orderedAppendEffect(oTip(1, 300), 30, 1, "id-a2", []byte("a2"), toPbRefs([]Tip{a1.Offset}))

	// Branch B: X → b1 → b2
	b1 := orderedAppendEffect(oTip(2, 210), 21, 2, "id-b1", []byte("b1"), toPbRefs([]Tip{x.Offset}))
	b2 := orderedAppendEffect(oTip(2, 310), 31, 2, "id-b2", []byte("b2"), toPbRefs([]Tip{b1.Offset}))

	// Branch C: X → c1 → c2
	c1 := orderedAppendEffect(oTip(3, 220), 22, 3, "id-c1", []byte("c1"), toPbRefs([]Tip{x.Offset}))
	c2 := orderedAppendEffect(oTip(3, 320), 32, 3, "id-c2", []byte("c2"), toPbRefs([]Tip{c1.Offset}))

	dag := BuildDAG([]*DAGNode{x, a1, a2, b1, b2, c1, c2})
	result := resolveFull(dag)
	got := extractOrderedValues(result)

	if len(got) != 7 {
		t.Fatalf("expected 7 elements, got %v", got)
	}

	branchA := []string{"a1", "a2"}
	branchB := []string{"b1", "b2"}
	branchC := []string{"c1", "c2"}

	if !isContiguous(got, branchA) {
		t.Errorf("branch A not contiguous in %v", got)
	}
	if !isContiguous(got, branchB) {
		t.Errorf("branch B not contiguous in %v", got)
	}
	if !isContiguous(got, branchC) {
		t.Errorf("branch C not contiguous in %v", got)
	}
}

// TestFilterTentative_CrossTxnDepsMustNotConfirm verifies that
// filterTentativeEffects does NOT confirm tentative effects from
// transaction A when walking deps from transaction B's confirmed bind.
//
// This is the root cause of the Jepsen G0: transaction B's bind is
// confirmed and its write depends on transaction A's tentative write
// (A's write was a tip when B ran). Phase 3's chain walk was confirming
// A's effect because it had a TxnId, even though A's own bind didn't
// exist yet.
func TestFilterTentative_CrossTxnDepsMustNotConfirm(t *testing.T) {
	// Setup: key "k" has a root effect (non-transactional)
	root := &DAGNode{
		Offset: oTip(1, 100),
		Effect: &pb.Effect{
			Key:            []byte("k"),
			Hlc:            oTs(10),
			NodeId:         1,
			ForkChoiceHash: ComputeForkChoiceHash(1, oTs(10)),
			Kind: &pb.Effect_Data{Data: &pb.DataEffect{
				Op:         pb.EffectOp_INSERT_OP,
				Collection: pb.CollectionKind_ORDERED,
				Placement:  pb.Placement_PLACE_TAIL,
				Id:         []byte("root"),
				Value:      &pb.DataEffect_Raw{Raw: []byte("root")},
			}},
		},
	}

	// Transaction A writes to "k" (tentative — no bind in DAG)
	txAWrite := &DAGNode{
		Offset: oTip(2, 200),
		Effect: &pb.Effect{
			Key:            []byte("k"),
			Hlc:            oTs(20),
			NodeId:         2,
			TxnId:          "txn-A",
			ForkChoiceHash: ComputeForkChoiceHash(2, oTs(20)),
			Deps:           toPbRefs([]Tip{root.Offset}),
			Kind: &pb.Effect_Data{Data: &pb.DataEffect{
				Op:         pb.EffectOp_INSERT_OP,
				Collection: pb.CollectionKind_ORDERED,
				Placement:  pb.Placement_PLACE_TAIL,
				Id:         []byte("a-val"),
				Value:      &pb.DataEffect_Raw{Raw: []byte("from-A")},
			}},
		},
	}

	// Transaction B writes to "k", depending on A's tentative write
	// (A's write was a tip when B ran)
	txBWrite := &DAGNode{
		Offset: oTip(3, 300),
		Effect: &pb.Effect{
			Key:            []byte("k"),
			Hlc:            oTs(30),
			NodeId:         3,
			TxnId:          "txn-B",
			ForkChoiceHash: ComputeForkChoiceHash(3, oTs(30)),
			Deps:           toPbRefs([]Tip{txAWrite.Offset}),
			Kind: &pb.Effect_Data{Data: &pb.DataEffect{
				Op:         pb.EffectOp_INSERT_OP,
				Collection: pb.CollectionKind_ORDERED,
				Placement:  pb.Placement_PLACE_TAIL,
				Id:         []byte("b-val"),
				Value:      &pb.DataEffect_Raw{Raw: []byte("from-B")},
			}},
		},
	}

	// Transaction B's bind (confirmed) — covers key "k"
	txBBind := &DAGNode{
		Offset: oTip(3, 400),
		Effect: &pb.Effect{
			Key:            []byte("k"),
			Hlc:            oTs(40),
			NodeId:         3,
			TxnId:          "txn-B",
			ForkChoiceHash: ComputeForkChoiceHash(3, oTs(40)),
			Deps:           toPbRefs([]Tip{txBWrite.Offset}),
			Kind: &pb.Effect_TxnBind{TxnBind: &pb.TransactionalBindEffect{
				Keys: []*pb.TransactionalBindEffect_KeyBind{{
					Key:          []byte("k"),
					ConsumedTips: toPbRefs([]Tip{root.Offset}),
					NewTip:       toPbRef(txBWrite.Offset),
				}},
				OriginatorNodeId: 3,
				TxnHlc:           oTs(30),
			}},
		},
	}

	nodes := []*DAGNode{root, txAWrite, txBWrite, txBBind}

	// Filter with no current txn
	filtered := filterTentativeEffects(nodes, "", "k", nil)

	// txAWrite must be filtered — its bind doesn't exist.
	// txBWrite and txBBind must be kept — B's bind confirms B's effects.
	// root must be kept — non-transactional.
	for _, n := range filtered {
		if n.Offset == txAWrite.Offset {
			t.Fatalf("tentative effect from txn-A (offset %d) was NOT filtered!\n"+
				"Phase 3 confirmed it by walking deps from txn-B's bind.\n"+
				"filtered nodes: %v", txAWrite.Offset, offsets(filtered))
		}
	}

	// Verify B's effects are present
	hasRoot, hasBWrite, hasBBind := false, false, false
	for _, n := range filtered {
		switch n.Offset {
		case oTip(1, 100):
			hasRoot = true
		case oTip(3, 300):
			hasBWrite = true
		case oTip(3, 400):
			hasBBind = true
		}
	}
	if !hasRoot || !hasBWrite || !hasBBind {
		t.Fatalf("expected root, txBWrite, txBBind in filtered; got offsets %v", offsets(filtered))
	}
}

func offsets(nodes []*DAGNode) []Tip {
	out := make([]Tip, len(nodes))
	for i, n := range nodes {
		out[i] = n.Offset
	}
	return out
}
