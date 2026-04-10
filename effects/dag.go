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
	"bytes"
	"sort"

	pb "github.com/swytchdb/cache/cluster/proto"
)

// DAGNode pairs a log offset with the deserialized effect at that offset.
type DAGNode struct {
	Offset Tip
	Effect *pb.Effect
}

// AnnotatedNode pairs a node with concurrency metadata from topological sort.
type AnnotatedNode struct {
	Node       *DAGNode
	Concurrent bool // true if concurrent with at least one other node at the same DAG level
}

// DAG represents the causal dependency graph for a set of effects.
// Nodes are effects, edges are dependency pointers (child → parent).
// Ported from the types package DAG which was proven correct.
type DAG struct {
	byOffset map[Tip]*DAGNode
	children map[Tip][]*DAGNode // parent offset → children
	roots    []*DAGNode
	tips     []*DAGNode
}

// BuildDAG constructs a dependency DAG from a set of nodes.
// Nodes are indexed by Offset. Edges come from Effect.Deps.
func BuildDAG(nodes []*DAGNode) *DAG {
	d := &DAG{
		byOffset: make(map[Tip]*DAGNode, len(nodes)),
		children: make(map[Tip][]*DAGNode, len(nodes)),
	}

	for _, n := range nodes {
		d.byOffset[n.Offset] = n
	}

	hasChildren := make(map[Tip]bool, len(nodes))

	for _, n := range nodes {
		for _, dep := range allDeps(n.Effect) {
			dd := r(dep)
			if _, ok := d.byOffset[dd]; ok {
				d.children[dd] = append(d.children[dd], n)
				hasChildren[dd] = true
			}
		}
	}

	for _, n := range nodes {
		// A node is a root if it has no deps within this DAG
		// (deps may point outside the DAG to nodes not in byOffset)
		isRoot := true
		for _, dep := range allDeps(n.Effect) {
			if _, ok := d.byOffset[r(dep)]; ok {
				isRoot = false
				break
			}
		}
		if isRoot {
			d.roots = append(d.roots, n)
		}
		if !hasChildren[n.Offset] {
			d.tips = append(d.tips, n)
		}
	}

	return d
}

// Roots returns nodes with no parents (no deps).
func (d *DAG) Roots() []*DAGNode {
	return d.roots
}

// Tips returns nodes that no other node depends on.
func (d *DAG) Tips() []*DAGNode {
	return d.tips
}

// Children returns the direct children of a node.
func (d *DAG) Children(n *DAGNode) []*DAGNode {
	return d.children[n.Offset]
}

// allDeps returns the effect's Deps plus any KeyBind.NewTip offsets for bind effects.
// Returns a fresh slice when there are bind tips to append; reusing eff.Deps as
// the backing array would race with concurrent callers appending to the same
// shared slice.
func allDeps(eff *pb.Effect) []*pb.EffectRef {
	bind := eff.GetTxnBind()
	if bind == nil || len(bind.Keys) == 0 {
		return eff.Deps
	}
	deps := make([]*pb.EffectRef, len(eff.Deps), len(eff.Deps)+len(bind.Keys))
	copy(deps, eff.Deps)
	for _, kb := range bind.Keys {
		deps = append(deps, kb.NewTip)
	}
	return deps
}

func allDepsTip(eff *pb.Effect) []Tip {
	return fromPbRefs(allDeps(eff))
}

// Parent returns the single parent of a node (first dep), or nil for roots.
func (d *DAG) Parent(n *DAGNode) *DAGNode {
	deps := allDeps(n.Effect)
	if len(deps) == 0 {
		return nil
	}
	return d.byOffset[r(deps[0])]
}

// Parents returns all parents of a node (all deps).
func (d *DAG) Parents(n *DAGNode) []*DAGNode {
	var parents []*DAGNode
	for _, dep := range allDeps(n.Effect) {
		if p := d.byOffset[r(dep)]; p != nil {
			parents = append(parents, p)
		}
	}
	return parents
}

// AreConcurrent returns true if neither a is an ancestor of b nor b is an
// ancestor of a. Two nodes are concurrent if they didn't observe each other.
func (d *DAG) AreConcurrent(a, b *DAGNode) bool {
	if a.Offset == b.Offset {
		return false
	}
	if d.isAncestor(a, b) || d.isAncestor(b, a) {
		return false
	}
	return true
}

// isAncestor returns true if ancestor is an ancestor of descendant.
func (d *DAG) isAncestor(ancestor, descendant *DAGNode) bool {
	visited := make(map[Tip]bool)
	return d.isAncestorDFS(ancestor.Offset, descendant, visited)
}

func (d *DAG) isAncestorDFS(ancestorOffset Tip, current *DAGNode, visited map[Tip]bool) bool {
	if current.Offset == ancestorOffset {
		return true
	}
	if visited[current.Offset] {
		return false
	}
	visited[current.Offset] = true

	for _, dep := range allDeps(current.Effect) {
		if parent := d.byOffset[r(dep)]; parent != nil {
			if d.isAncestorDFS(ancestorOffset, parent, visited) {
				return true
			}
		}
	}
	return false
}

// TopologicalSort returns nodes in causal order using Kahn's algorithm.
// When multiple nodes are ready simultaneously, they are sorted by HLC ascending
// with NodeID as tiebreaker. Nodes are annotated as Concurrent if they share the
// ready queue with at least one truly concurrent node (verified via AreConcurrent).
func (d *DAG) TopologicalSort() []AnnotatedNode {
	inDegree := make(map[Tip]int, len(d.byOffset))
	for offset := range d.byOffset {
		inDegree[offset] = 0
	}
	for _, n := range d.byOffset {
		for _, dep := range allDeps(n.Effect) {
			if _, ok := d.byOffset[r(dep)]; ok {
				inDegree[n.Offset]++
			}
		}
	}

	var ready []*DAGNode
	for offset, deg := range inDegree {
		if deg == 0 {
			ready = append(ready, d.byOffset[offset])
		}
	}

	var result []AnnotatedNode

	for len(ready) > 0 {
		sort.Slice(ready, func(i, j int) bool {
			return compareNodes(ready[i], ready[j]) < 0
		})

		concurrent := make(map[Tip]bool, len(ready))
		if len(ready) > 1 {
			for i := 0; i < len(ready); i++ {
				for j := i + 1; j < len(ready); j++ {
					if d.AreConcurrent(ready[i], ready[j]) {
						concurrent[ready[i].Offset] = true
						concurrent[ready[j].Offset] = true
					}
				}
			}
		}

		batch := ready
		ready = nil

		for _, n := range batch {
			result = append(result, AnnotatedNode{
				Node:       n,
				Concurrent: concurrent[n.Offset],
			})

			for _, child := range d.children[n.Offset] {
				inDegree[child.Offset]--
				if inDegree[child.Offset] == 0 {
					ready = append(ready, child)
				}
			}
		}
	}

	return result
}

// FindLCA finds the deepest (closest to tips) common ancestor of all tips.
// For a single tip, returns the tip itself.
func (d *DAG) FindLCA(tips []*DAGNode) *DAGNode {
	if len(tips) == 0 {
		return nil
	}
	if len(tips) == 1 {
		return tips[0]
	}

	ancestorSets := make([]map[Tip]bool, len(tips))
	for i, tp := range tips {
		ancestors := make(map[Tip]bool)
		d.collectAncestors(tp, ancestors)
		ancestorSets[i] = ancestors
	}

	common := make(map[Tip]bool)
	for offset := range ancestorSets[0] {
		isCommon := true
		for i := 1; i < len(ancestorSets); i++ {
			if !ancestorSets[i][offset] {
				isCommon = false
				break
			}
		}
		if isCommon {
			common[offset] = true
		}
	}

	// Find the deepest common ancestor: the one with no common-ancestor children
	var deepest *DAGNode
	for offset := range common {
		n := d.byOffset[offset]
		hasCommonChild := false
		for _, child := range d.children[offset] {
			if common[child.Offset] {
				hasCommonChild = true
				break
			}
		}
		if !hasCommonChild {
			if deepest == nil || ForkChoiceLess(n.Effect.ForkChoiceHash, deepest.Effect.ForkChoiceHash) {
				deepest = n
			}
		}
	}

	return deepest
}

// collectAncestors walks backwards from n collecting all ancestors (including n itself).
func (d *DAG) collectAncestors(n *DAGNode, ancestors map[Tip]bool) {
	if ancestors[n.Offset] {
		return
	}
	ancestors[n.Offset] = true
	for _, dep := range allDepsTip(n.Effect) {
		if parent := d.byOffset[dep]; parent != nil {
			d.collectAncestors(parent, ancestors)
		}
	}
}

// ExtractBranches returns all branches from a fork point. Each branch is a
// slice of nodes from fork's direct child to a tip, oldest-first.
func (d *DAG) ExtractBranches(fork *DAGNode) [][]*DAGNode {
	var branches [][]*DAGNode
	for _, tip := range d.tips {
		path := d.PathToTip(fork, tip)
		if len(path) > 0 {
			branches = append(branches, path)
		}
	}
	return branches
}

// PathToTip returns the nodes on the path from fork to tip, exclusive of fork,
// oldest-first. If fork is nil, starts from the root (includes all ancestors of tip).
// Walks backwards from tip following the first dep, then reverses.
func (d *DAG) PathToTip(fork *DAGNode, tp *DAGNode) []*DAGNode {
	var forkOffset Tip
	if fork != nil {
		forkOffset = fork.Offset
	}

	var path []*DAGNode
	current := tp
	for current != nil {
		if current.Offset == forkOffset {
			break
		}
		path = append(path, current)
		deps := allDepsTip(current.Effect)
		if len(deps) == 0 {
			// Reached root
			if fork != nil {
				return nil // fork not found on this path
			}
			break
		}
		current = d.byOffset[deps[0]]
	}

	// If fork is non-nil and we didn't reach it, this tip isn't reachable from fork
	if fork != nil && (current == nil || current.Offset != forkOffset) {
		return nil
	}

	// Reverse to get oldest-first
	for i, j := 0, len(path)-1; i < j; i, j = i+1, j-1 {
		path[i], path[j] = path[j], path[i]
	}

	return path
}

// ConnectedComponents partitions the DAG into groups of nodes where roots
// that share any descendant are merged into the same group. This prevents
// double-counting when a node has deps on multiple roots.
func (d *DAG) ConnectedComponents() [][]*DAGNode {
	// Union-find: map each root offset to its representative
	parent := make(map[Tip]Tip)
	for _, r := range d.roots {
		parent[r.Offset] = r.Offset
	}
	var find func(Tip) Tip
	find = func(x Tip) Tip {
		if parent[x] != x {
			parent[x] = find(parent[x])
		}
		return parent[x]
	}
	union := func(a, b Tip) {
		ra, rb := find(a), find(b)
		if ra != rb {
			parent[ra] = rb
		}
	}

	// For each non-root node, find which roots are its ancestors.
	// If multiple roots, union them.
	for _, n := range d.byOffset {
		var ancestorRoots []Tip
		for _, r := range d.roots {
			if d.isAncestor(r, n) {
				ancestorRoots = append(ancestorRoots, r.Offset)
			}
		}
		if len(ancestorRoots) > 1 {
			for i := 1; i < len(ancestorRoots); i++ {
				union(ancestorRoots[0], ancestorRoots[i])
			}
		}
	}

	// Group nodes by their root's representative
	groups := make(map[Tip][]*DAGNode)
	for _, n := range d.byOffset {
		// Find which root component this node belongs to
		bestRoot := Tip{0, 0}
		zero := bestRoot
		for _, r := range d.roots {
			if n.Offset == r.Offset || d.isAncestor(r, n) {
				rep := find(r.Offset)
				if bestRoot == zero {
					bestRoot = rep
				} else if rep != bestRoot {
					// Node reachable from multiple root components — merge
					union(bestRoot, rep)
					bestRoot = find(bestRoot)
				}
			}
		}
		if bestRoot == zero {
			// Orphan node (shouldn't happen), put in its own group
			bestRoot = n.Offset
		}
		groups[find(bestRoot)] = append(groups[find(bestRoot)], n)
	}

	result := make([][]*DAGNode, 0, len(groups))
	for _, nodes := range groups {
		// Sort nodes within each component for determinism
		sort.Slice(nodes, func(i, j int) bool {
			return nodes[i].Offset[1] < nodes[j].Offset[1] || nodes[i].Offset[0] < nodes[j].Offset[0]
		})
		result = append(result, nodes)
	}
	// Sort components for determinism (by smallest offset in each)
	sort.Slice(result, func(i, j int) bool {
		return result[i][0].Offset[1] < result[j][0].Offset[1] || result[i][0].Offset[0] < result[j][0].Offset[0]
	})
	return result
}

// DescendantsOf returns all nodes reachable from n following children edges,
// including n itself.
func (d *DAG) DescendantsOf(n *DAGNode) []*DAGNode {
	visited := make(map[Tip]bool)
	d.collectDescendants(n, visited)
	result := make([]*DAGNode, 0, len(visited))
	for offset := range visited {
		result = append(result, d.byOffset[offset])
	}
	return result
}

func (d *DAG) collectDescendants(n *DAGNode, visited map[Tip]bool) {
	if visited[n.Offset] {
		return
	}
	visited[n.Offset] = true
	for _, child := range d.children[n.Offset] {
		d.collectDescendants(child, visited)
	}
}

// removeTip removes a Tip node from the DAG in place.
// Parents of the removed Tip that have no remaining children become new tips.
func (d *DAG) removeTip(tip *DAGNode) {
	delete(d.byOffset, tip.Offset)

	// Remove from tips slice
	for i, t := range d.tips {
		if t.Offset == tip.Offset {
			d.tips[i] = d.tips[len(d.tips)-1]
			d.tips = d.tips[:len(d.tips)-1]
			break
		}
	}

	// Update parents: remove tip from their children, promote to tip if childless
	for _, dep := range allDepsTip(tip.Effect) {
		ch := d.children[dep]
		for i, c := range ch {
			if c.Offset == tip.Offset {
				ch[i] = ch[len(ch)-1]
				ch = ch[:len(ch)-1]
				d.children[dep] = ch
				break
			}
		}
		if len(ch) == 0 {
			delete(d.children, dep)
			if _, ok := d.byOffset[dep]; ok {
				d.tips = append(d.tips, d.byOffset[dep])
			}
		}
	}

	delete(d.children, tip.Offset)
}

// Restrict returns a lightweight view of the DAG containing only the specified offsets.
// References the same DAGNode pointers — does not copy node data.
func (d *DAG) Restrict(offsets map[Tip]bool) *DAG {
	view := &DAG{
		byOffset: make(map[Tip]*DAGNode, len(offsets)),
		children: make(map[Tip][]*DAGNode, len(offsets)),
	}
	for off := range offsets {
		if n, ok := d.byOffset[off]; ok {
			view.byOffset[off] = n
		}
	}

	hasChildren := make(map[Tip]bool, len(offsets))
	for off := range offsets {
		n := view.byOffset[off]
		if n == nil {
			continue
		}
		for _, dep := range allDepsTip(n.Effect) {
			if _, ok := view.byOffset[dep]; ok {
				view.children[dep] = append(view.children[dep], n)
				hasChildren[dep] = true
			}
		}
	}
	for off, n := range view.byOffset {
		isRoot := true
		for _, dep := range allDepsTip(n.Effect) {
			if _, ok := view.byOffset[dep]; ok {
				isRoot = false
				break
			}
		}
		if isRoot {
			view.roots = append(view.roots, n)
		}
		if !hasChildren[off] {
			view.tips = append(view.tips, n)
		}
	}
	return view
}

// hasFork returns true if any node in the DAG has more than one child.
func (d *DAG) hasFork() bool {
	for _, ch := range d.children {
		if len(ch) > 1 {
			return true
		}
	}
	return false
}

// compareNodes orders nodes by fork_choice_hash ascending.
func compareNodes(a, b *DAGNode) int {
	return bytes.Compare(a.Effect.ForkChoiceHash, b.Effect.ForkChoiceHash)
}
