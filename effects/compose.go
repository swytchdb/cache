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
	pb "github.com/swytchdb/cache/cluster/proto"
)

// composeSequential composes a base ReducedEffect (causally before) with a
// delta ReducedEffect (causally after). This is NOT a concurrent merge —
// it's sequential composition. The delta happened after the base, so:
//   - Non-commutative delta supersedes base
//   - Commutative delta accumulates on base
//   - DEL delta removes everything
//
// Ported from the old types package composeSequential concept.
func composeSequential(base, delta *pb.ReducedEffect) *pb.ReducedEffect {
	if base == nil {
		return delta
	}
	if delta == nil {
		return base
	}

	// DEL delta: key is gone
	if delta.Op == pb.EffectOp_REMOVE_OP {
		result := cloneReduced(delta)
		result.Subscribers = unionSubscribers(base.Subscribers, delta.Subscribers)
		return result
	}

	// Collection/type change: delta supersedes
	if base.Collection != delta.Collection && delta.Scalar != nil {
		result := cloneReduced(delta)
		result.Subscribers = unionSubscribers(base.Subscribers, delta.Subscribers)
		return result
	}

	// Non-commutative delta on scalar: supersedes base entirely
	if !delta.Commutative && delta.Collection == pb.CollectionKind_SCALAR && delta.Scalar != nil {
		result := cloneReduced(delta)
		result.Subscribers = unionSubscribers(base.Subscribers, delta.Subscribers)
		return result
	}

	switch delta.Collection {
	case pb.CollectionKind_SCALAR:
		return composeScalar(base, delta)
	case pb.CollectionKind_KEYED:
		return composeKeyed(base, delta)
	case pb.CollectionKind_ORDERED:
		return composeOrdered(base, delta)
	}

	// Fallback: delta supersedes
	result := cloneReduced(delta)
	result.Subscribers = unionSubscribers(base.Subscribers, delta.Subscribers)
	return result
}

func composeScalar(base, delta *pb.ReducedEffect) *pb.ReducedEffect {
	if delta.Scalar == nil {
		// Delta has no scalar data (metadata-only); keep base data
		result := cloneReduced(base)
		result.Subscribers = unionSubscribers(base.Subscribers, delta.Subscribers)
		if delta.TypeTag != pb.ValueType_TYPE_UNSPECIFIED {
			result.TypeTag = delta.TypeTag
		}
		if delta.ExpiresAt != nil {
			result.ExpiresAt = delta.ExpiresAt
		}
		result.Hlc = delta.Hlc
		result.NodeId = delta.NodeId
		return result
	}

	result := cloneReduced(base)
	result.Hlc = delta.Hlc
	result.NodeId = delta.NodeId
	result.Subscribers = unionSubscribers(base.Subscribers, delta.Subscribers)
	if delta.TypeTag != pb.ValueType_TYPE_UNSPECIFIED {
		result.TypeTag = delta.TypeTag
	}

	switch delta.Merge {
	case pb.MergeRule_ADDITIVE_INT:
		baseVal := int64(0)
		if base.Scalar != nil {
			baseVal = base.Scalar.GetIntVal()
			if raw := base.Scalar.GetRaw(); raw != nil {
				baseVal = rawToInt(raw)
			}
		}
		if result.Scalar == nil {
			result.Scalar = cloneData(delta.Scalar)
		}
		result.Scalar.Value = &pb.DataEffect_IntVal{IntVal: baseVal + delta.Scalar.GetIntVal()}
		result.Merge = base.Merge // keep base merge rule (could be LWW if SET+INCR)
		if base.Merge == pb.MergeRule_LAST_WRITE_WINS {
			result.Commutative = false
		}

	case pb.MergeRule_ADDITIVE_FLOAT:
		baseVal := float64(0)
		if base.Scalar != nil {
			baseVal = base.Scalar.GetFloatVal()
			if raw := base.Scalar.GetRaw(); raw != nil {
				baseVal = rawToFloat(raw)
			}
		}
		if result.Scalar == nil {
			result.Scalar = cloneData(delta.Scalar)
		}
		result.Scalar.Value = &pb.DataEffect_FloatVal{FloatVal: baseVal + delta.Scalar.GetFloatVal()}
		result.Merge = base.Merge
		if base.Merge == pb.MergeRule_LAST_WRITE_WINS {
			result.Commutative = false
		}

	case pb.MergeRule_MAX_INT:
		baseVal := int64(0)
		if base.Scalar != nil {
			baseVal = base.Scalar.GetIntVal()
		}
		deltaVal := delta.Scalar.GetIntVal()
		if result.Scalar == nil {
			result.Scalar = cloneData(delta.Scalar)
		}
		if baseVal > deltaVal {
			result.Scalar.Value = &pb.DataEffect_IntVal{IntVal: baseVal}
		} else {
			result.Scalar.Value = &pb.DataEffect_IntVal{IntVal: deltaVal}
		}
		result.Merge = delta.Merge

	case pb.MergeRule_MAX_BYTES:
		a := base.Scalar.GetRaw()
		b := delta.Scalar.GetRaw()
		out := make([]byte, max(len(a), len(b)))
		copy(out, a)
		for i := range b {
			if i < len(out) && b[i] > out[i] {
				out[i] = b[i]
			}
		}
		if result.Scalar == nil {
			result.Scalar = cloneData(delta.Scalar)
		}
		result.Scalar.Value = &pb.DataEffect_Raw{Raw: out}
		result.Merge = delta.Merge

	default:
		// LWW: delta supersedes
		result.Scalar = cloneData(delta.Scalar)
		result.Merge = delta.Merge
		result.Commutative = false
	}

	return result
}

func composeKeyed(base, delta *pb.ReducedEffect) *pb.ReducedEffect {
	result := cloneReduced(base)
	if result.NetAdds == nil {
		result.NetAdds = make(map[string]*pb.ReducedElement)
	}
	if result.NetRemoves == nil {
		result.NetRemoves = make(map[string]bool)
	}

	// Apply delta's removes first (delta happened after base)
	for k := range delta.NetRemoves {
		delete(result.NetAdds, k)
		result.NetRemoves[k] = true
	}

	// Apply delta's adds — delta is newer, use mergeElements for
	// per-element composition (handles ADDITIVE score deltas correctly)
	for k, deltaElem := range delta.NetAdds {
		if baseElem, exists := result.NetAdds[k]; exists {
			result.NetAdds[k] = mergeElements(baseElem, deltaElem)
		} else {
			result.NetAdds[k] = deltaElem
		}
		delete(result.NetRemoves, k)
	}

	result.Hlc = delta.Hlc
	result.NodeId = delta.NodeId
	result.Subscribers = unionSubscribers(base.Subscribers, delta.Subscribers)
	if delta.TypeTag != pb.ValueType_TYPE_UNSPECIFIED {
		result.TypeTag = delta.TypeTag
	}
	if delta.ExpiresAt != nil || base.ExpiresAt != nil {
		result.ExpiresAt = delta.ExpiresAt
	}
	result.Collection = pb.CollectionKind_KEYED

	return result
}

func composeOrdered(base, delta *pb.ReducedEffect) *pb.ReducedEffect {
	result := cloneReduced(base)
	if result.NetRemoves == nil {
		result.NetRemoves = make(map[string]bool)
	}

	// Apply delta's removes to base elements
	for k := range delta.NetRemoves {
		result.NetRemoves[k] = true
	}
	if len(delta.NetRemoves) > 0 {
		filtered := make([]*pb.ReducedElement, 0, len(result.OrderedElements))
		for _, elem := range result.OrderedElements {
			if !delta.NetRemoves[string(elem.Data.Id)] {
				filtered = append(filtered, elem)
			}
		}
		result.OrderedElements = filtered
	}

	// Append new delta elements after base elements. composeSequential
	// guarantees delta is causally AFTER base, so delta elements always
	// come after base elements regardless of fork-choice hash.
	// Hash-based ordering of concurrent branches is handled by
	// mergeOrderedElements (via Merge2/MergeN), not here.
	baseIDs := make(map[string]bool, len(result.OrderedElements))
	for _, elem := range result.OrderedElements {
		baseIDs[string(elem.Data.Id)] = true
	}

	for _, elem := range delta.OrderedElements {
		if !baseIDs[string(elem.Data.Id)] {
			result.OrderedElements = append(result.OrderedElements, elem)
		}
	}
	result.Subscribers = unionSubscribers(base.Subscribers, delta.Subscribers)
	if delta.TypeTag != pb.ValueType_TYPE_UNSPECIFIED {
		result.TypeTag = delta.TypeTag
	}
	result.Collection = pb.CollectionKind_ORDERED

	return result
}
