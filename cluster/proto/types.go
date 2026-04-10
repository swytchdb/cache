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

package proto

import (
	"crypto/rand"
	"encoding/binary"
	"time"
)

// NodeID is an ephemeral node identifier: 32 bits seconds-since-epoch | 32 bits random.
// Generated fresh on every startup — never reused across restarts.
type NodeID uint64

// EffectSeq identifies an effect globally: [0] = NodeID, [1] = monotonic offset.
// Supports == and != (array comparison) and works as a map key.
// NOT orderable by design — do not sort or compare with < / >.
type EffectSeq [2]uint64

// NewNodeID generates a fresh ephemeral node ID.
func NewNodeID() NodeID {
	var buf [4]byte
	_, _ = rand.Read(buf[:])
	random := uint64(binary.LittleEndian.Uint32(buf[:]))
	seconds := uint64(time.Now().Unix())
	return NodeID((seconds << 32) | random)
}

func (n NodeID) Uint64() uint64 { return uint64(n) }

func (e EffectSeq) NodeID() NodeID { return NodeID(e[0]) }
func (e EffectSeq) Offset() uint64 { return e[1] }

func NewEffectSeq(nodeID NodeID, offset uint64) EffectSeq {
	return EffectSeq{uint64(nodeID), offset}
}

// ToRef converts an EffectSeq to its protobuf representation.
func (e EffectSeq) ToRef() *EffectRef {
	return &EffectRef{
		NodeId: uint64(e[0]),
		Offset: e[1],
	}
}

// EffectSeqFromRef converts a protobuf EffectRef to an EffectSeq.
func EffectSeqFromRef(ref *EffectRef) EffectSeq {
	if ref == nil {
		return EffectSeq{}
	}
	return EffectSeq{ref.NodeId, ref.Offset}
}
