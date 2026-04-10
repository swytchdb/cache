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

package cache

import (
	"unsafe"

	"github.com/zeebo/xxh3"
)

func hashKey[K Key](key K) uint64 {
	ptr := (*byte)(unsafe.Pointer(&key))
	return xxh3.Hash(unsafe.Slice(ptr, unsafe.Sizeof(key)))
}

// HashKey returns a hash for any Key type (exported for tiered package)
func HashKey[K Key](key K) uint64 {
	return hashKey(key)
}
