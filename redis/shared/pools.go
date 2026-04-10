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

package shared

import (
	"bytes"
	"sync"
)

// Buffer sizes for optimal performance
// Larger buffers reduce syscall frequency at the cost of memory per connection
const (
	DefaultBufSize  = 32768   // 32KB for write buffers - reduces write syscalls
	ReadBufSize     = 65536   // 64KB read buffer - better pipelining support
	maxDataBufSize  = 1 << 20 // 1MB max pooled data buffer
	ResponseBufSize = 8192    // 8KB response buffer initial size
)

// commandPool pools Command structs
var commandPool = sync.Pool{
	New: func() any {
		return &Command{
			Args: make([][]byte, 0, 8), // Pre-allocate for common commands
		}
	},
}

// writerPool pools Writer structs
var writerPool = sync.Pool{
	New: func() any {
		buf := &bytes.Buffer{}
		buf.Grow(ResponseBufSize)
		return &Writer{buf: buf}
	},
}

// dataBufPools - tiered pools for data buffers of various sizes
var dataBufPools = [...]sync.Pool{
	{New: func() any { b := make([]byte, 64); return &b }},      // 0: 64B
	{New: func() any { b := make([]byte, 256); return &b }},     // 1: 256B
	{New: func() any { b := make([]byte, 1024); return &b }},    // 2: 1KB
	{New: func() any { b := make([]byte, 4096); return &b }},    // 3: 4KB
	{New: func() any { b := make([]byte, 16384); return &b }},   // 4: 16KB
	{New: func() any { b := make([]byte, 65536); return &b }},   // 5: 64KB
	{New: func() any { b := make([]byte, 262144); return &b }},  // 6: 256KB
	{New: func() any { b := make([]byte, 1048576); return &b }}, // 7: 1MB
}

// dataBufSizes maps pool index to buffer size
var dataBufSizes = [...]int{64, 256, 1024, 4096, 16384, 65536, 262144, 1048576}

// sizeClassIndex returns the pool index for the given size using O(1) bit math
// Pool sizes: 64, 256, 1K, 4K, 16K, 64K, 256K, 1M (indices 0-7)
func sizeClassIndex(n int) int {
	if n <= 64 {
		return 0
	}
	if n <= 256 {
		return 1
	}
	if n <= 1024 {
		return 2
	}
	if n <= 4096 {
		return 3
	}
	if n <= 16384 {
		return 4
	}
	if n <= 65536 {
		return 5
	}
	if n <= 262144 {
		return 6
	}
	return 7
}

// GetDataBuf gets a byte buffer of at least size n from the appropriate pool
func GetDataBuf(n int) []byte {
	if n > maxDataBufSize {
		// Too large to pool
		return make([]byte, n)
	}

	poolIdx := sizeClassIndex(n)
	buf := dataBufPools[poolIdx].Get().(*[]byte)
	if cap(*buf) < n {
		// Pool returned smaller buffer than expected - return it to the pool
		// before allocating a new one to avoid memory leak
		dataBufPools[poolIdx].Put(buf)
		return make([]byte, n)
	}
	return (*buf)[:n]
}

// PutDataBuf returns a byte buffer to the appropriate pool
func PutDataBuf(buf []byte) {
	if buf == nil || cap(buf) > maxDataBufSize {
		return
	}

	// Find the right pool based on capacity
	c := cap(buf)
	for i := len(dataBufSizes) - 1; i >= 0; i-- {
		if c >= dataBufSizes[i] {
			b := buf[:cap(buf)]
			dataBufPools[i].Put(&b)
			return
		}
	}
}

// GetCommand gets a Command from the pool
func GetCommand() *Command {
	cmd := commandPool.Get().(*Command)
	cmd.Reset()
	return cmd
}

// PutCommand returns a Command to the pool
func PutCommand(cmd *Command) {
	if cmd == nil {
		return
	}
	// Return data buffers if poolable
	for _, arg := range cmd.Args {
		if arg != nil {
			PutDataBuf(arg)
		}
	}
	cmd.Args = cmd.Args[:0]
	commandPool.Put(cmd)
}

// GetWriter gets a Writer from the pool
func GetWriter() *Writer {
	w := writerPool.Get().(*Writer)
	w.buf.Reset()
	return w
}

// PutWriter returns a Writer to the pool
func PutWriter(w *Writer) {
	if w == nil || w.buf == nil {
		return
	}
	if w.buf.Cap() > ResponseBufSize*4 {
		// Don't pool overly large buffers
		return
	}
	writerPool.Put(w)
}
