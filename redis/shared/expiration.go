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
	"container/heap"
	"sync"
	"time"
)

// MaxShortTTL is the maximum TTL for keys to be tracked in the expiration heap.
// Keys with longer TTLs are lazily expired on access.
const MaxShortTTL = 60 * time.Second

// expirationEntry represents a key scheduled for expiration
type expirationEntry struct {
	key      []byte
	dbIndex  int
	expireAt int64 // Unix milliseconds
}

// expirationHeap implements heap.Interface for expiration entries
// Ordered by expireAt (min-heap: soonest expiration at top)
type expirationHeap []expirationEntry

func (h expirationHeap) Len() int           { return len(h) }
func (h expirationHeap) Less(i, j int) bool { return h[i].expireAt < h[j].expireAt }
func (h expirationHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *expirationHeap) Push(x any) {
	*h = append(*h, x.(expirationEntry))
}

func (h *expirationHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

// DeleteFunc is called to delete an expired key
// dbIndex is the database index, key is the key to delete
type DeleteFunc func(dbIndex int, key []byte)

// ExpirationManager tracks keys with short TTLs for active expiration
type ExpirationManager struct {
	mu   sync.Mutex
	heap expirationHeap

	// Callback to delete expired keys
	deleteFunc DeleteFunc

	// Shutdown
	stopChan chan struct{}
	wg       sync.WaitGroup
}

// NewExpirationManager creates a new expiration manager
// deleteFunc is called for each expired key
func NewExpirationManager(deleteFunc DeleteFunc) *ExpirationManager {
	em := &ExpirationManager{
		heap:       make(expirationHeap, 0, 1024),
		deleteFunc: deleteFunc,
		stopChan:   make(chan struct{}),
	}
	heap.Init(&em.heap)
	return em
}

// Schedule adds a key to the expiration heap if its TTL is short enough
// expireAt is the absolute expiration time in Unix milliseconds
func (em *ExpirationManager) Schedule(dbIndex int, key []byte, expireAt int64) {
	// Only track keys expiring within MaxShortTTL
	now := time.Now().UnixMilli()
	if expireAt <= now {
		// Already expired, don't bother scheduling
		return
	}
	if expireAt-now > MaxShortTTL.Milliseconds() {
		// TTL too long, rely on lazy expiration
		return
	}

	em.mu.Lock()
	heap.Push(&em.heap, expirationEntry{
		key:      key,
		dbIndex:  dbIndex,
		expireAt: expireAt,
	})
	em.mu.Unlock()
}

// Start begins the background expiration goroutine
func (em *ExpirationManager) Start() {
	em.wg.Add(1)
	go em.expireLoop()
}

// Stop stops the background expiration goroutine
func (em *ExpirationManager) Stop() {
	close(em.stopChan)
	em.wg.Wait()
}

// expireLoop runs in the background, expiring keys as they become due
func (em *ExpirationManager) expireLoop() {
	defer em.wg.Done()

	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-em.stopChan:
			return
		case <-ticker.C:
			em.expireKeys()
		}
	}
}

// expireKeys processes all expired entries in the heap
func (em *ExpirationManager) expireKeys() {
	now := time.Now().UnixMilli()

	em.mu.Lock()
	defer em.mu.Unlock()

	// Process all expired entries
	for em.heap.Len() > 0 {
		// Peek at the top
		entry := em.heap[0]
		if entry.expireAt > now {
			// Not expired yet, we're done
			break
		}

		// Pop the expired entry
		heap.Pop(&em.heap)

		// Call the delete callback
		em.deleteFunc(entry.dbIndex, entry.key)
	}
}
