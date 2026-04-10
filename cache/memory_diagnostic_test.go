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
	"fmt"
	"runtime"
	"strconv"
	"testing"
	"time"
)

// countEntries counts the actual number of entries in the cache
func (c *CloxCache[K, V]) countEntries() int {
	count := 0
	for shardID := range c.shards {
		shard := &c.shards[shardID]
		for slotID := range shard.slots {
			node := shard.slots[slotID].Load()
			for node != nil {
				count++
				node = node.next.Load()
			}
		}
	}
	return count
}

// getSlotStats returns the total number of slots and occupied slots
func (c *CloxCache[K, V]) getSlotStats() (totalSlots, occupiedSlots int) {
	totalSlots = c.numShards * len(c.shards[0].slots)
	for shardID := range c.shards {
		shard := &c.shards[shardID]
		for slotID := range shard.slots {
			if shard.slots[slotID].Load() != nil {
				occupiedSlots++
			}
		}
	}
	return
}

func TestMemoryAllocationDiagnostic(t *testing.T) {
	// Configure a small cache to exacerbate the issue
	// 16 shards × 1024 slots = 16,384 total slots
	cfg := Config{
		NumShards:     16,
		SlotsPerShard: 1024,
		CollectStats:  true,
	}

	keysToInsert := 50000
	totalSlots := cfg.NumShards * cfg.SlotsPerShard

	t.Logf("Cache configuration:")
	t.Logf("  NumShards: %d", cfg.NumShards)
	t.Logf("  SlotsPerShard: %d", cfg.SlotsPerShard)
	t.Logf("  Total slots: %d", totalSlots)
	t.Logf("  Keys to insert: %d", keysToInsert)
	t.Logf("  Overfill ratio: %.1fx", float64(keysToInsert)/float64(totalSlots))

	// Force GC and get baseline
	runtime.GC()
	var memBefore runtime.MemStats
	runtime.ReadMemStats(&memBefore)

	cache := NewCloxCache[string, int](cfg)

	// Track memory during insertion
	insertionStart := time.Now()

	// Insert keys rapidly
	for i := range keysToInsert {
		key := "key-" + strconv.Itoa(i)
		cache.Put(key, i)
	}

	insertionDuration := time.Since(insertionStart)

	// Get stats immediately after insertion
	var memAfter runtime.MemStats
	runtime.ReadMemStats(&memAfter)

	entriesAfterInsert := cache.countEntries()
	totalSlotsCheck, occupiedSlotsAfter := cache.getSlotStats()
	hits, misses, evictions := cache.Stats()

	allocDeltaKB := float64(memAfter.Alloc-memBefore.Alloc) / 1024

	t.Logf("\n=== Immediately after insertion ===")
	t.Logf("  Time to insert: %v", insertionDuration)
	t.Logf("  Memory delta: %.2f KB", allocDeltaKB)
	t.Logf("  Entries in cache: %d", entriesAfterInsert)
	t.Logf("  Total slots: %d", totalSlotsCheck)
	t.Logf("  Occupied slots: %d (%.1f%%)", occupiedSlotsAfter, float64(occupiedSlotsAfter)*100/float64(totalSlotsCheck))
	t.Logf("  Avg chain length: %.2f", float64(entriesAfterInsert)/float64(occupiedSlotsAfter))
	t.Logf("  Evictions so far: %d", evictions)
	t.Logf("  Hits: %d, Misses: %d", hits, misses)

	// With synchronous eviction, entries should already be at capacity
	t.Logf("\n=== Verifying capacity enforcement ===")

	// Final stats
	runtime.GC()
	var memFinal runtime.MemStats
	runtime.ReadMemStats(&memFinal)

	entriesFinal := cache.countEntries()
	_, occupiedSlotsFinal := cache.getSlotStats()
	_, _, evictionsFinal := cache.Stats()

	t.Logf("  Final entries: %d (target capacity: %d)", entriesFinal, totalSlots)
	t.Logf("  Occupied slots: %d", occupiedSlotsFinal)
	t.Logf("  Total evictions: %d", evictionsFinal)
	t.Logf("  Memory delta: %.2f KB", float64(memFinal.Alloc-memBefore.Alloc)/1024)

	cache.Close()

	// Analysis
	t.Logf("\n=== Analysis ===")
	if entriesAfterInsert > totalSlots {
		t.Logf("  ISSUE: Cache has %.1fx more entries than capacity", float64(entriesAfterInsert)/float64(totalSlots))
	} else {
		t.Logf("  OK: Entries (%d) within capacity (%d)", entriesAfterInsert, totalSlots)
	}
	if evictions > 0 {
		t.Logf("  OK: Synchronous eviction working - %d evictions during insertion", evictions)
	}
}

func BenchmarkMemoryAllocation50K(b *testing.B) {
	// This benchmark measures memory allocation when overfilling the cache
	// Run with: go test -bench=BenchmarkMemoryAllocation50K -benchmem -count=5

	cfg := Config{
		NumShards:     16,
		SlotsPerShard: 1024, // 16K slots
		CollectStats:  true,
	}

	keysToInsert := 50000

	for b.Loop() {
		var memBefore runtime.MemStats
		runtime.ReadMemStats(&memBefore)

		cache := NewCloxCache[string, int](cfg)

		for j := range keysToInsert {
			key := "key-" + strconv.Itoa(j)
			cache.Put(key, j)
		}

		var memAfter runtime.MemStats
		runtime.ReadMemStats(&memAfter)

		allocDelta := memAfter.Alloc - memBefore.Alloc
		b.ReportMetric(float64(allocDelta)/1024, "KB/op")

		entries := cache.countEntries()
		b.ReportMetric(float64(entries), "entries")

		cache.Close()
	}
}

// TestEvictionRateVsInsertionRate measures the steady-state behavior
func TestEvictionRateVsInsertionRate(t *testing.T) {
	cfg := Config{
		NumShards:     16,
		SlotsPerShard: 1024,
		CollectStats:  true,
	}

	cache := NewCloxCache[string, int](cfg)
	defer cache.Close()

	// First, fill the cache to capacity
	totalSlots := cfg.NumShards * cfg.SlotsPerShard
	t.Logf("Pre-filling cache with %d keys...", totalSlots)

	for i := range totalSlots {
		key := "prefill-" + strconv.Itoa(i)
		cache.Put(key, i)
	}

	// Wait for sweeper to stabilize
	time.Sleep(2 * time.Second)

	initialEntries := cache.countEntries()
	t.Logf("Entries after prefill and stabilization: %d", initialEntries)

	// Now measure insertion rate vs eviction rate
	t.Logf("\nTesting sustained insertion...")

	insertionRates := []int{100, 1000, 5000, 10000} // keys per second

	for _, rate := range insertionRates {
		// Reset counters
		cache.evictions.Store(0)

		entriesBefore := cache.countEntries()
		duration := 2 * time.Second
		keysToInsert := rate * int(duration.Seconds())
		interval := time.Second / time.Duration(rate)

		start := time.Now()
		insertedCount := 0
		rejectedCount := 0

		for i := range keysToInsert {
			key := fmt.Sprintf("rate-test-%d-%d", rate, i)
			if success, _, _, _ := cache.Put(key, i); success {
				insertedCount++
			} else {
				rejectedCount++
			}

			// Pace the insertions
			elapsed := time.Since(start)
			expected := time.Duration(i+1) * interval
			if elapsed < expected {
				time.Sleep(expected - elapsed)
			}
		}

		entriesAfter := cache.countEntries()
		evictions := cache.evictions.Load()

		t.Logf("  Rate %d keys/sec:", rate)
		t.Logf("    Inserted: %d, Rejected: %d", insertedCount, rejectedCount)
		t.Logf("    Evictions: %d (%.1f/sec)", evictions, float64(evictions)/duration.Seconds())
		t.Logf("    Entry growth: %d -> %d (delta: %+d)", entriesBefore, entriesAfter, entriesAfter-entriesBefore)
	}
}

// TestBurstInsertionMemoryGrowth tests memory growth during burst insertion
func TestBurstInsertionMemoryGrowth(t *testing.T) {
	cfg := Config{
		NumShards:     16,
		SlotsPerShard: 1024,
		CollectStats:  true,
	}

	cache := NewCloxCache[string, int](cfg)
	defer cache.Close()

	runtime.GC()
	var memBaseline runtime.MemStats
	runtime.ReadMemStats(&memBaseline)

	totalSlots := cfg.NumShards * cfg.SlotsPerShard

	t.Logf("Testing memory growth with increasing key counts...")
	t.Logf("Total slots: %d", totalSlots)

	keyCounts := []int{10000, 25000, 50000}

	for _, count := range keyCounts {
		// Create fresh cache
		cache.Close()
		runtime.GC()

		var memBefore runtime.MemStats
		runtime.ReadMemStats(&memBefore)

		cache = NewCloxCache[string, int](cfg)

		// Burst insert
		for i := range count {
			key := "burst-" + strconv.Itoa(i)
			cache.Put(key, i)
		}

		var memAfter runtime.MemStats
		runtime.ReadMemStats(&memAfter)

		entries := cache.countEntries()
		allocKB := float64(memAfter.Alloc-memBefore.Alloc) / 1024

		t.Logf("  %6d keys: entries=%d (%.1fx slots), mem=%.0fKB (%.1f bytes/key)",
			count, entries, float64(entries)/float64(totalSlots),
			allocKB, allocKB*1024/float64(count))
	}
}
