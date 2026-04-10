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
	"strconv"
	"testing"
	"time"
)

// countGhosts counts ghost entries (freq <= 0) across all shards
func (c *CloxCache[K, V]) countGhosts() int {
	count := 0
	for shardID := range c.shards {
		shard := &c.shards[shardID]
		for slotID := range shard.slots {
			node := shard.slots[slotID].Load()
			for node != nil {
				if node.freq.Load() <= 0 {
					count++
				}
				node = node.next.Load()
			}
		}
	}
	return count
}

// countLiveEntries counts live entries (freq > 0) across all shards
func (c *CloxCache[K, V]) countLiveEntries() int {
	count := 0
	for shardID := range c.shards {
		shard := &c.shards[shardID]
		for slotID := range shard.slots {
			node := shard.slots[slotID].Load()
			for node != nil {
				if node.freq.Load() > 0 {
					count++
				}
				node = node.next.Load()
			}
		}
	}
	return count
}

// TestGhostsAccumulateWithoutSweeper shows that ghosts accumulate up to capacity.
// With large shards and small sweep percent, the CLOCK hand takes many cycles
// to rotate back to a ghost's slot, so ghosts hold their values longer than needed.
func TestGhostsAccumulateWithoutSweeper(t *testing.T) {
	// Use large slots relative to capacity so ghosts spread across many slots.
	// Capacity = 1024, but 8192 slots per shard means most slots are never scanned.
	cfg := Config{
		NumShards:     4,
		SlotsPerShard: 8192,
		Capacity:      1024, // small capacity forces lots of evictions
		CollectStats:  true,
		SweepPercent:  5, // only 5% scanned per eviction = ~410 slots out of 8192
	}
	cache := NewCloxCache[string, int](cfg)
	defer cache.Close()

	// Fill to capacity, then keep inserting to force evictions → ghosts
	for i := range 20000 {
		key := "ghost-test-" + strconv.Itoa(i)
		cache.Put(key, i)
	}

	ghosts := cache.countGhosts()
	live := cache.countLiveEntries()

	t.Logf("after 20k inserts into 1024-capacity cache:")
	t.Logf("  live=%d, ghosts=%d, total_nodes=%d", live, ghosts, live+ghosts)
	t.Logf("  ghost count from shards: %d", totalGhostCount(cache))

	// The bug: with large shards and small sweep, ghosts accumulate beyond
	// the ghost capacity because evictFromShard only culls ghosts it can see
	// in its scan window.
	if ghosts == 0 {
		t.Skip("no ghosts created (may have sizeFunc set or ghostCapacity=0)")
	}

	// There should be ghosts accumulated — this documents the existing behavior
	t.Logf("  ghost capacity per shard: %d", cache.shards[0].ghostCapacity)
	totalGhostCap := int64(0)
	for i := range cache.shards {
		totalGhostCap += cache.shards[i].ghostCapacity
	}
	t.Logf("  total ghost capacity: %d", totalGhostCap)

	// Ghost count should be within capacity — the CLOCK hand self-limits by
	// falling back to full eviction when it can't find a ghost to replace.
	// The issue isn't exceeding capacity, it's that ghosts hold values in memory
	// until the hand naturally rotates back to their slot.
	if int64(ghosts) > totalGhostCap {
		t.Logf("  UNEXPECTED: ghosts (%d) exceed total ghost capacity (%d)", ghosts, totalGhostCap)
	}
}

func totalGhostCount[K Key, V any](c *CloxCache[K, V]) int64 {
	var total int64
	for i := range c.shards {
		total += c.shards[i].ghostCount.Load()
	}
	return total
}

// TestGhostSweeperCullsGhostsUnderMildPressure verifies the background ghost
// sweeper removes ghosts when memory limit is set but not severely exceeded.
// This simulates a nearcache that is moderately memory-constrained.
func TestGhostSweeperCullsGhostsUnderMildPressure(t *testing.T) {
	cfg := Config{
		NumShards:     4,
		SlotsPerShard: 8192,
		Capacity:      1024,
		CollectStats:  true,
		SweepPercent:  5,
	}
	cache := NewCloxCache[string, int](cfg)
	defer cache.Close()

	// Fill and overflow to create ghosts
	for i := range 20000 {
		key := "sweep-test-" + strconv.Itoa(i)
		cache.Put(key, i)
	}

	ghostsBefore := cache.countGhosts()
	t.Logf("ghosts before sweeper: %d", ghostsBefore)

	if ghostsBefore == 0 {
		t.Skip("no ghosts to sweep")
	}

	// Set a memory limit that's lower than current RSS to trigger pressure
	// This is what EnforceMemoryTarget does in practice
	cache.memoryLimit.Store(1024) // tiny limit = pressure

	// Start the ghost sweeper
	cache.StartGhostSweeper(500 * time.Millisecond)

	// Wait for a few sweep cycles
	time.Sleep(2 * time.Second)

	ghostsAfter := cache.countGhosts()
	t.Logf("ghosts after sweeper: %d (was %d)", ghostsAfter, ghostsBefore)

	if ghostsAfter >= ghostsBefore {
		t.Errorf("ghost sweeper did not reduce ghosts: before=%d, after=%d",
			ghostsBefore, ghostsAfter)
	}

	// Under pressure, all ghosts should be culled
	if ghostsAfter > 0 {
		t.Errorf("expected 0 ghosts under pressure, got %d", ghostsAfter)
	}
}

// TestGhostSweeperUnderMemoryPressure verifies that the ghost sweeper
// culls ALL ghosts when memory pressure is high (memory target active).
func TestGhostSweeperUnderMemoryPressure(t *testing.T) {
	cfg := Config{
		NumShards:     4,
		SlotsPerShard: 4096,
		Capacity:      2048,
		CollectStats:  true,
		SweepPercent:  10,
	}
	cache := NewCloxCache[string, []byte](cfg)
	cache.SetSizeFunc(func(k string, v []byte) int64 {
		return int64(len(k) + len(v))
	})
	defer cache.Close()

	// Note: with sizeFunc set, ghosts are disabled in evictFromShard
	// So we test the sweeper on a cache WITHOUT sizeFunc
	cache2 := NewCloxCache[string, int](cfg)
	defer cache2.Close()

	// Create ghosts
	for i := range 30000 {
		key := "pressure-" + strconv.Itoa(i)
		cache2.Put(key, i)
	}

	ghostsBefore := cache2.countGhosts()
	t.Logf("ghosts before: %d", ghostsBefore)

	if ghostsBefore == 0 {
		t.Skip("no ghosts created")
	}

	// Simulate memory pressure by setting a very low memory limit
	cache2.memoryLimit.Store(1) // 1 byte = extreme pressure

	// Start sweeper
	cache2.StartGhostSweeper(200 * time.Millisecond)

	// Wait for sweep under pressure
	time.Sleep(2 * time.Second)

	ghostsAfter := cache2.countGhosts()
	t.Logf("ghosts after pressure sweep: %d (was %d)", ghostsAfter, ghostsBefore)

	// Under memory pressure, ALL ghosts should be culled
	if ghostsAfter > 0 {
		t.Errorf("expected 0 ghosts under memory pressure, got %d", ghostsAfter)
	}
}

// TestGhostSweeperStopsOnClose verifies the sweeper goroutine exits cleanly.
func TestGhostSweeperStopsOnClose(t *testing.T) {
	cfg := Config{
		NumShards:     4,
		SlotsPerShard: 256,
		Capacity:      256,
	}
	cache := NewCloxCache[string, int](cfg)

	cache.StartGhostSweeper(100 * time.Millisecond)

	// Close should not hang
	done := make(chan struct{})
	go func() {
		cache.Close()
		close(done)
	}()

	select {
	case <-done:
		// OK
	case <-time.After(5 * time.Second):
		t.Fatal("Close() hung — ghost sweeper goroutine did not exit")
	}
}
