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
	"testing"
)

func TestConfigFromCapacity(t *testing.T) {
	tests := []struct {
		capacity  int
		minShards int
		maxShards int
		minSlots  int
	}{
		{capacity: 1000, minShards: 16, maxShards: 8192, minSlots: 64},
		{capacity: 10000, minShards: 16, maxShards: 8192, minSlots: 64},
		{capacity: 100000, minShards: 16, maxShards: 8192, minSlots: 64},
		{capacity: 1000000, minShards: 16, maxShards: 8192, minSlots: 64},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("capacity=%d", tt.capacity), func(t *testing.T) {
			cfg := ConfigFromCapacity(tt.capacity)

			// Verify config is valid
			if cfg.NumShards <= 0 {
				t.Errorf("Invalid NumShards: %d", cfg.NumShards)
			}
			if cfg.SlotsPerShard <= 0 {
				t.Errorf("Invalid SlotsPerShard: %d", cfg.SlotsPerShard)
			}
			if cfg.Capacity != tt.capacity {
				t.Errorf("Capacity mismatch: got %d, expected %d", cfg.Capacity, tt.capacity)
			}

			// Verify power of 2
			if cfg.NumShards&(cfg.NumShards-1) != 0 {
				t.Errorf("NumShards not power of 2: %d", cfg.NumShards)
			}
			if cfg.SlotsPerShard&(cfg.SlotsPerShard-1) != 0 {
				t.Errorf("SlotsPerShard not power of 2: %d", cfg.SlotsPerShard)
			}

			// Verify bounds
			if cfg.NumShards < tt.minShards || cfg.NumShards > tt.maxShards {
				t.Errorf("NumShards out of bounds: %d (expected %d-%d)",
					cfg.NumShards, tt.minShards, tt.maxShards)
			}
			if cfg.SlotsPerShard < tt.minSlots {
				t.Errorf("SlotsPerShard too small: %d (min: %d)",
					cfg.SlotsPerShard, tt.minSlots)
			}

			// Verify total slots >= capacity * 3 (for ghost support)
			totalSlots := cfg.NumShards * cfg.SlotsPerShard
			minTotalSlots := tt.capacity * 3
			if totalSlots < minTotalSlots {
				t.Errorf("Total slots too small: %d (min: %d for capacity %d)",
					totalSlots, minTotalSlots, tt.capacity)
			}

			t.Logf("Capacity: %d → shards=%d, slots/shard=%d, total_slots=%d (%.1fx capacity)",
				tt.capacity, cfg.NumShards, cfg.SlotsPerShard, totalSlots, float64(totalSlots)/float64(tt.capacity))
		})
	}
}

func TestConfigFromMemorySize(t *testing.T) {
	tests := []uint64{
		256 * 1024 * 1024,      // 256MB
		512 * 1024 * 1024,      // 512MB
		1024 * 1024 * 1024,     // 1GB
		2 * 1024 * 1024 * 1024, // 2GB
		4 * 1024 * 1024 * 1024, // 4GB
	}

	for _, size := range tests {
		t.Run(FormatMemory(size), func(t *testing.T) {
			cfg := ConfigFromMemorySize(size)

			// Verify config is valid
			if cfg.NumShards <= 0 {
				t.Errorf("Invalid NumShards: %d", cfg.NumShards)
			}
			if cfg.SlotsPerShard <= 0 {
				t.Errorf("Invalid SlotsPerShard: %d", cfg.SlotsPerShard)
			}
			if cfg.Capacity <= 0 {
				t.Errorf("Invalid Capacity: %d", cfg.Capacity)
			}

			// Verify power of 2
			if cfg.NumShards&(cfg.NumShards-1) != 0 {
				t.Errorf("NumShards not power of 2: %d", cfg.NumShards)
			}
			if cfg.SlotsPerShard&(cfg.SlotsPerShard-1) != 0 {
				t.Errorf("SlotsPerShard not power of 2: %d", cfg.SlotsPerShard)
			}

			// Estimate memory usage
			estimated := cfg.EstimateMemoryUsage()
			t.Logf("Target: %s → Config: shards=%d, slots=%d, capacity=%d → Estimated: %s",
				FormatMemory(size), cfg.NumShards, cfg.SlotsPerShard, cfg.Capacity, FormatMemory(estimated))

			// Estimated should be reasonably close to target
			// Allow for overhead, so check it's within 3x
			if estimated > size*3 {
				t.Errorf("Estimated memory too high: %s (target: %s)",
					FormatMemory(estimated), FormatMemory(size))
			}
		})
	}
}

func TestNextPowerOf2(t *testing.T) {
	tests := []struct {
		input    int
		expected int
	}{
		{0, 1},
		{1, 1},
		{2, 2},
		{3, 4},
		{4, 4},
		{5, 8},
		{7, 8},
		{8, 8},
		{15, 16},
		{16, 16},
		{17, 32},
		{100, 128},
		{256, 256},
		{1000, 1024},
		{1024, 1024},
		{1025, 2048},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("%d", tt.input), func(t *testing.T) {
			result := nextPowerOf2(tt.input)
			if result != tt.expected {
				t.Errorf("nextPowerOf2(%d) = %d, expected %d",
					tt.input, result, tt.expected)
			}

			// Verify it's actually a power of 2
			if result > 0 && result&(result-1) != 0 {
				t.Errorf("Result is not power of 2: %d", result)
			}
		})
	}
}

func TestFormatMemory(t *testing.T) {
	tests := []struct {
		bytes    uint64
		expected string
	}{
		{0, "0 B"},
		{100, "100 B"},
		{1023, "1023 B"},
		{1024, "1.0 KB"},
		{1536, "1.5 KB"},
		{1024 * 1024, "1.0 MB"},
		{1024 * 1024 * 1024, "1.0 GB"},
		{1536 * 1024 * 1024, "1.5 GB"},
		{4 * 1024 * 1024 * 1024, "4.0 GB"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			result := FormatMemory(tt.bytes)
			if result != tt.expected {
				t.Errorf("FormatMemory(%d) = %q, expected %q",
					tt.bytes, result, tt.expected)
			}
		})
	}
}

func TestEstimateMemoryUsage(t *testing.T) {
	configs := []Config{
		{NumShards: 16, SlotsPerShard: 256, Capacity: 1000},
		{NumShards: 64, SlotsPerShard: 1024, Capacity: 10000},
		{NumShards: 128, SlotsPerShard: 4096, Capacity: 100000},
	}

	for _, cfg := range configs {
		t.Run(fmt.Sprintf("capacity=%d", cfg.Capacity), func(t *testing.T) {
			estimated := cfg.EstimateMemoryUsage()

			if estimated == 0 {
				t.Error("Estimated memory should not be zero")
			}

			totalSlots := cfg.NumShards * cfg.SlotsPerShard
			t.Logf("Config: %d shards × %d slots = %d total slots, capacity=%d → %s estimated",
				cfg.NumShards, cfg.SlotsPerShard, totalSlots, cfg.Capacity, FormatMemory(estimated))
		})
	}
}

func BenchmarkConfigFromCapacity(b *testing.B) {
	b.ReportAllocs()

	for b.Loop() {
		_ = ConfigFromCapacity(100000)
	}
}

func BenchmarkConfigFromMemorySize(b *testing.B) {
	const memorySize = 1024 * 1024 * 1024 // 1GB

	b.ReportAllocs()

	for b.Loop() {
		_ = ConfigFromMemorySize(memorySize)
	}
}
