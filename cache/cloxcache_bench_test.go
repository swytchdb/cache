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
	"math"
	"math/rand"
	"sync"
	"testing"
)

// Zipf distribution generator for realistic hotspot patterns
type zipfGenerator struct {
	rng      *rand.Rand
	n        uint64
	theta    float64
	alpha    float64
	zetan    float64
	eta      float64
	zetabase float64
}

func newZipfGenerator(n uint64, theta float64, seed int64) *zipfGenerator {
	z := &zipfGenerator{
		rng:   rand.New(rand.NewSource(seed)),
		n:     n,
		theta: theta,
		alpha: 1.0 / (1.0 - theta),
	}

	z.zetabase = z.zetaN(0, 2)
	z.zetan = z.zetaN(0, n)
	z.eta = (1.0 - math.Pow(2.0/float64(n), 1.0-theta)) / (1.0 - z.zetabase/z.zetan)

	return z
}

func (z *zipfGenerator) zetaN(start, end uint64) float64 {
	sum := 0.0
	for i := start; i < end; i++ {
		sum += 1.0 / math.Pow(float64(i+1), z.theta)
	}
	return sum
}

func (z *zipfGenerator) next() uint64 {
	u := z.rng.Float64()
	uz := u * z.zetan

	if uz < 1.0 {
		return 0
	}

	if uz < 1.0+math.Pow(0.5, z.theta) {
		return 1
	}

	return uint64((float64(z.n) * math.Pow(z.eta*u-z.eta+1.0, z.alpha)))
}

// BenchmarkCloxCacheGet benchmarks Get operations
func BenchmarkCloxCacheGet(b *testing.B) {
	cfg := Config{
		NumShards:     128,
		SlotsPerShard: 4096,
	}
	cache := NewCloxCache[uint64, int](cfg)
	defer cache.Close()

	// Pre-populate cache
	const numKeys = 10000
	for i := range numKeys {
		cache.Put(uint64(i), i)
	}

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			cache.Get(uint64(i%numKeys), 0)
			i++
		}
	})
}

// BenchmarkSyncMapGet benchmarks sync.Map Get operations
func BenchmarkSyncMapGet(b *testing.B) {
	var m sync.Map

	// Pre-populate map
	const numKeys = 10000
	for i := range numKeys {
		m.Store(uint64(i), i)
	}

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			m.Load(uint64(i % numKeys))
			i++
		}
	})
}

// BenchmarkCloxCachePut benchmarks Put operations
func BenchmarkCloxCachePut(b *testing.B) {
	cfg := Config{
		NumShards:     128,
		SlotsPerShard: 4096,
	}
	cache := NewCloxCache[uint64, int](cfg)
	defer cache.Close()

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			cache.Put(uint64(i), i)
			i++
		}
	})
}

// BenchmarkSyncMapPut benchmarks sync.Map Store operations
func BenchmarkSyncMapPut(b *testing.B) {
	var m sync.Map

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			m.Store(uint64(i), i)
			i++
		}
	})
}

// BenchmarkCloxCacheMixed benchmarks 80/20 read/write workload
func BenchmarkCloxCacheMixed(b *testing.B) {
	cfg := Config{
		NumShards:     128,
		SlotsPerShard: 4096,
	}
	cache := NewCloxCache[uint64, int](cfg)
	defer cache.Close()

	// Pre-populate cache
	const numKeys = 10000
	for i := range numKeys {
		cache.Put(uint64(i), i)
	}

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		rng := rand.New(rand.NewSource(42))
		i := 0
		for pb.Next() {
			key := uint64(i % numKeys)
			if rng.Float64() < 0.8 {
				cache.Get(key, 0)
			} else {
				cache.Put(key, i)
			}
			i++
		}
	})

	hits, misses, evictions := cache.Stats()
	hitRate := float64(hits) / float64(hits+misses) * 100
	b.ReportMetric(hitRate, "hit%")
	b.ReportMetric(float64(evictions), "evictions")
}

// BenchmarkSyncMapMixed benchmarks sync.Map with 80/20 read/write workload
func BenchmarkSyncMapMixed(b *testing.B) {
	var m sync.Map

	// Pre-populate map
	const numKeys = 10000
	for i := range numKeys {
		m.Store(uint64(i), i)
	}

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		rng := rand.New(rand.NewSource(42))
		i := 0
		for pb.Next() {
			key := uint64(i % numKeys)
			if rng.Float64() < 0.8 {
				m.Load(key)
			} else {
				m.Store(key, i)
			}
			i++
		}
	})
}

// BenchmarkCloxCacheZipf benchmarks with Zipf distribution (realistic hotspots)
func BenchmarkCloxCacheZipf(b *testing.B) {
	cfg := Config{
		NumShards:     128,
		SlotsPerShard: 4096,
	}
	cache := NewCloxCache[uint64, int](cfg)
	defer cache.Close()

	const numKeys = 100000
	const theta = 0.99 // Zipf parameter (higher = more skewed)

	// Pre-populate cache with subset of keys
	for i := range numKeys / 10 {
		cache.Put(uint64(i), i)
	}

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		zipf := newZipfGenerator(numKeys, theta, 42)
		for pb.Next() {
			idx := zipf.next()
			if rand.Float64() < 0.8 {
				cache.Get(idx, 0)
			} else {
				cache.Put(idx, int(idx))
			}
		}
	})

	hits, misses, evictions := cache.Stats()
	hitRate := float64(hits) / float64(hits+misses) * 100
	b.ReportMetric(hitRate, "hit%")
	b.ReportMetric(float64(evictions), "evictions")
}

// BenchmarkSyncMapZipf benchmarks sync.Map with Zipf distribution
func BenchmarkSyncMapZipf(b *testing.B) {
	var m sync.Map

	const numKeys = 100000
	const theta = 0.99

	// Pre-populate map with subset of keys
	for i := range numKeys / 10 {
		m.Store(uint64(i), i)
	}

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		zipf := newZipfGenerator(numKeys, theta, 42)
		for pb.Next() {
			idx := zipf.next()
			if rand.Float64() < 0.8 {
				m.Load(idx)
			} else {
				m.Store(idx, int(idx))
			}
		}
	})
}

// BenchmarkCloxCacheContention benchmarks high contention on hot keys
func BenchmarkCloxCacheContention(b *testing.B) {
	cfg := Config{
		NumShards:     128,
		SlotsPerShard: 4096,
	}
	cache := NewCloxCache[uint64, int](cfg)
	defer cache.Close()

	// Pre-populate with hot keys
	const numHotKeys = 100
	for i := range numHotKeys {
		cache.Put(uint64(i), i)
	}

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		rng := rand.New(rand.NewSource(42))
		for pb.Next() {
			key := uint64(rng.Intn(numHotKeys))
			if rng.Float64() < 0.9 {
				cache.Get(key, 0)
			} else {
				cache.Put(key, rng.Intn(1000))
			}
		}
	})
}

// BenchmarkSyncMapContention benchmarks sync.Map with high contention
func BenchmarkSyncMapContention(b *testing.B) {
	var m sync.Map

	// Pre-populate with hot keys
	const numHotKeys = 100
	for i := range numHotKeys {
		m.Store(uint64(i), i)
	}

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		rng := rand.New(rand.NewSource(42))
		for pb.Next() {
			key := uint64(rng.Intn(numHotKeys))
			if rng.Float64() < 0.9 {
				m.Load(key)
			} else {
				m.Store(key, rng.Intn(1000))
			}
		}
	})
}

// BenchmarkCloxCacheSizes benchmarks different cache sizes
func BenchmarkCloxCacheSizes(b *testing.B) {
	sizes := []struct {
		name          string
		numShards     int
		slotsPerShard int
	}{
		{"Small", 16, 256},
		{"Medium", 64, 1024},
		{"Large", 128, 4096},
		{"XLarge", 256, 8192},
	}

	for _, size := range sizes {
		b.Run(size.name, func(b *testing.B) {
			cfg := Config{
				NumShards:     size.numShards,
				SlotsPerShard: size.slotsPerShard,
			}
			cache := NewCloxCache[uint64, int](cfg)
			defer cache.Close()

			// Pre-populate
			const numKeys = 10000
			for i := range numKeys {
				cache.Put(uint64(i), i)
			}

			b.ResetTimer()
			b.ReportAllocs()

			b.RunParallel(func(pb *testing.PB) {
				rng := rand.New(rand.NewSource(42))
				i := 0
				for pb.Next() {
					key := uint64(i % numKeys)
					if rng.Float64() < 0.8 {
						cache.Get(key, 0)
					} else {
						cache.Put(key, i)
					}
					i++
				}
			})
		})
	}
}

// BenchmarkCloxCachePointers benchmarks with pointer types
func BenchmarkCloxCachePointers(b *testing.B) {
	type Record struct {
		ID   int
		Data [128]byte
	}

	cfg := Config{
		NumShards:     128,
		SlotsPerShard: 4096,
	}
	cache := NewCloxCache[uint64, *Record](cfg)
	defer cache.Close()

	// Pre-populate
	const numKeys = 10000
	for i := range numKeys {
		record := &Record{ID: i}
		cache.Put(uint64(i), record)
	}

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			cache.Get(uint64(i%numKeys), 0)
			i++
		}
	})
}
