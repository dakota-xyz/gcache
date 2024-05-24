package gcache

import (
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSimple(t *testing.T) {
	// Given items
	items := map[string]*baseCacheItem[string, int]{}
	for i := 0; i < 10; i++ {
		key := strconv.Itoa(i)
		items[key] = &baseCacheItem[string, int]{
			key:   key,
			value: i,
		}
	}

	// When we evict
	SimpleEvictionStrategy(items, 1)

	// Then the size reduces
	require.Len(t, items, 9)

	// When we evict more than one
	SimpleEvictionStrategy(items, 3)

	// Then the size reduces
	require.Len(t, items, 6)
}

func TestLRU(t *testing.T) {
	// Given items accessed now and for the next 10 seconds
	clock := NewFakeClock()
	items := map[string]*baseCacheItem[string, int]{}
	for i := 0; i < 10; i++ {
		key := strconv.Itoa(i)
		items[key] = &baseCacheItem[string, int]{
			clock:      clock,
			key:        key,
			value:      i,
			lastAccess: clock.Now().Add(time.Duration(i) * time.Second),
		}
	}

	// When we evict
	LRUStrategy(items, 1)

	// Then the we evict "now"
	require.Len(t, items, 9)
	now := clock.Now()
	for _, item := range items {
		require.True(t, item.lastAccess.After(now), "%v is at or before now (%v)",
			item.lastAccess, now)
	}

	// When we evict multiple
	LRUStrategy(items, 2)

	// Then the we evict "now+1" and "now+2"
	now = now.Add(2 * time.Second)
	require.Len(t, items, 7)
	for _, item := range items {
		require.True(t, item.lastAccess.After(now), "%v is at or before now (%v)",
			item.lastAccess, now)
	}
}

func TestLFU(t *testing.T) {
	// Given items accessed now and for the next 10 seconds
	items := map[string]*baseCacheItem[string, int]{}
	for i := 0; i < 10; i++ {
		key := strconv.Itoa(i)
		items[key] = &baseCacheItem[string, int]{
			key:         key,
			value:       i,
			accessCount: uint64(i),
		}
	}

	// When we evict
	LFUStrategy(items, 1)

	// Then the we evict "count 0"
	require.Len(t, items, 9)
	for _, item := range items {
		require.Greater(t, item.accessCount, uint64(0))
	}

	// When we evict multiple
	LFUStrategy(items, 2)

	// Then the we evict "count 1" and "count 2"
	require.Len(t, items, 7)
	for _, item := range items {
		require.Greater(t, item.accessCount, uint64(2))
	}
}
