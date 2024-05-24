package gcache

import (
	"sort"

	"golang.org/x/exp/maps"
)

// SimpleEvictionStrategy is an eviction strategy that randomly evicts items when needed
func SimpleEvictionStrategy[K comparable, V any](items map[K]*baseCacheItem[K, V], count int) {
	keys := maps.Keys(items)
	for _, key := range keys[:count] {
		delete(items, key)
	}
}

// LRUStrategy evicts least recently used
func LRUStrategy[K comparable, V any](items map[K]*baseCacheItem[K, V], count int) {
	if len(items) == 0 {
		return
	}
	if count > len(items) {
		count = len(items)
	}
	values := maps.Values(items)
	sort.Slice(values, func(i, j int) bool {
		return values[i].lastAccess.Before(values[j].lastAccess)
	})
	for _, toDelete := range values[:count] {
		delete(items, toDelete.key)
	}
}

// LFUStrategy evicts least frequently used
func LFUStrategy[K comparable, V any](items map[K]*baseCacheItem[K, V], count int) {
	if len(items) == 0 {
		return
	}
	if count > len(items) {
		count = len(items)
	}
	values := maps.Values(items)
	sort.Slice(values, func(i, j int) bool {
		return values[i].accessCount < values[j].accessCount
	})
	for _, toDelete := range values[:count] {
		delete(items, toDelete.key)
	}
}
