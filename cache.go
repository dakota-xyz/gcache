package gcache

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

const (
	TYPE_SIMPLE = "simple"
	TYPE_LRU    = "lru"
	TYPE_LFU    = "lfu"
	TYPE_ARC    = "arc"
)

var KeyNotFoundError = errors.New("Key not found.")

// CacheType defines the different cache types
type CacheType string

const (
	Simple CacheType = "SIMPLE"
	LRU    CacheType = "LRU"
	LFU    CacheType = "LFU"
)

// Cache is an interface to interact with the cache
type Cache[K comparable, V any] interface {
	// Set inserts or updates the specified key-value pair.
	Set(key K, value V)
	// SetWithExpire inserts or updates the specified key-value pair with an expiration time.
	SetWithExpire(key K, value V, expiration time.Duration) error
	// Get returns the value for the specified key if it is present in the cache.
	// If the key is not present in the cache and the cache has LoaderFunc,
	// invoke the `LoaderFunc` function and inserts the key-value pair in the cache.
	// If the key is not present in the cache and the cache does not have a LoaderFunc,
	// return KeyNotFoundError.
	Get(key K) (V, error)
	// GetIFPresent returns the value for the specified key if it is present in the cache.
	// Return KeyNotFoundError if the key is not present.
	GetIFPresent(key K) (V, error)
	// GetAll returns a map containing all key-value pairs in the cache.
	GetALL(checkExpired bool) map[K]V
	get(key K, onLoad bool) (V, error)
	// Remove removes the specified key from the cache if the key is present.
	// Returns true if the key was present and the key has been deleted.
	Remove(key K) bool
	// Purge removes all key-value pairs from the cache.
	Purge()
	// Keys returns a slice containing all keys in the cache.
	Keys(checkExpired bool) []K
	// Len returns the number of items in the cache.
	Len(checkExpired bool) int
	// Has returns true if the key exists in the cache.
	Has(key K) bool

	statsAccessor
}

type baseCacheItem[K comparable, V any] struct {
	clock       Clock
	key         K
	value       V
	expiration  *time.Time
	lastAccess  time.Time
	accessCount uint64
}

// IsExpired returns boolean value whether this item is expired or not.
func (b *baseCacheItem[_, _]) IsExpired(now *time.Time) bool {
	if b.expiration == nil {
		return false
	}
	if now == nil {
		t := b.clock.Now()
		now = &t
	}
	return b.expiration.Before(*now)
}

type baseCache[K comparable, V any] struct {
	clock            Clock
	size             int
	loaderExpireFunc LoaderExpireFunc[K, V]
	evictedFunc      EvictedFunc[K, V]
	purgeVisitorFunc PurgeVisitorFunc[K, V]
	addedFunc        AddedFunc[K, V]
	expiration       *time.Duration
	mu               sync.RWMutex
	loadGroup        Group[K, V]
	items            map[K]*baseCacheItem[K, V]
	evictionStrategy EvictionStrategy[K, V]
	*stats
}

type (
	LoaderFunc[K comparable, V any]       func(K) (V, error)
	LoaderExpireFunc[K comparable, V any] func(K) (V, *time.Duration, error)
	EvictedFunc[K comparable, V any]      func(K, V)
	PurgeVisitorFunc[K comparable, V any] func(K, V)
	AddedFunc[K comparable, V any]        func(K, V)
	DeserializeFunc[K comparable, V any]  func(K, interface{}) (V, error)
	SerializeFunc[K comparable, V any]    func(K, V) (interface{}, error)
	EvictionStrategy[K comparable, V any] func(map[K]*baseCacheItem[K, V], int)
)

type CacheBuilder[K comparable, V any] struct {
	clock            Clock
	tp               CacheType
	size             int
	loaderExpireFunc LoaderExpireFunc[K, V]
	evictedFunc      EvictedFunc[K, V]
	purgeVisitorFunc PurgeVisitorFunc[K, V]
	addedFunc        AddedFunc[K, V]
	expiration       *time.Duration
	deserializeFunc  DeserializeFunc[K, V]
	serializeFunc    SerializeFunc[K, V]
}

func New[K comparable, V any](size int) *CacheBuilder[K, V] {
	return &CacheBuilder[K, V]{
		clock: NewRealClock(),
		tp:    Simple,
		size:  size,
	}
}

func (cb *CacheBuilder[K, V]) Clock(clock Clock) *CacheBuilder[K, V] {
	cb.clock = clock
	return cb
}

// Set a loader function.
// loaderFunc: create a new value with this function if cached value is expired.
func (cb *CacheBuilder[K, V]) LoaderFunc(loaderFunc LoaderFunc[K, V]) *CacheBuilder[K, V] {
	cb.loaderExpireFunc = func(k K) (V, *time.Duration, error) {
		v, err := loaderFunc(k)
		return v, nil, err
	}
	return cb
}

// Set a loader function with expiration.
// loaderExpireFunc: create a new value with this function if cached value is expired.
// If nil returned instead of time.Duration from loaderExpireFunc than value will never expire.
func (cb *CacheBuilder[K, V]) LoaderExpireFunc(loaderExpireFunc LoaderExpireFunc[K, V]) *CacheBuilder[K, V] {
	cb.loaderExpireFunc = loaderExpireFunc
	return cb
}

func (cb *CacheBuilder[K, V]) EvictType(tp CacheType) *CacheBuilder[K, V] {
	cb.tp = tp
	return cb
}

func (cb *CacheBuilder[K, V]) Simple() *CacheBuilder[K, V] {
	return cb.EvictType(Simple)
}

func (cb *CacheBuilder[K, V]) LRU() *CacheBuilder[K, V] {
	return cb.EvictType(LRU)
}

func (cb *CacheBuilder[K, V]) LFU() *CacheBuilder[K, V] {
	return cb.EvictType(LFU)
}

func (cb *CacheBuilder[K, V]) EvictedFunc(evictedFunc EvictedFunc[K, V]) *CacheBuilder[K, V] {
	cb.evictedFunc = evictedFunc
	return cb
}

func (cb *CacheBuilder[K, V]) PurgeVisitorFunc(purgeVisitorFunc PurgeVisitorFunc[K, V]) *CacheBuilder[K, V] {
	cb.purgeVisitorFunc = purgeVisitorFunc
	return cb
}

func (cb *CacheBuilder[K, V]) AddedFunc(addedFunc AddedFunc[K, V]) *CacheBuilder[K, V] {
	cb.addedFunc = addedFunc
	return cb
}

func (cb *CacheBuilder[K, V]) DeserializeFunc(deserializeFunc DeserializeFunc[K, V]) *CacheBuilder[K, V] {
	cb.deserializeFunc = deserializeFunc
	return cb
}

func (cb *CacheBuilder[K, V]) SerializeFunc(serializeFunc SerializeFunc[K, V]) *CacheBuilder[K, V] {
	cb.serializeFunc = serializeFunc
	return cb
}

func (cb *CacheBuilder[K, V]) Expiration(expiration time.Duration) *CacheBuilder[K, V] {
	cb.expiration = &expiration
	return cb
}

func (cb *CacheBuilder[K, V]) Build() Cache[K, V] {
	if cb.size <= 0 && cb.tp != Simple {
		panic("gcache: Cache size <= 0")
	}

	return cb.build()
}

func (cb *CacheBuilder[K, V]) build() Cache[K, V] {
	c := &baseCache[K, V]{}
	if cb.size <= 0 {
		c.items = make(map[K]*baseCacheItem[K, V])
	} else {
		c.items = make(map[K]*baseCacheItem[K, V], cb.size)
	}
	c.loadGroup.cache = c
	switch cb.tp {
	case Simple:
		c.evictionStrategy = SimpleEvictionStrategy[K, V]
	case LRU:
		c.evictionStrategy = LRUStrategy[K, V]
	case LFU:
		c.evictionStrategy = LFUStrategy[K, V]
	default:
		panic("gcache: Unknown strategy type ")
	}
	return c
}

// load a new value using by specified key.
func (c *baseCache[K, V]) load(key K, cb func(V, *time.Duration, error) (V, error), isWait bool) (V, bool, error) {
	var zeroVal V
	v, called, err := c.loadGroup.Do(key, func() (v V, e error) {
		defer func() {
			if r := recover(); r != nil {
				e = fmt.Errorf("loader panics: %v", r)
			}
		}()
		return cb(c.loaderExpireFunc(key))
	}, isWait)
	if err != nil {
		return zeroVal, called, err
	}
	return v, called, nil
}

// Set a new key-value pair
func (c *baseCache[K, V]) Set(key K, value V) {
	c.mu.Lock()
	defer c.mu.Unlock()
	_ = c.set(key, value)
}

// Set a new key-value pair with an expiration time
func (c *baseCache[K, V]) SetWithExpire(key K, value V, expiration time.Duration) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	item := c.set(key, value)
	t := c.clock.Now().Add(expiration)
	item.expiration = &t
	return nil
}

func (c *baseCache[K, V]) set(key K, value V) *baseCacheItem[K, V] {
	// Check for existing item
	item, ok := c.items[key]
	if ok {
		item.value = value
	} else {
		// Verify size not exceeded
		if (len(c.items) >= c.size) && c.size > 0 {
			c.evict(1)
		}
		item = &baseCacheItem[K, V]{
			clock: c.clock,
			key:   key,
			value: value,
		}
		c.items[key] = item
	}

	if c.expiration != nil {
		t := c.clock.Now().Add(*c.expiration)
		item.expiration = &t
	}

	if c.addedFunc != nil {
		c.addedFunc(key, value)
	}

	item.lastAccess = c.clock.Now()
	return item
}

// Get a value from cache pool using key if it exists.
// If it does not exists key and has LoaderFunc,
// generate a value using `LoaderFunc` method returns value.
func (c *baseCache[K, V]) Get(key K) (V, error) {
	v, err := c.get(key, false)
	if err == KeyNotFoundError {
		return c.getWithLoader(key, true)
	}
	return v, err
}

// GetIFPresent gets a value from cache pool using key if it exists.
// If it does not exists key, returns KeyNotFoundError.
// And send a request which refresh value for specified key if cache object has LoaderFunc.
func (c *baseCache[K, V]) GetIFPresent(key K) (V, error) {
	v, err := c.get(key, false)
	if err == KeyNotFoundError {
		return c.getWithLoader(key, false)
	}
	return v, nil
}

func (c *baseCache[K, V]) get(key K, onLoad bool) (V, error) {
	v, err := c.getValue(key, onLoad)
	if err != nil {
		return v, err
	}
	return v, nil
}

func (c *baseCache[K, V]) getValue(key K, onLoad bool) (V, error) {
	c.mu.Lock()
	item, ok := c.items[key]
	if ok {
		if !item.IsExpired(nil) {
			v := item.value
			c.mu.Unlock()
			if !onLoad {
				c.stats.IncrHitCount()
			}
			return v, nil
		}
		c.remove(key)
	}
	c.mu.Unlock()
	if !onLoad {
		c.stats.IncrMissCount()
	}
	var zeroVal V
	return zeroVal, KeyNotFoundError
}

func (c *baseCache[K, V]) getWithLoader(key K, isWait bool) (V, error) {
	var zeroVal V
	if c.loaderExpireFunc == nil {
		return zeroVal, KeyNotFoundError
	}
	value, _, err := c.load(key, func(v V, expiration *time.Duration, e error) (V, error) {
		var zeroVal V
		if e != nil {
			return zeroVal, e
		}
		c.mu.Lock()
		defer c.mu.Unlock()
		item := c.set(key, v)
		if expiration != nil {
			t := c.clock.Now().Add(*expiration)
			item.expiration = &t
		}
		return v, nil
	}, isWait)
	if err != nil {
		return zeroVal, err
	}
	return value, nil
}

func (c *baseCache[K, V]) evict(count int) {
	now := c.clock.Now()
	if len(c.items) == 0 {
		return
	}
	if count > len(c.items)-1 {
		count = len(c.items) - 1
	}
	for key, item := range c.items {
		if count <= 0 {
			return
		}
		if item.expiration == nil || now.After(*item.expiration) {
			defer c.remove(key)
			count--
		}
	}
	c.evictionStrategy(c.items, count)
}

// Has checks if key exists in cache
func (c *baseCache[K, V]) Has(key K) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	now := time.Now()
	return c.has(key, &now)
}

func (c *baseCache[K, V]) has(key K, now *time.Time) bool {
	item, ok := c.items[key]
	if !ok {
		return false
	}
	return !item.IsExpired(now)
}

// Remove removes the provided key from the cache.
func (c *baseCache[K, V]) Remove(key K) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.remove(key)
}

func (c *baseCache[K, V]) remove(key K) bool {
	item, ok := c.items[key]
	if ok {
		delete(c.items, key)
		if c.evictedFunc != nil {
			c.evictedFunc(key, item.value)
		}
		return true
	}
	return false
}

// GetALL returns all key-value pairs in the cache.
func (c *baseCache[K, V]) GetALL(checkExpired bool) map[K]V {
	c.mu.RLock()
	defer c.mu.RUnlock()
	items := make(map[K]V, len(c.items))
	now := time.Now()
	for k, item := range c.items {
		if !checkExpired || c.has(k, &now) {
			items[k] = item.value
		}
	}
	return items
}

// Keys returns a slice of the keys in the cache.
func (c *baseCache[K, _]) Keys(checkExpired bool) []K {
	c.mu.RLock()
	defer c.mu.RUnlock()
	keys := make([]K, 0, len(c.items))
	now := time.Now()
	for k := range c.items {
		if !checkExpired || c.has(k, &now) {
			keys = append(keys, k)
		}
	}
	return keys
}

// Len returns the number of items in the cache.
func (c *baseCache[K, V]) Len(checkExpired bool) int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if !checkExpired {
		return len(c.items)
	}
	var length int
	now := time.Now()
	for k := range c.items {
		if c.has(k, &now) {
			length++
		}
	}
	return length
}

// Completely clear the cache
func (c *baseCache[K, V]) Purge() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.purgeVisitorFunc != nil {
		for key, item := range c.items {
			c.purgeVisitorFunc(key, item.value)
		}
	}
}
