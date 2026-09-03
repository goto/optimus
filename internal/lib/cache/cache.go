// Package cache provides a generic in-memory TTL cache with request coalescing,
// intended for reuse across Optimus wherever a fan-out of concurrent lookups shares a
// smaller set of underlying keys (e.g. many different queries referencing overlapping
// tables).
package cache

import (
	"context"
	"sync"
	"time"
)

// defaultJanitorInterval caps how often the background sweep runs, so a very long TTL
// doesn't leave one giant wait before the first cleanup pass.
const defaultJanitorInterval = time.Minute

type entry[V any] struct {
	value     V
	expiresAt time.Time
}

type inflightCall[V any] struct {
	done  chan struct{}
	value V
	err   error
}

// Cache is a generic in-memory TTL cache. Concurrent GetOrLoad calls for the same key
// share one underlying load rather than each hitting the source independently
// (protects against a stampede when many callers miss the same cold key at once), and
// expired entries are periodically swept so keys that stop being requested don't
// accumulate in memory forever.
//
// Failed loads are never cached -- every call to GetOrLoad for a key whose last load
// failed will call load again.
type Cache[K comparable, V any] struct {
	ttl      time.Duration
	mu       sync.Mutex
	items    map[K]entry[V]
	inflight map[K]*inflightCall[V]

	stopOnce sync.Once
	stop     chan struct{}
}

// New creates a Cache with the given TTL. A ttl <= 0 disables caching entirely: every
// GetOrLoad call invokes load directly and nothing is ever stored. This lets a caller
// wire a Cache unconditionally off of a config value without a separate "is caching
// enabled" branch.
func New[K comparable, V any](ttl time.Duration) *Cache[K, V] {
	c := &Cache[K, V]{
		ttl:      ttl,
		items:    make(map[K]entry[V]),
		inflight: make(map[K]*inflightCall[V]),
		stop:     make(chan struct{}),
	}
	if ttl > 0 {
		go c.runJanitor()
	}
	return c
}

// GetOrLoad returns the cached value for key if present and unexpired. Otherwise it
// calls load exactly once -- even under concurrent callers racing on the same key --
// and, if load succeeds, caches the result for the configured TTL.
func (c *Cache[K, V]) GetOrLoad(ctx context.Context, key K, load func(ctx context.Context) (V, error)) (V, error) {
	if c.ttl <= 0 {
		return load(ctx)
	}

	c.mu.Lock()
	if e, ok := c.items[key]; ok && time.Now().Before(e.expiresAt) {
		c.mu.Unlock()
		return e.value, nil
	}
	if call, ok := c.inflight[key]; ok {
		c.mu.Unlock()
		select {
		case <-call.done:
			return call.value, call.err
		case <-ctx.Done():
			var zero V
			return zero, ctx.Err()
		}
	}

	call := &inflightCall[V]{done: make(chan struct{})}
	c.inflight[key] = call
	c.mu.Unlock()

	value, err := load(ctx)
	call.value, call.err = value, err
	close(call.done)

	c.mu.Lock()
	delete(c.inflight, key)
	if err == nil {
		c.items[key] = entry[V]{value: value, expiresAt: time.Now().Add(c.ttl)}
	}
	c.mu.Unlock()

	return value, err
}

// Invalidate removes key from the cache, if present. A no-op if caching is disabled
// (ttl <= 0) or the key isn't cached.
func (c *Cache[K, V]) Invalidate(key K) {
	c.mu.Lock()
	delete(c.items, key)
	c.mu.Unlock()
}

// Len reports the number of unexpired entries currently held. For observability only,
// not intended for hot-path use.
func (c *Cache[K, V]) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now()
	n := 0
	for _, e := range c.items {
		if now.Before(e.expiresAt) {
			n++
		}
	}
	return n
}

// Close stops the background janitor goroutine. Safe to call multiple times, and safe
// to never call if ttl <= 0 (no goroutine was ever started in that case).
func (c *Cache[K, V]) Close() {
	c.stopOnce.Do(func() {
		close(c.stop)
	})
}

func (c *Cache[K, V]) runJanitor() {
	interval := c.ttl
	if interval > defaultJanitorInterval {
		interval = defaultJanitorInterval
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.sweep()
		case <-c.stop:
			return
		}
	}
}

func (c *Cache[K, V]) sweep() {
	now := time.Now()
	c.mu.Lock()
	for k, e := range c.items {
		if !now.Before(e.expiresAt) {
			delete(c.items, k)
		}
	}
	c.mu.Unlock()
}
