package cache_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/goto/optimus/internal/lib/cache"
)

func TestCache(t *testing.T) {
	ctx := context.Background()

	t.Run("caches a successful load and does not call load again before expiry", func(t *testing.T) {
		c := cache.New[string, int](time.Minute)
		defer c.Close()

		var calls int32
		load := func(context.Context) (int, error) {
			atomic.AddInt32(&calls, 1)
			return 42, nil
		}

		v1, err := c.GetOrLoad(ctx, "k", load)
		require.NoError(t, err)
		assert.Equal(t, 42, v1)

		v2, err := c.GetOrLoad(ctx, "k", load)
		require.NoError(t, err)
		assert.Equal(t, 42, v2)

		assert.Equal(t, int32(1), atomic.LoadInt32(&calls))
	})

	t.Run("reloads after the entry expires", func(t *testing.T) {
		c := cache.New[string, int](10 * time.Millisecond)
		defer c.Close()

		var calls int32
		load := func(context.Context) (int, error) {
			n := atomic.AddInt32(&calls, 1)
			return int(n), nil
		}

		v1, err := c.GetOrLoad(ctx, "k", load)
		require.NoError(t, err)
		assert.Equal(t, 1, v1)

		time.Sleep(30 * time.Millisecond)

		v2, err := c.GetOrLoad(ctx, "k", load)
		require.NoError(t, err)
		assert.Equal(t, 2, v2)
	})

	t.Run("never caches a failed load", func(t *testing.T) {
		c := cache.New[string, int](time.Minute)
		defer c.Close()

		wantErr := errors.New("boom")
		var calls int32
		load := func(context.Context) (int, error) {
			atomic.AddInt32(&calls, 1)
			return 0, wantErr
		}

		_, err := c.GetOrLoad(ctx, "k", load)
		require.ErrorIs(t, err, wantErr)

		_, err = c.GetOrLoad(ctx, "k", load)
		require.ErrorIs(t, err, wantErr)

		assert.Equal(t, int32(2), atomic.LoadInt32(&calls))
	})

	t.Run("concurrent callers for the same cold key coalesce into one load", func(t *testing.T) {
		c := cache.New[string, int](time.Minute)
		defer c.Close()

		var calls int32
		release := make(chan struct{})
		load := func(context.Context) (int, error) { //nolint:unparam // signature is fixed by GetOrLoad
			atomic.AddInt32(&calls, 1)
			<-release // hold every concurrent caller here until we let go
			return 7, nil
		}

		const n = 20
		var wg sync.WaitGroup
		results := make([]int, n)
		for i := 0; i < n; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				v, err := c.GetOrLoad(ctx, "k", load)
				assert.NoError(t, err)
				results[i] = v
			}(i)
		}

		time.Sleep(20 * time.Millisecond) // let every goroutine reach the load call
		close(release)
		wg.Wait()

		assert.Equal(t, int32(1), atomic.LoadInt32(&calls))
		for _, v := range results {
			assert.Equal(t, 7, v)
		}
	})

	t.Run("ttl <= 0 disables caching, load runs every time", func(t *testing.T) {
		c := cache.New[string, int](0)
		defer c.Close()

		var calls int32
		load := func(context.Context) (int, error) {
			atomic.AddInt32(&calls, 1)
			return 1, nil
		}

		_, err := c.GetOrLoad(ctx, "k", load)
		require.NoError(t, err)
		_, err = c.GetOrLoad(ctx, "k", load)
		require.NoError(t, err)

		assert.Equal(t, int32(2), atomic.LoadInt32(&calls))
		assert.Equal(t, 0, c.Len())
	})

	t.Run("Invalidate removes a cached entry", func(t *testing.T) {
		c := cache.New[string, int](time.Minute)
		defer c.Close()

		var calls int32
		load := func(context.Context) (int, error) {
			n := atomic.AddInt32(&calls, 1)
			return int(n), nil
		}

		v1, _ := c.GetOrLoad(ctx, "k", load)
		assert.Equal(t, 1, v1)

		c.Invalidate("k")

		v2, _ := c.GetOrLoad(ctx, "k", load)
		assert.Equal(t, 2, v2)
	})

	t.Run("background janitor sweeps expired entries so Len reflects only live ones", func(t *testing.T) {
		c := cache.New[string, int](10 * time.Millisecond)
		defer c.Close()

		_, err := c.GetOrLoad(ctx, "k", func(context.Context) (int, error) { return 1, nil })
		require.NoError(t, err)
		assert.Equal(t, 1, c.Len())

		time.Sleep(100 * time.Millisecond) // well past ttl and at least one janitor tick

		assert.Equal(t, 0, c.Len())
	})

	t.Run("Close is safe to call multiple times", func(t *testing.T) {
		c := cache.New[string, int](time.Minute)
		c.Close()
		assert.NotPanics(t, c.Close)
	})

	t.Run("ctx cancellation while waiting on another caller's inflight load returns ctx.Err", func(t *testing.T) {
		c := cache.New[string, int](time.Minute)
		defer c.Close()

		release := make(chan struct{})
		started := make(chan struct{})
		go func() { //nolint:contextcheck // deliberately independent of the test's ctx, simulating an unrelated caller
			_, _ = c.GetOrLoad(context.Background(), "k", func(context.Context) (int, error) {
				close(started)
				<-release
				return 1, nil
			})
		}()

		<-started
		cancelCtx, cancel := context.WithCancel(ctx)
		cancel()

		_, err := c.GetOrLoad(cancelCtx, "k", func(context.Context) (int, error) {
			t.Fatal("load should not be called by the waiting caller")
			return 0, nil
		})
		require.ErrorIs(t, err, context.Canceled)

		close(release)
	})
}
