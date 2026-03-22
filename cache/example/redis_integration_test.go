package example

import (
	"context"
	"log"
	"os"
	"testing"
	"time"

	"cache"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go/modules/redis"
)

// sharedCache is populated once by TestMain and reused by every test.
var sharedCache cache.Cache

// TestMain starts one Redis container for the whole suite, runs the tests,
// then terminates the container.
func TestMain(m *testing.M) {
	ctx := context.Background()

	ctr, err := redis.Run(ctx, "redis:7-alpine")
	if err != nil {
		log.Fatalf("start redis container: %v", err)
	}

	addr, err := ctr.ConnectionString(ctx)
	if err != nil {
		log.Fatalf("get connection string: %v", err)
	}

	// ConnectionString returns "redis://host:port" — strip the scheme.
	addr = addr[len("redis://"):]

	sharedCache = cache.NewRedisCache(addr)

	code := m.Run()

	_ = sharedCache.Close()
	if err := ctr.Terminate(ctx); err != nil {
		log.Printf("warn: terminate container: %v", err)
	}

	os.Exit(code)
}

// newCache returns the shared cache and registers a per-test key cleanup so
// tests do not interfere with each other.
func newCache(t *testing.T, keys ...string) cache.Cache {
	t.Helper()
	t.Cleanup(func() {
		ctx := context.Background()
		for _, k := range keys {
			_ = sharedCache.Del(ctx, k)
		}
	})
	return sharedCache
}

// ---- Tests -----------------------------------------------------------------

func TestPing(t *testing.T) {
	c := newCache(t)
	err := c.Ping(context.Background())
	assert.NoError(t, err)
}

func TestSet_And_Get(t *testing.T) {
	c := newCache(t, "set-get-key")
	ctx := context.Background()

	require.NoError(t, c.Set(ctx, "set-get-key", "hello", time.Minute))

	val, err := c.Get(ctx, "set-get-key")
	require.NoError(t, err)
	assert.Equal(t, "hello", val)
}

func TestSet_Overwrites(t *testing.T) {
	c := newCache(t, "overwrite-key")
	ctx := context.Background()

	require.NoError(t, c.Set(ctx, "overwrite-key", "first", time.Minute))
	require.NoError(t, c.Set(ctx, "overwrite-key", "second", time.Minute))

	val, err := c.Get(ctx, "overwrite-key")
	require.NoError(t, err)
	assert.Equal(t, "second", val)
}

func TestSet_TTL_Expires(t *testing.T) {
	c := newCache(t, "ttl-key")
	ctx := context.Background()

	require.NoError(t, c.Set(ctx, "ttl-key", "value", 100*time.Millisecond))

	time.Sleep(300 * time.Millisecond)

	_, err := c.Get(ctx, "ttl-key")
	assert.Error(t, err, "key should have expired")
}

func TestGet_Miss(t *testing.T) {
	c := newCache(t)
	ctx := context.Background()

	_, err := c.Get(ctx, "non-existent-key")
	assert.Error(t, err)
}

func TestDel_ExistingKey(t *testing.T) {
	c := newCache(t, "del-key")
	ctx := context.Background()

	require.NoError(t, c.Set(ctx, "del-key", "value", time.Minute))
	require.NoError(t, c.Del(ctx, "del-key"))

	_, err := c.Get(ctx, "del-key")
	assert.Error(t, err)
}

func TestDel_NonExistentKey(t *testing.T) {
	c := newCache(t)
	ctx := context.Background()

	// Del on a missing key must not error
	err := c.Del(ctx, "missing-del-key")
	assert.NoError(t, err)
}

func TestSetNX_NewKey(t *testing.T) {
	c := newCache(t, "setnx-key")
	ctx := context.Background()

	ok, err := c.SetNX(ctx, "setnx-key", "value", time.Minute)
	require.NoError(t, err)
	assert.True(t, ok)

	val, err := c.Get(ctx, "setnx-key")
	require.NoError(t, err)
	assert.Equal(t, "value", val)
}

func TestSetNX_ExistingKey(t *testing.T) {
	c := newCache(t, "setnx-exists-key")
	ctx := context.Background()

	require.NoError(t, c.Set(ctx, "setnx-exists-key", "original", time.Minute))

	ok, err := c.SetNX(ctx, "setnx-exists-key", "new", time.Minute)
	require.NoError(t, err)
	assert.False(t, ok)

	val, err := c.Get(ctx, "setnx-exists-key")
	require.NoError(t, err)
	assert.Equal(t, "original", val)
}

func TestDistributedLock(t *testing.T) {
	c := newCache(t, "lock-key")
	ctx := context.Background()

	// Acquire lock
	ok, err := c.SetNX(ctx, "lock-key", "owner-1", 5*time.Second)
	require.NoError(t, err)
	require.True(t, ok, "first acquire must succeed")

	// Competing acquire must fail
	ok, err = c.SetNX(ctx, "lock-key", "owner-2", 5*time.Second)
	require.NoError(t, err)
	assert.False(t, ok, "second acquire must fail while lock is held")

	// Release and re-acquire
	require.NoError(t, c.Del(ctx, "lock-key"))

	ok, err = c.SetNX(ctx, "lock-key", "owner-2", 5*time.Second)
	require.NoError(t, err)
	assert.True(t, ok, "acquire after release must succeed")
}

func TestCacheRefresh(t *testing.T) {
	c := newCache(t, "refresh-key")
	ctx := context.Background()

	require.NoError(t, c.Set(ctx, "refresh-key", "v1", time.Minute))

	val, err := c.Get(ctx, "refresh-key")
	require.NoError(t, err)
	assert.Equal(t, "v1", val)

	require.NoError(t, c.Set(ctx, "refresh-key", "v2", time.Minute))

	val, err = c.Get(ctx, "refresh-key")
	require.NoError(t, err)
	assert.Equal(t, "v2", val)

	require.NoError(t, c.Del(ctx, "refresh-key"))

	_, err = c.Get(ctx, "refresh-key")
	assert.Error(t, err)
}
