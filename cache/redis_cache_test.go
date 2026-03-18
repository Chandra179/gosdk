package cache

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// setupTestRedis creates a miniredis server for testing
func setupTestRedis(t *testing.T) (*miniredis.Miniredis, *RedisCache) {
	t.Helper()

	mr, err := miniredis.Run()
	require.NoError(t, err, "failed to start miniredis")

	cache := NewRedisCache(mr.Addr()).(*RedisCache)

	return mr, cache
}

func TestNewRedisCache(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

	cache := NewRedisCache(mr.Addr())

	assert.NotNil(t, cache)
	assert.IsType(t, &RedisCache{}, cache)

	// Verify the cache is functional
	redisCache := cache.(*RedisCache)
	assert.NotNil(t, redisCache.client)
}

func TestRedisCache_Set(t *testing.T) {
	mr, cache := setupTestRedis(t)
	defer mr.Close()

	ctx := context.Background()

	t.Run("successful set", func(t *testing.T) {
		err := cache.Set(ctx, "test-key", "test-value", time.Minute)
		assert.NoError(t, err)

		// Verify the value was set in miniredis
		val, err := mr.Get("test-key")
		assert.NoError(t, err)
		assert.Equal(t, "test-value", val)
	})

	t.Run("set with zero TTL", func(t *testing.T) {
		err := cache.Set(ctx, "no-ttl-key", "no-ttl-value", 0)
		assert.NoError(t, err)

		val, err := mr.Get("no-ttl-key")
		assert.NoError(t, err)
		assert.Equal(t, "no-ttl-value", val)
	})

	t.Run("set overwrites existing key", func(t *testing.T) {
		err := cache.Set(ctx, "overwrite-key", "original", time.Minute)
		assert.NoError(t, err)

		err = cache.Set(ctx, "overwrite-key", "updated", time.Minute)
		assert.NoError(t, err)

		val, err := mr.Get("overwrite-key")
		assert.NoError(t, err)
		assert.Equal(t, "updated", val)
	})

	t.Run("set with TTL expires", func(t *testing.T) {
		err := cache.Set(ctx, "expire-key", "expire-value", 1*time.Second)
		assert.NoError(t, err)

		// Fast forward time in miniredis
		mr.FastForward(2 * time.Second)

		exists := mr.Exists("expire-key")
		assert.False(t, exists)
	})
}

func TestRedisCache_Get(t *testing.T) {
	mr, cache := setupTestRedis(t)
	defer mr.Close()

	ctx := context.Background()

	t.Run("successful get", func(t *testing.T) {
		require.NoError(t, mr.Set("existing-key", "existing-value"))

		val, err := cache.Get(ctx, "existing-key")
		assert.NoError(t, err)
		assert.Equal(t, "existing-value", val)
	})

	t.Run("get non-existent key", func(t *testing.T) {
		val, err := cache.Get(ctx, "non-existent-key")
		assert.Error(t, err)
		assert.Equal(t, redis.Nil, err)
		assert.Empty(t, val)
	})

	t.Run("get expired key", func(t *testing.T) {
		require.NoError(t, mr.Set("expired-key", "expired-value"))
		mr.SetTTL("expired-key", 1*time.Second)
		mr.FastForward(2 * time.Second)

		val, err := cache.Get(ctx, "expired-key")
		assert.Error(t, err)
		assert.Equal(t, redis.Nil, err)
		assert.Empty(t, val)
	})
}

func TestRedisCache_Del(t *testing.T) {
	mr, cache := setupTestRedis(t)
	defer mr.Close()

	ctx := context.Background()

	t.Run("delete existing key", func(t *testing.T) {
		require.NoError(t, mr.Set("delete-key", "delete-value"))

		err := cache.Del(ctx, "delete-key")
		assert.NoError(t, err)

		exists := mr.Exists("delete-key")
		assert.False(t, exists)
	})

	t.Run("delete non-existent key", func(t *testing.T) {
		err := cache.Del(ctx, "non-existent-delete-key")
		assert.NoError(t, err) // Redis Del doesn't error on non-existent keys
	})

	t.Run("delete multiple times", func(t *testing.T) {
		require.NoError(t, mr.Set("multi-delete-key", "value"))

		err := cache.Del(ctx, "multi-delete-key")
		assert.NoError(t, err)

		// Delete again
		err = cache.Del(ctx, "multi-delete-key")
		assert.NoError(t, err)
	})
}

func TestRedisCache_SetNX(t *testing.T) {
	mr, cache := setupTestRedis(t)
	defer mr.Close()

	ctx := context.Background()

	t.Run("setnx on non-existent key", func(t *testing.T) {
		ok, err := cache.SetNX(ctx, "setnx-key", "setnx-value", time.Minute)
		assert.NoError(t, err)
		assert.True(t, ok)

		val, err := mr.Get("setnx-key")
		assert.NoError(t, err)
		assert.Equal(t, "setnx-value", val)
	})

	t.Run("setnx on existing key", func(t *testing.T) {
		require.NoError(t, mr.Set("existing-setnx-key", "original-value"))

		ok, err := cache.SetNX(ctx, "existing-setnx-key", "new-value", time.Minute)
		assert.NoError(t, err)
		assert.False(t, ok)

		// Verify original value is unchanged
		val, err := mr.Get("existing-setnx-key")
		assert.NoError(t, err)
		assert.Equal(t, "original-value", val)
	})

	t.Run("setnx with TTL", func(t *testing.T) {
		ok, err := cache.SetNX(ctx, "setnx-ttl-key", "setnx-ttl-value", 1*time.Second)
		assert.NoError(t, err)
		assert.True(t, ok)

		// Verify TTL is set
		ttl := mr.TTL("setnx-ttl-key")
		assert.True(t, ttl > 0)

		// Fast forward and verify expiration
		mr.FastForward(2 * time.Second)
		exists := mr.Exists("setnx-ttl-key")
		assert.False(t, exists)
	})

	t.Run("setnx after key expires", func(t *testing.T) {
		ok, err := cache.SetNX(ctx, "expire-then-setnx", "first-value", 1*time.Second)
		assert.NoError(t, err)
		assert.True(t, ok)

		// Fast forward to expire the key
		mr.FastForward(2 * time.Second)

		// Now SetNX should succeed
		ok, err = cache.SetNX(ctx, "expire-then-setnx", "second-value", time.Minute)
		assert.NoError(t, err)
		assert.True(t, ok)

		val, err := mr.Get("expire-then-setnx")
		assert.NoError(t, err)
		assert.Equal(t, "second-value", val)
	})
}

func TestRedisCache_ContextCancellation(t *testing.T) {
	mr, cache := setupTestRedis(t)
	defer mr.Close()

	t.Run("set with cancelled context", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel immediately

		err := cache.Set(ctx, "cancel-key", "cancel-value", time.Minute)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "context canceled")
	})

	t.Run("get with cancelled context", func(t *testing.T) {
		require.NoError(t, mr.Set("cancel-get-key", "value"))

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		_, err := cache.Get(ctx, "cancel-get-key")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "context canceled")
	})
}

func TestRedisCache_Ping(t *testing.T) {
	mr, cache := setupTestRedis(t)
	defer mr.Close()

	ctx := context.Background()

	t.Run("successful ping", func(t *testing.T) {
		err := cache.Ping(ctx)
		assert.NoError(t, err)
	})

	t.Run("ping after close returns error", func(t *testing.T) {
		cache2 := NewRedisCache(mr.Addr()).(*RedisCache)
		cache2.Close()

		err := cache2.Ping(ctx)
		assert.Error(t, err)
	})
}

func TestRedisCache_Close(t *testing.T) {
	t.Run("successful close", func(t *testing.T) {
		mr, err := miniredis.Run()
		require.NoError(t, err)
		defer mr.Close()

		cache := NewRedisCache(mr.Addr()).(*RedisCache)

		err = cache.Close()
		assert.NoError(t, err)
	})

	t.Run("close multiple times", func(t *testing.T) {
		mr, err := miniredis.Run()
		require.NoError(t, err)
		defer mr.Close()

		cache := NewRedisCache(mr.Addr()).(*RedisCache)

		err = cache.Close()
		assert.NoError(t, err)

		// Close again - redis.Client errors when closing already-closed client
		err = cache.Close()
		assert.Error(t, err)
	})
}

func TestRedisCache_IntegrationScenario(t *testing.T) {
	mr, cache := setupTestRedis(t)
	defer mr.Close()

	ctx := context.Background()

	// Scenario: Distributed lock pattern
	t.Run("distributed lock pattern", func(t *testing.T) {
		lockKey := "resource-lock"
		lockValue := "process-1"

		// Acquire lock
		acquired, err := cache.SetNX(ctx, lockKey, lockValue, 5*time.Second)
		assert.NoError(t, err)
		assert.True(t, acquired)

		// Try to acquire again (should fail)
		acquired, err = cache.SetNX(ctx, lockKey, "process-2", 5*time.Second)
		assert.NoError(t, err)
		assert.False(t, acquired)

		// Release lock
		err = cache.Del(ctx, lockKey)
		assert.NoError(t, err)

		// Now acquire should succeed
		acquired, err = cache.SetNX(ctx, lockKey, "process-2", 5*time.Second)
		assert.NoError(t, err)
		assert.True(t, acquired)
	})

	// Scenario: Cache with refresh
	t.Run("cache refresh pattern", func(t *testing.T) {
		cacheKey := "user:123"

		// Set initial value
		err := cache.Set(ctx, cacheKey, "user-data-v1", time.Minute)
		assert.NoError(t, err)

		// Get value
		val, err := cache.Get(ctx, cacheKey)
		assert.NoError(t, err)
		assert.Equal(t, "user-data-v1", val)

		// Update cache
		err = cache.Set(ctx, cacheKey, "user-data-v2", time.Minute)
		assert.NoError(t, err)

		// Get updated value
		val, err = cache.Get(ctx, cacheKey)
		assert.NoError(t, err)
		assert.Equal(t, "user-data-v2", val)

		// Delete cache
		err = cache.Del(ctx, cacheKey)
		assert.NoError(t, err)

		// Verify deletion
		_, err = cache.Get(ctx, cacheKey)
		assert.Error(t, err)
		assert.Equal(t, redis.Nil, err)
	})
}
