package oauth2

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewInMemoryStorage(t *testing.T) {
	storage := NewInMemoryStorage()
	require.NotNil(t, storage)
	assert.NotNil(t, storage.data)
	assert.NotNil(t, storage.done)

	// Cleanup
	storage.Cleanup()
}

func TestInMemoryStorage_SaveState(t *testing.T) {
	storage := NewInMemoryStorage()
	defer storage.Cleanup()

	expiresAt := time.Now().Add(5 * time.Minute)
	err := storage.SaveState("test-state", "test-nonce", "test-verifier", expiresAt)
	require.NoError(t, err)

	// Verify state was saved
	data, err := storage.GetStateData("test-state")
	require.NoError(t, err)
	assert.Equal(t, "test-nonce", data.Nonce)
	assert.Equal(t, "test-verifier", data.CodeVerifier)
	assert.Equal(t, expiresAt.Unix(), data.ExpiresAt.Unix())
}

func TestInMemoryStorage_GetStateData_NotFound(t *testing.T) {
	storage := NewInMemoryStorage()
	defer storage.Cleanup()

	_, err := storage.GetStateData("nonexistent-state")
	assert.ErrorIs(t, err, ErrStateNotFound)
}

func TestInMemoryStorage_GetStateData_Expired(t *testing.T) {
	storage := NewInMemoryStorage()
	defer storage.Cleanup()

	// Save expired state
	expiresAt := time.Now().Add(-1 * time.Second)
	err := storage.SaveState("expired-state", "nonce", "verifier", expiresAt)
	require.NoError(t, err)

	// Should return ErrStateExpired and delete the state
	_, err = storage.GetStateData("expired-state")
	assert.ErrorIs(t, err, ErrStateExpired)

	// Verify state was deleted
	_, err = storage.GetStateData("expired-state")
	assert.ErrorIs(t, err, ErrStateNotFound)
}

func TestInMemoryStorage_GetStateData_Valid(t *testing.T) {
	storage := NewInMemoryStorage()
	defer storage.Cleanup()

	expiresAt := time.Now().Add(5 * time.Minute)
	err := storage.SaveState("valid-state", "nonce-123", "verifier-456", expiresAt)
	require.NoError(t, err)

	data, err := storage.GetStateData("valid-state")
	require.NoError(t, err)
	assert.Equal(t, "nonce-123", data.Nonce)
	assert.Equal(t, "verifier-456", data.CodeVerifier)
}

func TestInMemoryStorage_DeleteState(t *testing.T) {
	storage := NewInMemoryStorage()
	defer storage.Cleanup()

	// Save and then delete
	expiresAt := time.Now().Add(5 * time.Minute)
	err := storage.SaveState("delete-me", "nonce", "verifier", expiresAt)
	require.NoError(t, err)

	err = storage.DeleteState("delete-me")
	require.NoError(t, err)

	// Verify deletion
	_, err = storage.GetStateData("delete-me")
	assert.ErrorIs(t, err, ErrStateNotFound)
}

func TestInMemoryStorage_DeleteState_Nonexistent(t *testing.T) {
	storage := NewInMemoryStorage()
	defer storage.Cleanup()

	// Should not error when deleting non-existent state
	err := storage.DeleteState("nonexistent")
	assert.NoError(t, err)
}

func TestInMemoryStorage_ConcurrentAccess(t *testing.T) {
	storage := NewInMemoryStorage()
	defer storage.Cleanup()

	var wg sync.WaitGroup
	numGoroutines := 100

	// Concurrent writes
	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func(idx int) {
			defer wg.Done()
			state := "state-" + string(rune(idx))
			expiresAt := time.Now().Add(5 * time.Minute)
			err := storage.SaveState(state, "nonce", "verifier", expiresAt)
			assert.NoError(t, err)
		}(i)
	}
	wg.Wait()

	// Concurrent reads
	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func(idx int) {
			defer wg.Done()
			state := "state-" + string(rune(idx))
			_, _ = storage.GetStateData(state)
		}(i)
	}
	wg.Wait()
}

func TestInMemoryStorage_CleanupRoutine(t *testing.T) {
	storage := NewInMemoryStorage()

	// Save expired states
	expiresAt := time.Now().Add(-1 * time.Second)
	for i := 0; i < 5; i++ {
		state := "expired-" + string(rune(i))
		err := storage.SaveState(state, "nonce", "verifier", expiresAt)
		require.NoError(t, err)
	}

	// Save valid states
	validExpiresAt := time.Now().Add(5 * time.Minute)
	for i := 0; i < 5; i++ {
		state := "valid-" + string(rune(i))
		err := storage.SaveState(state, "nonce", "verifier", validExpiresAt)
		require.NoError(t, err)
	}

	// Verify all states exist
	storage.mu.RLock()
	assert.Len(t, storage.data, 10)
	storage.mu.RUnlock()

	// Trigger cleanup
	storage.removeExpired()

	// Verify only valid states remain
	storage.mu.RLock()
	assert.Len(t, storage.data, 5)
	storage.mu.RUnlock()

	// Verify expired states are gone
	for i := 0; i < 5; i++ {
		state := "expired-" + string(rune(i))
		_, err := storage.GetStateData(state)
		assert.ErrorIs(t, err, ErrStateNotFound)
	}

	// Verify valid states still exist
	for i := 0; i < 5; i++ {
		state := "valid-" + string(rune(i))
		_, err := storage.GetStateData(state)
		assert.NoError(t, err)
	}

	storage.Cleanup()
}

func TestInMemoryStorage_Cleanup_Idempotent(t *testing.T) {
	storage := NewInMemoryStorage()

	// Cleanup should not panic when called multiple times
	assert.NotPanics(t, func() {
		storage.Cleanup()
		storage.Cleanup()
		storage.Cleanup()
	})
}
