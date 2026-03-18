package oauth2

import (
	"errors"
	"sync"
	"time"
)

var (
	ErrStateNotFound = errors.New("state not found")
	ErrStateExpired  = errors.New("state expired")
)

// StateStorage interface for state, nonce, and PKCE verifier management
type StateStorage interface {
	SaveState(state string, nonce string, codeVerifier string, expiresAt time.Time) error
	GetStateData(state string) (*StateData, error)
	DeleteState(state string) error
	Cleanup()
}

// StateData holds all security parameters for OAuth2 flow
type StateData struct {
	Nonce        string
	CodeVerifier string
	ExpiresAt    time.Time
}

// InMemoryStorage implements StateStorage interface using in-memory map
// Note: This implementation is suitable for single-instance deployments.
// For multi-instance/production use, implement StateStorage with Redis or similar.
type InMemoryStorage struct {
	mu   sync.RWMutex
	data map[string]*StateData
	done chan struct{}
	once sync.Once
}

// NewInMemoryStorage creates a new in-memory state storage with background cleanup
func NewInMemoryStorage() *InMemoryStorage {
	s := &InMemoryStorage{
		data: make(map[string]*StateData),
		done: make(chan struct{}),
	}
	go s.cleanupRoutine()
	return s
}

// SaveState stores state data with expiration time
// This method is safe for concurrent use
func (s *InMemoryStorage) SaveState(state string, nonce string, codeVerifier string, expiresAt time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[state] = &StateData{
		Nonce:        nonce,
		CodeVerifier: codeVerifier,
		ExpiresAt:    expiresAt,
	}
	return nil
}

// GetStateData retrieves state data by state string
// Returns ErrStateNotFound if state doesn't exist
// Returns ErrStateExpired if state exists but has expired (and deletes it)
// This method is safe for concurrent use
func (s *InMemoryStorage) GetStateData(state string) (*StateData, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	data, exists := s.data[state]
	if !exists {
		return nil, ErrStateNotFound
	}

	// Check expiration and delete immediately if expired
	if time.Now().After(data.ExpiresAt) {
		delete(s.data, state) // Clean up expired state immediately
		return nil, ErrStateExpired
	}

	return data, nil
}

// DeleteState removes state data from storage
// This method is safe for concurrent use
func (s *InMemoryStorage) DeleteState(state string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.data, state)
	return nil
}

// Cleanup stops the background cleanup routine
// Safe to call multiple times
func (s *InMemoryStorage) Cleanup() {
	s.once.Do(func() {
		close(s.done)
	})
}

// cleanupRoutine runs periodically to remove expired states
func (s *InMemoryStorage) cleanupRoutine() {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			s.removeExpired()
		case <-s.done:
			return
		}
	}
}

// removeExpired deletes all expired states from storage
func (s *InMemoryStorage) removeExpired() {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	for state, data := range s.data {
		if now.After(data.ExpiresAt) {
			delete(s.data, state)
		}
	}
}
