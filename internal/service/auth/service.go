package auth

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"gosdk/internal/service/session"
	"gosdk/pkg/db"
	"gosdk/pkg/oauth2"
)

type Service struct {
	oauth2Manager *oauth2.Manager
	sessionStore  session.Client
	userRepo      *UserRepository
}

func NewService(sessionStore session.Client, database db.SQLExecutor) *Service {
	return &Service{
		sessionStore: sessionStore,
		userRepo:     NewUserRepository(database),
	}
}

// InitiateLogin generates the OAuth2 authorization URL
func (s *Service) InitiateLogin(provider string) (string, error) {
	return s.oauth2Manager.GetAuthURL(provider)
}

// HandleCallback processes OAuth2 callback and creates a session
// This is the callback handler that gets invoked by oauth2.Manager
func (s *Service) HandleCallback(ctx context.Context, provider string,
	userInfo *oauth2.UserInfo, tokenSet *oauth2.TokenSet) (*oauth2.CallbackInfo, error) {
	internalUserID, err := s.GetOrCreateUser(ctx, provider, userInfo.ID, userInfo.Email, userInfo.Name)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve user: %w", ErrUserNotFound)
	}

	sessionData := &SessionData{
		UserID:   internalUserID,
		TokenSet: tokenSet,
		Provider: provider,
	}

	data, err := json.Marshal(sessionData)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal session data: %w", errors.New("invalid input"))
	}

	sess, err := s.sessionStore.Create(data, SessionTimeout)
	if err != nil {
		return nil, fmt.Errorf("failed to create session: %w", ErrSessionNotFound)
	}

	return &oauth2.CallbackInfo{
		SessionID:         sess.ID,
		UserID:            internalUserID,
		SessionCookieName: SessionCookieName,
		CookieMaxAge:      CookieMaxAge,
	}, nil
}

// OAuthCallbackHandler returns a callback handler function for OAuth2 authentication.
// This is used to break the circular dependency between oauth2.Manager and auth.Service.
// The handler is wired after both components are created.
func (s *Service) OAuthCallbackHandler() oauth2.CallbackHandler {
	return func(ctx context.Context, provider string, userInfo *oauth2.UserInfo, tokenSet *oauth2.TokenSet) (*oauth2.CallbackInfo, error) {
		return s.HandleCallback(ctx, provider, userInfo, tokenSet)
	}
}

// SetOAuth2Manager sets the OAuth2 manager after initialization.
// This is used to break the circular dependency between oauth2.Manager and auth.Service.
func (s *Service) SetOAuth2Manager(manager *oauth2.Manager) {
	s.oauth2Manager = manager
}

// GetSessionData retrieves and unmarshals session data
func (s *Service) GetSessionData(sessionID string) (*SessionData, error) {
	sess, err := s.sessionStore.Get(sessionID)
	if err != nil {
		return nil, err
	}

	var sessionData SessionData
	if err := json.Unmarshal(sess.Data, &sessionData); err != nil {
		return nil, fmt.Errorf("failed to unmarshal session data: %w", errors.New("invalid input"))
	}

	return &sessionData, nil
}

// RefreshToken refreshes the access token for a session
func (s *Service) RefreshToken(ctx context.Context, sessionID string) error {
	sessionData, err := s.GetSessionData(sessionID)
	if err != nil {
		return err
	}

	if sessionData.TokenSet.RefreshToken == "" {
		return ErrNoRefreshToken
	}

	provider, err := s.oauth2Manager.GetProvider(sessionData.Provider)
	if err != nil {
		return err
	}

	googleProvider, ok := provider.(*oauth2.GoogleOIDCProvider)
	if !ok {
		return ErrProviderNotFound
	}

	newTokenSet, err := googleProvider.RefreshToken(ctx, sessionData.TokenSet.RefreshToken)
	if err != nil {
		if deleteErr := s.sessionStore.Delete(sessionID); deleteErr != nil {
			return fmt.Errorf("failed to delete session after token refresh failure: %w", deleteErr)
		}
		return fmt.Errorf("failed to refresh token (session deleted): %w", ErrInvalidToken)
	}

	sessionData.TokenSet = newTokenSet
	data, err := json.Marshal(sessionData)
	if err != nil {
		return fmt.Errorf("failed to marshal session data: %w", errors.New("invalid input"))
	}

	return s.sessionStore.Update(sessionID, data)
}

// ValidateAndRefreshSession validates a session and refreshes tokens if needed
func (s *Service) ValidateAndRefreshSession(ctx context.Context, sessionID string) (*SessionData, error) {
	sessionData, err := s.GetSessionData(sessionID)
	if err != nil {
		return nil, err
	}

	if time.Until(sessionData.TokenSet.ExpiresAt) <= TokenRefreshLeeway {
		if err := s.RefreshToken(ctx, sessionID); err != nil {
			return nil, err
		}
	}

	return sessionData, nil
}

// GetOrCreateUser finds existing user by provider and subject_id or creates new user
func (s *Service) GetOrCreateUser(ctx context.Context, provider, subjectID, email, fullName string) (string, error) {
	user, err := s.userRepo.GetOrCreate(ctx, provider, subjectID, email, fullName)
	if err != nil {
		return "", fmt.Errorf("failed to get or create user: %w", err)
	}
	return user.ID.String(), nil
}

// DeleteSession deletes a session (logout)
func (s *Service) DeleteSession(sessionID string) error {
	return s.sessionStore.Delete(sessionID)
}
