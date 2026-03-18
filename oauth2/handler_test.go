package oauth2

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupTestRouter() *gin.Engine {
	gin.SetMode(gin.TestMode)
	return gin.New()
}

func TestGoogleCallbackHandler_MissingParams(t *testing.T) {
	router := setupTestRouter()

	config := DefaultManagerConfig()
	config.CallbackHandler = func(ctx context.Context, provider string, userInfo *UserInfo, tokenSet *TokenSet) (*CallbackInfo, error) {
		return &CallbackInfo{}, nil
	}

	manager, err := NewManager(context.Background(), config)
	require.NoError(t, err)

	router.GET("/callback", GoogleCallbackHandler(manager, nil))

	// Test missing code
	w := httptest.NewRecorder()
	req, _ := http.NewRequestWithContext(context.Background(), "GET", "/callback?state=test", nil)
	router.ServeHTTP(w, req)
	assert.Equal(t, http.StatusBadRequest, w.Code)
	assert.Contains(t, w.Body.String(), "missing code")

	// Test missing state
	w = httptest.NewRecorder()
	req, _ = http.NewRequestWithContext(context.Background(), "GET", "/callback?code=test", nil)
	router.ServeHTTP(w, req)
	assert.Equal(t, http.StatusBadRequest, w.Code)
	assert.Contains(t, w.Body.String(), "missing code") // Both are missing, so generic message

	// Test missing both
	w = httptest.NewRecorder()
	req, _ = http.NewRequestWithContext(context.Background(), "GET", "/callback", nil)
	router.ServeHTTP(w, req)
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestGoogleCallbackHandler_InvalidState(t *testing.T) {
	router := setupTestRouter()

	config := DefaultManagerConfig()
	config.CallbackHandler = func(ctx context.Context, provider string, userInfo *UserInfo, tokenSet *TokenSet) (*CallbackInfo, error) {
		return &CallbackInfo{}, nil
	}

	manager, err := NewManager(context.Background(), config)
	require.NoError(t, err)

	// Register provider
	mockProvider := &MockProvider{name: "google", authURL: "https://example.com/auth"}
	manager.RegisterProvider(mockProvider)

	router.GET("/callback", GoogleCallbackHandler(manager, nil))

	w := httptest.NewRecorder()
	req, _ := http.NewRequestWithContext(context.Background(), "GET", "/callback?code=test&state=invalid", nil)
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusUnauthorized, w.Code)
	assert.Contains(t, w.Body.String(), "invalid or expired state")
}

func TestGoogleCallbackHandler_Success(t *testing.T) {
	router := setupTestRouter()

	expectedCallbackInfo := &CallbackInfo{
		SessionCookieName: "test-session",
		UserID:            "user-123",
		SessionID:         "session-abc-xyz",
		CookieMaxAge:      3600,
	}

	config := DefaultManagerConfig()
	config.CallbackHandler = func(ctx context.Context, provider string, userInfo *UserInfo, tokenSet *TokenSet) (*CallbackInfo, error) {
		return expectedCallbackInfo, nil
	}

	manager, err := NewManager(context.Background(), config)
	require.NoError(t, err)

	// Register mock provider
	mockProvider := &MockProvider{
		name:    "google",
		authURL: "https://example.com/auth",
	}
	manager.RegisterProvider(mockProvider)

	// Generate valid state
	authURL, err := manager.GetAuthURL("google")
	require.NoError(t, err)
	state := extractStateFromURL(authURL)

	handlerConfig := &HandlerConfig{
		SecureCookies:  true,
		CookiePath:     "/",
		RequestTimeout: 30 * time.Second,
	}

	router.GET("/callback", GoogleCallbackHandler(manager, handlerConfig))

	w := httptest.NewRecorder()
	req, _ := http.NewRequestWithContext(context.Background(), "GET", "/callback?code=auth-code&state="+state, nil)
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, w.Body.String(), "authenticated")
	assert.Contains(t, w.Body.String(), "user-123")

	// Verify cookie was set
	cookies := w.Result().Cookies()
	require.Len(t, cookies, 1)
	assert.Equal(t, "test-session", cookies[0].Name)
	assert.Equal(t, "session-abc-xyz", cookies[0].Value)
	assert.True(t, cookies[0].HttpOnly)
	assert.True(t, cookies[0].Secure)
	assert.Equal(t, "/", cookies[0].Path)
}

func TestGoogleCallbackHandler_CallbackHandlerError(t *testing.T) {
	router := setupTestRouter()

	config := DefaultManagerConfig()
	config.CallbackHandler = func(ctx context.Context, provider string, userInfo *UserInfo, tokenSet *TokenSet) (*CallbackInfo, error) {
		return nil, errors.New("callback error")
	}

	manager, err := NewManager(context.Background(), config)
	require.NoError(t, err)

	mockProvider := &MockProvider{
		name:    "google",
		authURL: "https://example.com/auth",
	}
	manager.RegisterProvider(mockProvider)

	authURL, err := manager.GetAuthURL("google")
	require.NoError(t, err)
	state := extractStateFromURL(authURL)

	router.GET("/callback", GoogleCallbackHandler(manager, nil))

	w := httptest.NewRecorder()
	req, _ := http.NewRequestWithContext(context.Background(), "GET", "/callback?code=auth-code&state="+state, nil)
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusInternalServerError, w.Code)
}

func TestGoogleCallbackHandler_Timeout(t *testing.T) {
	router := setupTestRouter()

	config := DefaultManagerConfig()
	config.CallbackHandler = func(ctx context.Context, provider string, userInfo *UserInfo, tokenSet *TokenSet) (*CallbackInfo, error) {
		// Simulate slow operation
		select {
		case <-time.After(500 * time.Millisecond):
			return &CallbackInfo{}, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	manager, err := NewManager(context.Background(), config)
	require.NoError(t, err)

	mockProvider := &MockProvider{
		name:    "google",
		authURL: "https://example.com/auth",
		handleCallback: func(ctx context.Context, code, state, nonce, codeVerifier string) (*UserInfo, *TokenSet, error) {
			// Simulate slow provider
			select {
			case <-time.After(500 * time.Millisecond):
				return &UserInfo{}, &TokenSet{}, nil
			case <-ctx.Done():
				return nil, nil, ctx.Err()
			}
		},
	}
	manager.RegisterProvider(mockProvider)

	authURL, err := manager.GetAuthURL("google")
	require.NoError(t, err)
	state := extractStateFromURL(authURL)

	// Set very short timeout
	handlerConfig := &HandlerConfig{
		RequestTimeout: 50 * time.Millisecond,
	}

	router.GET("/callback", GoogleCallbackHandler(manager, handlerConfig))

	w := httptest.NewRecorder()
	req, _ := http.NewRequestWithContext(context.Background(), "GET", "/callback?code=auth-code&state="+state, nil)
	router.ServeHTTP(w, req)

	// Should get error due to timeout (either 500 or 401 depending on when context is cancelled)
	assert.True(t, w.Code == http.StatusInternalServerError || w.Code == http.StatusUnauthorized)
}

func TestDefaultHandlerConfig(t *testing.T) {
	config := DefaultHandlerConfig()
	assert.NotNil(t, config)
	assert.True(t, config.SecureCookies)
	assert.Equal(t, "/", config.CookiePath)
	assert.Equal(t, 30*time.Second, config.RequestTimeout)
}

func TestGoogleCallbackHandler_DefaultConfig(t *testing.T) {
	router := setupTestRouter()

	config := DefaultManagerConfig()
	config.CallbackHandler = func(ctx context.Context, provider string, userInfo *UserInfo, tokenSet *TokenSet) (*CallbackInfo, error) {
		return &CallbackInfo{
			SessionCookieName: "session",
			SessionID:         "abc",
			CookieMaxAge:      3600,
		}, nil
	}

	manager, err := NewManager(context.Background(), config)
	require.NoError(t, err)

	mockProvider := &MockProvider{
		name:    "google",
		authURL: "https://example.com/auth",
	}
	manager.RegisterProvider(mockProvider)

	authURL, err := manager.GetAuthURL("google")
	require.NoError(t, err)
	state := extractStateFromURL(authURL)

	// Pass nil config to use defaults
	router.GET("/callback", GoogleCallbackHandler(manager, nil))

	w := httptest.NewRecorder()
	req, _ := http.NewRequestWithContext(context.Background(), "GET", "/callback?code=auth-code&state="+state, nil)
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	// Verify cookie uses default secure settings
	cookies := w.Result().Cookies()
	require.Len(t, cookies, 1)
	assert.True(t, cookies[0].HttpOnly)
	assert.True(t, cookies[0].Secure) // Default is secure
}
