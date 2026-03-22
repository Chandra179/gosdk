//go:build integration

package example

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/go-jose/go-jose/v4"
	josejwt "github.com/go-jose/go-jose/v4/jwt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	oauth2pkg "oauth2"
)

// oidcServer is a minimal mock OIDC provider for integration testing.
type oidcServer struct {
	server     *httptest.Server
	privateKey *rsa.PrivateKey
	keyID      string
}

func (s *oidcServer) issuer() string { return s.server.URL }

func (s *oidcServer) discovery(w http.ResponseWriter, r *http.Request) {
	doc := map[string]any{
		"issuer":                                s.issuer(),
		"authorization_endpoint":                s.issuer() + "/auth",
		"token_endpoint":                        s.issuer() + "/token",
		"jwks_uri":                              s.issuer() + "/jwks",
		"id_token_signing_alg_values_supported": []string{"RS256"},
		"response_types_supported":              []string{"code"},
		"subject_types_supported":               []string{"public"},
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(doc)
}

func (s *oidcServer) jwks(w http.ResponseWriter, r *http.Request) {
	jwk := jose.JSONWebKey{
		Key:       &s.privateKey.PublicKey,
		KeyID:     s.keyID,
		Algorithm: string(jose.RS256),
		Use:       "sig",
	}
	set := jose.JSONWebKeySet{Keys: []jose.JSONWebKey{jwk}}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(set)
}

func (s *oidcServer) token(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	// The test encodes the nonce into the code as "code:<nonce>" so the token server
	// can embed the correct nonce in the ID token without a shared state store.
	code := r.FormValue("code")
	nonce := ""
	if len(code) > 5 && code[:5] == "code:" {
		nonce = code[5:]
	}

	clientID := r.FormValue("client_id")
	if clientID == "" {
		// OAuth2 library may send credentials via HTTP Basic Auth
		if id, _, ok := r.BasicAuth(); ok {
			clientID = id
		}
	}

	sig, err := jose.NewSigner(
		jose.SigningKey{Algorithm: jose.RS256, Key: s.privateKey},
		(&jose.SignerOptions{}).WithType("JWT").WithHeader("kid", s.keyID),
	)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	now := time.Now()
	claims := josejwt.Claims{
		Issuer:   s.issuer(),
		Subject:  "user-sub-123",
		Audience: josejwt.Audience{clientID},
		IssuedAt: josejwt.NewNumericDate(now),
		Expiry:   josejwt.NewNumericDate(now.Add(time.Hour)),
	}
	extra := struct {
		Email string `json:"email"`
		Name  string `json:"name"`
		Nonce string `json:"nonce"`
	}{
		Email: "test@example.com",
		Name:  "Test User",
		Nonce: nonce,
	}

	idToken, err := josejwt.Signed(sig).Claims(claims).Claims(extra).Serialize()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	resp := map[string]any{
		"access_token":  "access-token-value",
		"token_type":    "Bearer",
		"refresh_token": "refresh-token-value",
		"expires_in":    3600,
		"id_token":      idToken,
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// package-level variables shared across tests
var (
	mockOIDC *oidcServer
	manager  oauth2pkg.Manager
)

func TestMain(m *testing.M) {
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		fmt.Fprintf(os.Stderr, "generate RSA key: %v\n", err)
		os.Exit(1)
	}

	mockOIDC = &oidcServer{privateKey: privateKey, keyID: "test-key-id"}

	mux := http.NewServeMux()
	mux.HandleFunc("/.well-known/openid-configuration", mockOIDC.discovery)
	mux.HandleFunc("/jwks", mockOIDC.jwks)
	mux.HandleFunc("/token", mockOIDC.token)
	// /auth endpoint is not exercised in these tests (we simulate callbacks directly)
	mockOIDC.server = httptest.NewServer(mux)

	ctx := context.Background()

	// Use NewOIDCProvider so we can point at the mock server instead of accounts.google.com
	provider, err := oauth2pkg.NewOIDCProvider(
		ctx,
		mockOIDC.issuer(),
		"google",
		"test-client-id",
		"test-client-secret",
		mockOIDC.issuer()+"/callback",
		nil,
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "create provider: %v\n", err)
		mockOIDC.server.Close()
		os.Exit(1)
	}

	manager, err = oauth2pkg.NewManager(ctx, &oauth2pkg.ManagerConfig{
		StateTimeout: 5 * time.Minute,
		CallbackHandler: func(ctx context.Context, providerName string, userInfo *oauth2pkg.UserInfo, tokenSet *oauth2pkg.TokenSet) (*oauth2pkg.CallbackInfo, error) {
			return &oauth2pkg.CallbackInfo{
				SessionCookieName: "session",
				UserID:            userInfo.ID,
				SessionID:         "session-" + userInfo.ID,
				CookieMaxAge:      3600,
			}, nil
		},
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "create manager: %v\n", err)
		mockOIDC.server.Close()
		os.Exit(1)
	}
	manager.RegisterProvider(provider)

	code := m.Run()

	manager.Cleanup()
	mockOIDC.server.Close()
	os.Exit(code)
}

func TestIntegration_GetAuthURL(t *testing.T) {
	authURL, err := manager.GetAuthURL("google")
	require.NoError(t, err)
	assert.Contains(t, authURL, mockOIDC.issuer())
	assert.Contains(t, authURL, "state=")
	assert.Contains(t, authURL, "code_challenge=")
	assert.Contains(t, authURL, "nonce=")
}

func TestIntegration_FullFlow(t *testing.T) {
	ctx := context.Background()

	// Step 1: generate auth URL (creates state + nonce + PKCE in storage)
	authURL, err := manager.GetAuthURL("google")
	require.NoError(t, err)

	state := extractParam(authURL, "state")
	require.NotEmpty(t, state)

	// Encode nonce into the code so the mock token server can embed it in the ID token.
	nonce := extractParam(authURL, "nonce")
	require.NotEmpty(t, nonce)
	code := "code:" + nonce

	// Step 2: simulate provider callback — code exchange happens against mock server
	info, err := manager.HandleCallback(ctx, "google", code, state)
	require.NoError(t, err)
	assert.Equal(t, "user-sub-123", info.UserID)
	assert.Equal(t, "session", info.SessionCookieName)
	assert.Equal(t, 3600, info.CookieMaxAge)

	// Step 3: verify state is one-time-use (replay attack prevention)
	_, err = manager.HandleCallback(ctx, "google", code, state)
	assert.ErrorIs(t, err, oauth2pkg.ErrInvalidState)
}

func TestIntegration_InvalidState(t *testing.T) {
	_, err := manager.HandleCallback(context.Background(), "google", "test-code", "bogus-state")
	assert.ErrorIs(t, err, oauth2pkg.ErrInvalidState)
}

func TestIntegration_UnknownProvider(t *testing.T) {
	_, err := manager.GetAuthURL("github")
	assert.ErrorIs(t, err, oauth2pkg.ErrProviderNotFound)
}

func TestIntegration_ConcurrentAuthURLs(t *testing.T) {
	const n = 10
	results := make(chan string, n)

	for i := 0; i < n; i++ {
		go func() {
			url, err := manager.GetAuthURL("google")
			if err != nil {
				results <- ""
				return
			}
			results <- extractParam(url, "state")
		}()
	}

	seen := make(map[string]bool)
	for i := 0; i < n; i++ {
		state := <-results
		require.NotEmpty(t, state, "state should not be empty")
		assert.False(t, seen[state], "state must be unique across concurrent requests")
		seen[state] = true
	}
}

// extractParam parses a named query parameter from a raw URL string.
func extractParam(rawURL, param string) string {
	idx := 0
	for i, c := range rawURL {
		if c == '?' {
			idx = i + 1
			break
		}
	}
	if idx == 0 {
		return ""
	}
	query := rawURL[idx:]
	start := 0
	for i := 0; i <= len(query); i++ {
		if i == len(query) || query[i] == '&' {
			part := query[start:i]
			if len(part) > len(param)+1 && part[:len(param)+1] == param+"=" {
				return part[len(param)+1:]
			}
			start = i + 1
		}
	}
	return ""
}
