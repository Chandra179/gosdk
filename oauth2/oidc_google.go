package oauth2

import (
	"context"
	"fmt"
	"time"

	"github.com/coreos/go-oidc/v3/oidc"
	"golang.org/x/oauth2"
)

// GoogleOIDCProvider implements Provider interface using OIDC with PKCE
type GoogleOIDCProvider struct {
	config       *oauth2.Config
	provider     *oidc.Provider
	verifier     *oidc.IDTokenVerifier
	logoutURL    string
	providerName string
}

// NewGoogleOIDCProvider creates a new Google OIDC provider
func NewGoogleOIDCProvider(ctx context.Context, clientID, clientSecret, redirectURL string, scopes []string) (*GoogleOIDCProvider, error) {
	provider, err := oidc.NewProvider(ctx, "https://accounts.google.com")
	if err != nil {
		return nil, fmt.Errorf("failed to create OIDC provider: %w", err)
	}

	if len(scopes) == 0 {
		scopes = []string{oidc.ScopeOpenID, "profile", "email"}
	}

	config := &oauth2.Config{
		ClientID:     clientID,
		ClientSecret: clientSecret,
		RedirectURL:  redirectURL,
		Endpoint:     provider.Endpoint(),
		Scopes:       scopes,
	}

	verifier := provider.Verifier(&oidc.Config{
		ClientID: clientID,
	})

	return &GoogleOIDCProvider{
		config:       config,
		provider:     provider,
		verifier:     verifier,
		logoutURL:    "https://accounts.google.com/logout",
		providerName: "google",
	}, nil
}

// GetName returns the provider name
func (g *GoogleOIDCProvider) GetName() string {
	return g.providerName
}

// GetAuthURL generates the authorization URL with PKCE and nonce
func (g *GoogleOIDCProvider) GetAuthURL(state string, nonce string, codeChallenge string) string {
	opts := []oauth2.AuthCodeOption{
		oauth2.AccessTypeOffline,
		oauth2.ApprovalForce,
		oidc.Nonce(nonce),
		oauth2.SetAuthURLParam("code_challenge", codeChallenge),
		oauth2.SetAuthURLParam("code_challenge_method", "S256"),
	}
	return g.config.AuthCodeURL(state, opts...)
}

// HandleCallback processes the OAuth2 callback, exchanges code for tokens, and validates ID token
func (g *GoogleOIDCProvider) HandleCallback(ctx context.Context, code string, state string,
	nonce string, codeVerifier string) (*UserInfo, *TokenSet, error) {
	oauth2Token, err := g.config.Exchange(
		ctx,
		code,
		oauth2.SetAuthURLParam("code_verifier", codeVerifier),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to exchange code: %w", err)
	}

	rawIDToken, ok := oauth2Token.Extra("id_token").(string)
	if !ok {
		return nil, nil, fmt.Errorf("no id_token in response")
	}

	idToken, err := g.verifier.Verify(ctx, rawIDToken)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to verify ID token: %w", err)
	}

	if idToken.Nonce != nonce {
		return nil, nil, fmt.Errorf("nonce mismatch")
	}

	var claims struct {
		Email string `json:"email"`
		Name  string `json:"name"`
	}

	if err := idToken.Claims(&claims); err != nil {
		return nil, nil, fmt.Errorf("failed to parse claims: %w", err)
	}

	userInfo := &UserInfo{
		ID:        idToken.Subject,
		Email:     claims.Email,
		Name:      claims.Name,
		Provider:  g.providerName,
		CreatedAt: time.Now(),
	}

	tokenSet := &TokenSet{
		AccessToken:  oauth2Token.AccessToken,
		TokenType:    oauth2Token.TokenType,
		RefreshToken: oauth2Token.RefreshToken,
		IDToken:      rawIDToken,
		ExpiresAt:    oauth2Token.Expiry,
	}

	return userInfo, tokenSet, nil
}

// RefreshToken refreshes the access token using refresh token
func (g *GoogleOIDCProvider) RefreshToken(ctx context.Context, refreshToken string) (*TokenSet, error) {
	tokenSource := g.config.TokenSource(ctx, &oauth2.Token{
		RefreshToken: refreshToken,
	})

	newToken, err := tokenSource.Token()
	if err != nil {
		return nil, fmt.Errorf("failed to refresh token: %w", err)
	}

	tokenSet := &TokenSet{
		AccessToken:  newToken.AccessToken,
		TokenType:    newToken.TokenType,
		RefreshToken: newToken.RefreshToken,
		ExpiresAt:    newToken.Expiry,
	}

	// Preserve existing ID token if refresh doesn't return one
	if idToken, ok := newToken.Extra("id_token").(string); ok && idToken != "" {
		tokenSet.IDToken = idToken
	}

	return tokenSet, nil
}

// GetEndSessionEndpoint returns Google's logout endpoint
func (g *GoogleOIDCProvider) GetEndSessionEndpoint() string {
	return g.logoutURL
}
