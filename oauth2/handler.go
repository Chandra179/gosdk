package oauth2

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
)

// HandlerConfig contains configuration for OAuth2 handlers
type HandlerConfig struct {
	// SecureCookies sets the Secure flag on cookies (must be true in production)
	SecureCookies bool
	// CookieDomain sets the domain for cookies (empty for default)
	CookieDomain string
	// CookiePath sets the path for cookies
	CookiePath string
	// RequestTimeout is the timeout for OAuth2 operations
	RequestTimeout time.Duration
}

// DefaultHandlerConfig returns a secure default configuration
func DefaultHandlerConfig() *HandlerConfig {
	return &HandlerConfig{
		SecureCookies:  true,
		CookiePath:     "/",
		RequestTimeout: 30 * time.Second,
	}
}

// GoogleCallbackHandler handles Google OAuth2 callback
// Note: This handler sets secure HTTP-only cookies for session management
// @Summary Google OAuth2 callback
// @Description Handles Google OAuth2 callback and returns user info
// @Tags oauth2
// @Produce json
// @Param code query string true "OAuth2 code"
// @Param state query string true "OAuth2 state"
// @Success 200 {object} map[string]interface{} "Authenticated"
// @Failure 400 {object} map[string]string "Bad Request - missing parameters"
// @Failure 401 {object} map[string]string "Unauthorized - invalid state or code"
// @Failure 500 {object} map[string]string "Internal Server Error"
// @Router /auth/callback/google [get]
func GoogleCallbackHandler(manager *Manager, config *HandlerConfig) gin.HandlerFunc {
	if config == nil {
		config = DefaultHandlerConfig()
	}

	return func(c *gin.Context) {
		code := c.Query("code")
		state := c.Query("state")

		if code == "" || state == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "missing code or state parameter"})
			return
		}

		// Create context with timeout to prevent hanging requests
		ctx, cancel := context.WithTimeout(c.Request.Context(), config.RequestTimeout)
		defer cancel()

		info, err := manager.HandleCallback(ctx, "google", code, state)
		if err != nil {
			status := http.StatusInternalServerError
			errMsg := "authentication failed"

			// Determine appropriate status code based on error type
			switch {
			case errors.Is(err, ErrProviderNotFound):
				status = http.StatusBadRequest
				errMsg = "provider not found"
			case errors.Is(err, ErrInvalidState), errors.Is(err, ErrStateNotFound), errors.Is(err, ErrStateExpired):
				status = http.StatusUnauthorized
				errMsg = "invalid or expired state"
			case errors.Is(err, ErrOAuth2Callback):
				status = http.StatusUnauthorized
				errMsg = "oauth2 callback failed"
			}

			c.JSON(status, gin.H{"error": errMsg})
			return
		}

		// Set secure HTTP-only cookie
		c.SetSameSite(http.SameSiteStrictMode)
		c.SetCookie(
			info.SessionCookieName,
			info.SessionID,
			info.CookieMaxAge,
			config.CookiePath,
			config.CookieDomain,
			config.SecureCookies, // Secure: from config
			false,                // HttpOnly: always true
		)

		c.JSON(http.StatusOK, gin.H{
			"status":  "authenticated",
			"user_id": info.UserID,
		})
	}
}
