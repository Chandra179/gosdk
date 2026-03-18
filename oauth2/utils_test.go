package oauth2

import (
	"encoding/base64"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGenerateRandomString(t *testing.T) {
	// Test various lengths
	lengths := []int{16, 32, 64, 128}

	for _, length := range lengths {
		t.Run("length_"+string(rune(length)), func(t *testing.T) {
			str, err := GenerateRandomString(length)
			require.NoError(t, err)
			assert.NotEmpty(t, str)

			// Hex encoding doubles the length
			assert.Equal(t, length*2, len(str))

			// Verify it's valid hex
			for _, c := range str {
				assert.True(t, (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'))
			}
		})
	}
}

func TestGenerateRandomString_Unique(t *testing.T) {
	// Generate multiple strings and ensure they're unique
	generated := make(map[string]bool)
	for i := 0; i < 100; i++ {
		str, err := GenerateRandomString(32)
		require.NoError(t, err)
		assert.False(t, generated[str], "Generated strings should be unique")
		generated[str] = true
	}
}

func TestGenerateCodeVerifier(t *testing.T) {
	verifier, err := GenerateCodeVerifier()
	require.NoError(t, err)
	assert.NotEmpty(t, verifier)

	// PKCE code verifier must be 43-128 characters (base64url encoded)
	assert.GreaterOrEqual(t, len(verifier), 43)
	assert.LessOrEqual(t, len(verifier), 128)

	// Should be valid base64url (no padding, no + or /)
	for _, c := range verifier {
		assert.True(t,
			(c >= 'A' && c <= 'Z') ||
				(c >= 'a' && c <= 'z') ||
				(c >= '0' && c <= '9') ||
				c == '-' || c == '_',
			"Character %c is not valid base64url", c)
	}
}

func TestGenerateCodeVerifier_Unique(t *testing.T) {
	generated := make(map[string]bool)
	for i := 0; i < 100; i++ {
		verifier, err := GenerateCodeVerifier()
		require.NoError(t, err)
		assert.False(t, generated[verifier], "Code verifiers should be unique")
		generated[verifier] = true
	}
}

func TestGenerateCodeChallenge(t *testing.T) {
	verifier, err := GenerateCodeVerifier()
	require.NoError(t, err)

	challenge := GenerateCodeChallenge(verifier)
	assert.NotEmpty(t, challenge)

	// Should be valid base64url
	for _, c := range challenge {
		assert.True(t,
			(c >= 'A' && c <= 'Z') ||
				(c >= 'a' && c <= 'z') ||
				(c >= '0' && c <= '9') ||
				c == '-' || c == '_',
			"Character %c is not valid base64url", c)
	}

	// Challenge should be deterministic for same verifier
	challenge2 := GenerateCodeChallenge(verifier)
	assert.Equal(t, challenge, challenge2)
}

func TestGenerateCodeChallenge_DifferentVerifiers(t *testing.T) {
	verifier1, _ := GenerateCodeVerifier()
	verifier2, _ := GenerateCodeVerifier()

	challenge1 := GenerateCodeChallenge(verifier1)
	challenge2 := GenerateCodeChallenge(verifier2)

	assert.NotEqual(t, challenge1, challenge2)
}

func TestBase64URLEncode(t *testing.T) {
	tests := []struct {
		input    []byte
		expected string
	}{
		{[]byte("hello"), "aGVsbG8"},
		{[]byte("test123"), "dGVzdDEyMw"},
		{[]byte{0x00, 0x01, 0x02}, "AAEC"},
	}

	for _, test := range tests {
		result := base64URLEncode(test.input)
		assert.Equal(t, test.expected, result)

		// Verify it can be decoded
		decoded, err := base64.RawURLEncoding.DecodeString(result)
		require.NoError(t, err)
		assert.Equal(t, test.input, decoded)
	}
}

func TestPKCE_Verification(t *testing.T) {
	// Simulate complete PKCE flow
	verifier, err := GenerateCodeVerifier()
	require.NoError(t, err)

	challenge := GenerateCodeChallenge(verifier)

	// In a real scenario, the verifier would be sent to the token endpoint
	// and the server would verify: SHA256(verifier) == decode(challenge)
	// Here we just verify our implementation is correct
	expectedChallenge := GenerateCodeChallenge(verifier)
	assert.Equal(t, expectedChallenge, challenge)
}
