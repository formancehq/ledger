package hmac

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestGenerateReturnsAnHMACString(t *testing.T) {
	t.Parallel()

	headers, _ := GenerateHeaders("api_key", "api_secret", "", false)
	expectedSignature := "Signature keyId=\"api_key\",algorithm=\"hmac-sha1\",headers=\"date x-mod-nonce\",signature=\""
	assert.Equal(t, expectedSignature, headers["Authorization"][0:86], "generate should return the hmac headers")
}

func TestGenerateReturnsADateHeader(t *testing.T) {
	t.Parallel()

	timestamp := time.Date(2020, 1, 2, 15, 4, 5, 0, time.UTC)

	headers := constructHeadersMap("api_key", "api_secret", "", false, timestamp)

	expectedDate := "Thu, 02 Jan 2020 15:04:05 UTC"

	assert.Equal(t, expectedDate, headers["Date"])
}

func TestGenerateReturnsANonceHeaderWithExpectedValue(t *testing.T) {
	t.Parallel()

	nonce := "thisIsTheNonce"
	headers, _ := GenerateHeaders("api_key", "api_secret", nonce, false)
	assert.Equal(t, nonce, headers["x-mod-nonce"])
}

func TestGenerateReturnsARetryHeaderWithTrueIfRetryIsExpected(t *testing.T) {
	t.Parallel()

	headers, _ := GenerateHeaders("api_key", "api_secret", "", true)
	assert.Equal(t, "true", headers["x-mod-retry"])
}

func TestGenerateReturnsARetryHeaderWithFalseIfRetryIsNotExpected(t *testing.T) {
	t.Parallel()

	headers, _ := GenerateHeaders("api_key", "api_secret", "", false)
	assert.Equal(t, "false", headers["x-mod-retry"])
}

func TestGenerateReturnsAGeneratedNonceHeaderIfNonceIsEmpty(t *testing.T) {
	t.Parallel()

	headers, _ := GenerateHeaders("api_key", "api_secret", "", false)
	assert.True(t, headers["x-mod-nonce"] != "", "x-mod-nonce header should have been populated")
}

func TestGenerateThrowsErrorIfApiKeyIsNull(t *testing.T) {
	t.Parallel()

	_, err := GenerateHeaders("", "api_secret", "", false)
	assert.ErrorIs(t, err, ErrInvalidCredentials)
}

func TestGenerateThrowsErrorIfApiSecretIsNull(t *testing.T) {
	t.Parallel()

	_, err := GenerateHeaders("api_key", "", "", false)
	assert.ErrorIs(t, err, ErrInvalidCredentials)
}
