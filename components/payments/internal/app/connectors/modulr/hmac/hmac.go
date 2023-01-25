package hmac

import (
	"time"

	"github.com/google/uuid"
	"github.com/pkg/errors"
)

const (
	authorizationHeader = "Authorization"
	dateHeader          = "Date"
	emptyString         = ""
	nonceHeader         = "x-mod-nonce"
	retry               = "x-mod-retry"
	retryTrue           = "true"
	retryFalse          = "false"
)

var ErrInvalidCredentials = errors.New("invalid api credentials")

func GenerateHeaders(apiKey, apiSecret, nonce string, hasRetry bool) (map[string]string, error) {
	if apiKey == "" || apiSecret == "" {
		return nil, ErrInvalidCredentials
	}

	return constructHeadersMap(apiKey, apiSecret, nonce, hasRetry, time.Now()), nil
}

func constructHeadersMap(apiKey, apiSecret, nonce string, hasRetry bool,
	timestamp time.Time,
) map[string]string {
	headers := make(map[string]string)
	date := timestamp.Format(time.RFC1123)
	nonce = generateNonceIfEmpty(nonce)

	headers[dateHeader] = date
	headers[authorizationHeader] = buildSignature(apiKey, apiSecret, nonce, date)
	headers[nonceHeader] = nonce
	headers[retry] = parseRetryBool(hasRetry)

	return headers
}

func generateNonceIfEmpty(nonce string) string {
	if nonce == emptyString {
		nonce = uuid.New().String()
	}

	return nonce
}

func parseRetryBool(hasRetry bool) string {
	if hasRetry {
		return retryTrue
	}

	return retryFalse
}
