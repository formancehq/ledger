package hmac

import (
	"crypto/hmac"
	"crypto/sha1" //nolint:gosec // we need sha1 for the hmac
	"encoding/base64"
	"net/url"
)

const (
	algorithm   = "algorithm=\"hmac-sha1\","
	datePrefix  = "date: "
	headers     = "headers=\"date x-mod-nonce\","
	prefix      = "signature=\""
	suffix      = "\""
	newline     = "\n"
	nonceKey    = "x-mod-nonce: "
	keyIDPrefix = "Signature keyId=\""
)

func buildSignature(apiKey, apiSecret, nonce, date string) string {
	keyID := keyIDPrefix + apiKey + "\","

	mac := hmac.New(sha1.New, []byte(apiSecret))
	mac.Write([]byte(datePrefix + date + newline + nonceKey + nonce))

	encodedMac := mac.Sum(nil)
	base64Encoded := base64.StdEncoding.EncodeToString(encodedMac)
	encodedSignature := prefix + url.QueryEscape(base64Encoded) + suffix

	return keyID + algorithm + headers + encodedSignature
}
