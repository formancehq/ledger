package idempotency

import (
	"crypto/sha256"
	"encoding/base64"
)

func hashRequest(url, data string) string {
	sh := sha256.New()
	sh.Write([]byte(url))
	sh.Write([]byte(data))

	return base64.RawURLEncoding.EncodeToString(sh.Sum(nil))
}
