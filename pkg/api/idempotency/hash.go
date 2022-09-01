package idempotency

import (
	"crypto/sha256"
	"encoding/base64"
	"io"
	"net/http"
)

func hashRequest(req *http.Request) string {
	data, err := io.ReadAll(req.Body)
	if err != nil {
		return ""
	}

	sh := sha256.New()
	sh.Write([]byte(req.URL.String()))
	sh.Write(data)

	return base64.RawURLEncoding.EncodeToString(sh.Sum(nil))
}
