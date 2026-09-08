package http

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net/http"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/go-chi/chi/v5/middleware"
)

// correlationID returns a request-scoped token that is echoed to the client in
// sanitized error responses and logged server-side alongside the raw cause, so
// operators can grep the logs for the same string the caller reports. It
// reuses Chi's RequestID (installed as the first middleware in NewHandler),
// which is already present on every request context; on the rare path where no
// request ID is set it falls back to a freshly generated token so the caller
// always has something to quote.
func correlationID(r *http.Request) string {
	if id := middleware.GetReqID(r.Context()); validCorrelationID(id) {
		return id
	}

	return generatedCorrelationID()
}

const maxCorrelationIDLength = 128

func validCorrelationID(id string) bool {
	if id == "" || len(id) > maxCorrelationIDLength || !utf8.ValidString(id) {
		return false
	}

	for _, r := range id {
		if unicode.IsControl(r) {
			return false
		}
	}

	return true
}

// generatedCorrelationID returns a short hex token. Mirrors the gRPC adapter's
// newCorrelationID fallback: rand.Read failing on a server is exceptional, so
// fall back to a non-secret deterministic token that is still unique enough to
// correlate a single failure.
func generatedCorrelationID() string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return fmt.Sprintf("ts-%d", time.Now().UnixNano())
	}

	return hex.EncodeToString(b[:])
}
