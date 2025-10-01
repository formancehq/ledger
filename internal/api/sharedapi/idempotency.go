package api

import "net/http"

func IdempotencyKeyFromRequest(r *http.Request) string {
	return r.Header.Get("Idempotency-Key")
}
