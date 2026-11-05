package http

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/formancehq/ledger/v3/internal/proto/signaturepb"
)

type signedRequestContextKey struct{}

func isSignedRequest(r *http.Request) bool {
	_, signature := r.Header["Signature"]
	_, input := r.Header["Signature-Input"]

	return signature || input
}

// signedRequest captures the original HTTP proof before the ordinary handler
// decodes its body. Admission verifies the proof; applyRequests binds its decoded
// commands to the handler's commands so HTTP and forwarded gRPC agree.
func signedRequest(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !isSignedRequest(r) {
			next(w, r)

			return
		}
		proof, err := captureSignedRequest(r)
		if err != nil {
			writeBadRequest(w, "INVALID_REQUEST", err)

			return
		}
		r = r.WithContext(context.WithValue(r.Context(), signedRequestContextKey{}, proof))
		r.Body = io.NopCloser(bytes.NewReader(proof.GetBody()))
		next(w, r)
	}
}

func captureSignedRequest(r *http.Request) (*signaturepb.SignedHTTPRequest, error) {
	headers := make(map[string]string, 5)
	for _, name := range []string{"Content-Type", "Content-Digest", "Idempotency-Key", "Signature-Input", "Signature"} {
		values := r.Header.Values(name)
		if len(values) != 1 || (name != "Idempotency-Key" && values[0] == "") {
			return nil, fmt.Errorf("signed request requires exactly one %s header", name)
		}
		headers[name] = values[0]
	}
	if headers["Content-Type"] != "application/json" {
		return nil, errors.New("signed request Content-Type must be application/json")
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, fmt.Errorf("invalid signed request body: %w", err)
	}
	if r.Method == http.MethodDelete && len(body) != 0 {
		return nil, errors.New("signed metadata deletion requires an empty body")
	}
	if len(body) != 0 && !json.Valid(body) {
		return nil, errors.New("signed request body must contain exactly one JSON value")
	}
	scheme := r.URL.Scheme
	if scheme == "" {
		scheme = "http"
		if r.TLS != nil {
			scheme = "https"
		}
	}

	return &signaturepb.SignedHTTPRequest{
		Scheme: scheme,
		Method: r.Method, RequestTarget: r.URL.RequestURI(), Authority: r.Host,
		ContentType: headers["Content-Type"], ContentDigest: headers["Content-Digest"],
		IdempotencyKey: headers["Idempotency-Key"], SignatureInput: headers["Signature-Input"],
		Signature: headers["Signature"], Body: body,
	}, nil
}

func httpSignatureFromContext(ctx context.Context) *signaturepb.SignedHTTPRequest {
	proof, _ := ctx.Value(signedRequestContextKey{}).(*signaturepb.SignedHTTPRequest)

	return proof
}
