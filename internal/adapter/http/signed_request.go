package http

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"

	"google.golang.org/protobuf/encoding/protojson"

	"github.com/formancehq/ledger/v3/internal/proto/signaturepb"
)

const signedRequestMediaType = "application/vnd.formance.signed-request+json"

type signedRequestContextKey struct{}

func isSignedRequest(r *http.Request) bool {
	mediaType, _, _ := mime.ParseMediaType(r.Header.Get("Content-Type"))

	return mediaType == signedRequestMediaType
}

// signedRequest unwraps the opt-in representation on supported routes only.
// Existing decoders remain responsible for converting the visible request to
// business intent. applyRequests binds that intent to the signed batch before
// forwarding the original envelope to admission's authoritative verifier.
func signedRequest(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !isSignedRequest(r) {
			next(w, r)

			return
		}

		body, err := io.ReadAll(r.Body)
		if err != nil {
			writeBadRequest(w, "INVALID_REQUEST", fmt.Errorf("invalid signed request body: %w", err))

			return
		}
		request, envelope, err := decodeSignedRequest(body)
		if err != nil {
			writeBadRequest(w, "INVALID_REQUEST", err)

			return
		}
		if r.Method == http.MethodDelete && !bytes.Equal(bytes.TrimSpace(request), []byte("null")) {
			writeBadRequest(w, "INVALID_REQUEST", errors.New("signed metadata deletion requires request: null"))

			return
		}

		r = r.WithContext(context.WithValue(r.Context(), signedRequestContextKey{}, envelope))
		r.Body = io.NopCloser(bytes.NewReader(request))
		next(w, r)
	}
}

// decodeSignedRequest rejects unknown and duplicate envelope fields. The
// protobuf JSON decoder does the same for signedBatch; the inner request keeps
// the existing route's JSON semantics. ReadAll above enforces the router's
// whole-body limit, including any trailing data.
func decodeSignedRequest(body []byte) (json.RawMessage, *signaturepb.SignedApplyBatch, error) {
	decoder := json.NewDecoder(bytes.NewReader(body))
	first, err := decoder.Token()
	if err != nil || first != json.Delim('{') {
		return nil, nil, errors.New("signed request must be a JSON object")
	}
	fields := make(map[string]json.RawMessage, 2)
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return nil, nil, fmt.Errorf("invalid signed request: %w", err)
		}
		key, ok := token.(string)
		if !ok || (key != "request" && key != "signedBatch") {
			return nil, nil, errors.New("unknown signed request field")
		}
		if _, exists := fields[key]; exists {
			return nil, nil, fmt.Errorf("duplicate signed request field %q", key)
		}
		var value json.RawMessage
		if err := decoder.Decode(&value); err != nil {
			return nil, nil, fmt.Errorf("invalid signed request field: %w", err)
		}
		fields[key] = value
	}
	if _, err := decoder.Token(); err != nil {
		return nil, nil, fmt.Errorf("invalid signed request: %w", err)
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return nil, nil, errors.New("signed request must contain exactly one JSON value")
	}
	if len(fields["request"]) == 0 || len(fields["signedBatch"]) == 0 || bytes.Equal(fields["signedBatch"], []byte("null")) {
		return nil, nil, errors.New("signed request requires request and signedBatch")
	}
	envelope := &signaturepb.SignedApplyBatch{}
	if err := protojson.Unmarshal(fields["signedBatch"], envelope); err != nil {
		return nil, nil, fmt.Errorf("invalid signedBatch: %w", err)
	}

	return fields["request"], envelope, nil
}

func signedBatchFromContext(ctx context.Context) *signaturepb.SignedApplyBatch {
	envelope, _ := ctx.Value(signedRequestContextKey{}).(*signaturepb.SignedApplyBatch)

	return envelope
}
