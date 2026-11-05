package http

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"strings"

	"github.com/formancehq/ledger/v3/internal/proto/signaturepb"
)

const (
	signedRequestMediaType       = "application/vnd.formance.signed-request+json"
	signedRequestKeyIDHeader     = "Formance-Signing-Key-Id"
	signedRequestSignatureHeader = "Formance-Signature"
)

type signedRequestContextKey struct{}

func isSignedRequest(r *http.Request) bool {
	mediaType, _, _ := mime.ParseMediaType(r.Header.Get("Content-Type"))

	return mediaType == signedRequestMediaType ||
		len(r.Header.Values(signedRequestKeyIDHeader)) > 0 ||
		len(r.Header.Values(signedRequestSignatureHeader)) > 0
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

		mediaType, _, _ := mime.ParseMediaType(r.Header.Get("Content-Type"))
		if mediaType != signedRequestMediaType {
			writeErrorResponse(w, http.StatusUnsupportedMediaType, "UNSUPPORTED_MEDIA_TYPE", errors.New("signing headers require "+signedRequestMediaType))

			return
		}
		keyID, err := signingHeader(r, signedRequestKeyIDHeader)
		if err != nil {
			writeBadRequest(w, "INVALID_REQUEST", err)

			return
		}
		encodedSignature, err := signingHeader(r, signedRequestSignatureHeader)
		if err != nil {
			writeBadRequest(w, "INVALID_REQUEST", err)

			return
		}
		signature, err := base64.StdEncoding.Strict().DecodeString(encodedSignature)
		if err != nil || len(signature) != ed25519.SignatureSize {
			writeBadRequest(w, "INVALID_REQUEST", errors.New(signedRequestSignatureHeader+" must be a standard-base64 Ed25519 signature (64 bytes)"))

			return
		}

		body, err := io.ReadAll(r.Body)
		if err != nil {
			writeBadRequest(w, "INVALID_REQUEST", fmt.Errorf("invalid signed request body: %w", err))

			return
		}
		request, payload, err := decodeSignedRequest(body)
		if err != nil {
			writeBadRequest(w, "INVALID_REQUEST", err)

			return
		}
		if r.Method == http.MethodDelete && !bytes.Equal(bytes.TrimSpace(request), []byte("null")) {
			writeBadRequest(w, "INVALID_REQUEST", errors.New("signed metadata deletion requires request: null"))

			return
		}

		// Only the transport representation changes: admission receives the same
		// envelope and original payload bytes as a signed gRPC Apply request.
		envelope := &signaturepb.SignedApplyBatch{KeyId: keyID, Signature: signature, Payload: payload}
		r = r.WithContext(context.WithValue(r.Context(), signedRequestContextKey{}, envelope))
		r.Body = io.NopCloser(bytes.NewReader(request))
		next(w, r)
	}
}

func signingHeader(r *http.Request, name string) (string, error) {
	values := r.Header.Values(name)
	if len(values) != 1 || values[0] == "" || strings.TrimSpace(values[0]) != values[0] {
		return "", fmt.Errorf("%s must appear exactly once with a nonempty value and no surrounding whitespace", name)
	}

	return values[0], nil
}

// decodeSignedRequest rejects unknown and duplicate wrapper fields. The inner
// request keeps the existing route's JSON semantics; payload is the exact signed
// protobuf batch. ReadAll above enforces the router's whole-body limit,
// including any trailing data.
func decodeSignedRequest(body []byte) (json.RawMessage, []byte, error) {
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
		if !ok || (key != "request" && key != "payload") {
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
	if len(fields["request"]) == 0 || len(fields["payload"]) == 0 {
		return nil, nil, errors.New("signed request requires request and payload")
	}
	var encodedPayload string
	if err := json.Unmarshal(fields["payload"], &encodedPayload); err != nil {
		return nil, nil, fmt.Errorf("invalid payload: %w", err)
	}
	payload, err := base64.StdEncoding.Strict().DecodeString(encodedPayload)
	if err != nil || len(payload) == 0 {
		return nil, nil, errors.New("payload must be a nonempty standard-base64 protobuf batch")
	}

	return fields["request"], payload, nil
}

func signedBatchFromContext(ctx context.Context) *signaturepb.SignedApplyBatch {
	envelope, _ := ctx.Value(signedRequestContextKey{}).(*signaturepb.SignedApplyBatch)

	return envelope
}
