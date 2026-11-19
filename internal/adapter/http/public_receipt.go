package http

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"maps"
	"net/http"

	"github.com/formancehq/ledger/v3/pkg/httpreceipt"
)

const publicReceiptBodyLimit = 16 << 20

type publicReceiptContextKey struct{}

// publicReceiptState marks the single route that supports an Apply proof.
type publicReceiptState struct{}

func receiptState(ctx context.Context) *publicReceiptState {
	state, _ := ctx.Value(publicReceiptContextKey{}).(*publicReceiptState)

	return state
}

func hasPublicReceipt(r *http.Request) bool {
	return len(r.Header.Values(httpreceipt.Header)) != 0
}

// publicReceipt authenticates the complete public Apply response, including
// redacted logs whose original SignedLog cannot be exposed to HTTP clients.
// The responding node signs after service Apply; admission and FSM are unchanged.
func (s *Server) publicReceipt(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !hasPublicReceipt(r) {
			next(w, r)

			return
		}
		values := r.Header.Values(httpreceipt.Header)
		if len(values) != 1 || values[0] != httpreceipt.Version {
			writeBadRequest(w, "INVALID_REQUEST", errors.New("Formance-Public-Receipt must occur exactly once with value v1"))

			return
		}
		if s.responseSigner == nil {
			w.Header().Set("Retry-After", "1")
			writeErrorResponse(w, http.StatusServiceUnavailable, "RESPONSE_SIGNING_UNAVAILABLE", errors.New("server response signing is disabled"))

			return
		}
		state := &publicReceiptState{}
		r = r.WithContext(context.WithValue(r.Context(), publicReceiptContextKey{}, state))
		captured := &publicReceiptWriter{header: w.Header().Clone()}
		next(captured, r)
		if captured.status == 0 {
			captured.WriteHeader(http.StatusOK)
		}
		body := captured.body.Bytes()
		// Streaming JSON encoders may append an error after partial output. Never
		// attest a malformed public document or publish unsigned success on overflow.
		if captured.err != nil || (len(body) > 0 && !json.Valid(body)) {
			writeInternalServerError(w, r, errors.New("public receipt response could not be encoded within its limit; mutation may have committed"))

			return
		}
		targetHash := sha256.Sum256([]byte(r.URL.RequestURI()))
		bodyHash := sha256.Sum256(body)
		receipt, err := s.responseSigner.SignPublicReceipt(httpreceipt.Claims{
			Version: httpreceipt.Version, Method: r.Method, Status: captured.status,
			TargetHash: hex.EncodeToString(targetHash[:]), BodyHash: hex.EncodeToString(bodyHash[:]),
		})
		if err != nil {
			writeInternalServerError(w, r, err)

			return
		}
		maps.Copy(w.Header(), captured.sentHeader)
		w.Header().Set(httpreceipt.Header, receipt)
		w.Header().Add("Access-Control-Expose-Headers", httpreceipt.Header)
		w.WriteHeader(captured.status)
		if len(body) == 0 {
			return
		}
		if _, err := w.Write(body); err != nil {
			// Commit and signing already happened; delivery failure cannot undo either.
			s.logger.Errorf("writing public receipt response: %v", err)
		}
	}
}

// publicReceiptWriter withholds headers and bounds retained output before it
// can be signed. Overflow is a post-Apply failure, never evidence of rollback.
type publicReceiptWriter struct {
	header     http.Header
	sentHeader http.Header
	status     int
	body       bytes.Buffer
	err        error
}

func (w *publicReceiptWriter) Header() http.Header { return w.header }
func (w *publicReceiptWriter) WriteHeader(status int) {
	if w.status != 0 {
		return
	}
	w.status = status
	w.sentHeader = w.header.Clone()
}
func (w *publicReceiptWriter) Write(data []byte) (int, error) {
	if w.status == 0 {
		w.WriteHeader(http.StatusOK)
	}
	if w.err != nil {
		return 0, w.err
	}
	if len(data) > publicReceiptBodyLimit-w.body.Len() {
		w.err = errors.New("public receipt response exceeds 16 MiB")

		return 0, w.err
	}

	return w.body.Write(data)
}
