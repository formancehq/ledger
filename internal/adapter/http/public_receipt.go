package http

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"hash"
	"maps"
	"net/http"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/pkg/httpreceipt"
)

const publicReceiptBodyLimit = 16 << 20

type publicReceiptContextKey struct{}

// publicReceiptState records only successful Apply results, including historical
// replay provenance returned through follower forwarding. It never retains raw
// logs: their signed protobuf payloads can contain connector credentials.
// HTTP bulk calls are sequential; this request-local collector is not shared.
type publicReceiptState struct {
	applies   uint64
	logs      uint64
	replayed  uint64
	sequences hash.Hash
}

func receiptState(ctx context.Context) *publicReceiptState {
	state, _ := ctx.Value(publicReceiptContextKey{}).(*publicReceiptState)

	return state
}

func (s *publicReceiptState) record(result *domain.ApplyResult) {
	if s == nil {
		return
	}
	if result == nil {
		panic("successful Apply returned no result")
	}
	s.applies++
	s.logs += uint64(len(result.Logs))
	if result.Replayed {
		s.replayed++
	}
	// Length-prefix each successful Apply group to preserve batch boundaries.
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], uint64(len(result.Logs)))
	_, _ = s.sequences.Write(encoded[:]) // hash.Hash.Write never returns an error.
	for _, log := range result.Logs {
		if log == nil {
			panic("successful Apply returned a nil log")
		}
		binary.BigEndian.PutUint64(encoded[:], log.GetSequence())
		_, _ = s.sequences.Write(encoded[:]) // hash.Hash.Write never returns an error.
	}
}

func hasPublicReceipt(r *http.Request) bool {
	return len(r.Header.Values(httpreceipt.Header)) != 0
}

// publicReceipt authenticates the public HTTP exchange, not an authoritative
// SignedLog or audit record. Signing a redacted Log would violate that contract;
// this separate claim signs only the digest of the credential-safe HTTP bytes.
// The responding node signs after ordinary Apply; admission and FSM are unchanged.
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
		state := &publicReceiptState{sequences: sha256.New()}
		r = r.WithContext(context.WithValue(r.Context(), publicReceiptContextKey{}, state))
		captured := &publicReceiptWriter{header: w.Header().Clone()}
		next(captured, r)
		if captured.status == 0 {
			captured.WriteHeader(http.StatusOK)
		}
		body := captured.body.Bytes()
		// Streaming JSON encoders may append an error after partial output. Never
		// attest a malformed public document or publish unsigned success on overflow.
		if captured.err != nil || (captured.status != http.StatusNoContent && !json.Valid(body)) {
			writeInternalServerError(w, r, errors.New("public receipt response could not be encoded within its limit; mutation may have committed"))

			return
		}
		targetHash := sha256.Sum256([]byte(r.URL.RequestURI()))
		bodyHash := sha256.Sum256(body)
		receipt, err := s.responseSigner.SignPublicReceipt(httpreceipt.Claims{
			Version: httpreceipt.Version, Method: r.Method, Status: captured.status,
			TargetHash: hex.EncodeToString(targetHash[:]), BodyHash: hex.EncodeToString(bodyHash[:]),
			Applies: state.applies, Logs: state.logs, Replayed: state.replayed,
			SequencesHash: hex.EncodeToString(state.sequences.Sum(nil)),
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

// rejectPublicReceipt is used by routes without a mutation receipt contract.
func rejectPublicReceipt(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if hasPublicReceipt(r) {
			writeBadRequest(w, "INVALID_REQUEST", errors.New("public receipts are not supported on this route"))

			return
		}
		next.ServeHTTP(w, r)
	})
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
