package http

import (
	"errors"
	"io"
	"net/http"
	"unicode/utf8"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

func requireKVParams(w http.ResponseWriter, r *http.Request) (string, string, bool) {
	ledger, ok := requireLedgerName(w, r)
	if !ok {
		return "", "", false
	}
	key, ok := requirePathParameter(w, r, "key", "KV key")
	if !ok {
		return "", "", false
	}
	if !utf8.ValidString(key) || len(key) > domain.MaxKVKeyBytes {
		writeBadRequest(w, "INVALID_REQUEST", errors.New("KV key must contain 1 to 256 UTF-8 bytes"))

		return "", "", false
	}

	return ledger, key, true
}

func (s *Server) handleGetKV(w http.ResponseWriter, r *http.Request) {
	ledger, key, ok := requireKVParams(w, r)
	if !ok {
		return
	}
	value, err := s.backend.GetKV(r.Context(), ledger, key)
	if err != nil {
		handleError(w, r, err)

		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(value)
}

func (s *Server) handlePutKV(w http.ResponseWriter, r *http.Request) {
	ledger, key, ok := requireKVParams(w, r)
	if !ok {
		return
	}
	value, err := io.ReadAll(io.LimitReader(r.Body, domain.MaxKVValueBytes+1))
	if err != nil {
		writeBadRequest(w, "INVALID_REQUEST", err)

		return
	}
	if len(value) > domain.MaxKVValueBytes {
		http.Error(w, "KV value exceeds 1 MiB", http.StatusRequestEntityTooLarge)

		return
	}
	_, err = s.applyUnsigned(r.Context(), r.Header.Get("Idempotency-Key"), &servicepb.Request{
		Type: &servicepb.Request_PutKv{PutKv: &servicepb.PutKVRequest{Ledger: ledger, Key: key, Value: value}},
	})
	if err != nil {
		handleError(w, r, err)

		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleDeleteKV(w http.ResponseWriter, r *http.Request) {
	ledger, key, ok := requireKVParams(w, r)
	if !ok {
		return
	}
	_, err := s.applyUnsigned(r.Context(), r.Header.Get("Idempotency-Key"), &servicepb.Request{
		Type: &servicepb.Request_DeleteKv{DeleteKv: &servicepb.DeleteKVRequest{Ledger: ledger, Key: key}},
	})
	if err != nil {
		handleError(w, r, err)

		return
	}
	w.WriteHeader(http.StatusNoContent)
}
