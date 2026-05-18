package http

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"

	"github.com/formancehq/ledger-v3-poc/internal/adapter/json"
	"github.com/formancehq/ledger-v3-poc/internal/domain"
	"github.com/formancehq/ledger-v3-poc/internal/pkg/cursor"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
)

// requireLedgerName extracts and validates the ledgerName URL parameter.
// Returns the ledger name and true on success; writes a 400 response and returns false on failure.
func requireLedgerName(w http.ResponseWriter, r *http.Request) (string, bool) {
	ledgerName := chi.URLParam(r, "ledgerName")
	if err := domain.ValidateLedgerName(ledgerName); err != nil {
		writeBadRequest(w, "INVALID_REQUEST", err)

		return "", false
	}

	return ledgerName, true
}

// requireTransactionID extracts and validates the transactionId URL parameter.
// Returns the transaction ID and true on success; writes a 400 response and returns false on failure.
func requireTransactionID(w http.ResponseWriter, r *http.Request) (uint64, bool) {
	transactionIDRaw := chi.URLParam(r, "transactionId")
	if transactionIDRaw == "" {
		writeBadRequest(w, "INVALID_REQUEST", errors.New("transaction id is required"))

		return 0, false
	}

	transactionID, err := strconv.ParseUint(transactionIDRaw, 10, 64)
	if err != nil {
		writeBadRequest(w, "INVALID_REQUEST", fmt.Errorf("invalid transaction id: %w", err))

		return 0, false
	}

	return transactionID, true
}

// parsePageSize extracts and validates the optional pageSize query parameter.
// Returns the page size (0 if not specified) and true on success; writes a 400 response and returns false on failure.
func parsePageSize(w http.ResponseWriter, r *http.Request) (uint32, bool) {
	ps := r.URL.Query().Get("pageSize")
	if ps == "" {
		return 0, true
	}

	parsed, err := strconv.ParseUint(ps, 10, 32)
	if err != nil {
		writeBadRequest(w, "INVALID_REQUEST", errors.New("invalid pageSize parameter"))

		return 0, false
	}

	return uint32(parsed), true
}

// parseMetadataBody reads and validates a metadata JSON body from the request.
// Returns the parsed metadata map and true on success; writes a 400 response and returns false on failure.
func parseMetadataBody(w http.ResponseWriter, r *http.Request) (map[string]*commonpb.MetadataValue, bool) {
	var inputMetadata map[string]any
	if err := json.UnmarshalRead(r.Body, &inputMetadata); err != nil {
		writeBadRequest(w, "INVALID_REQUEST", fmt.Errorf("invalid request body: %w", err))

		return nil, false
	}

	ms, err := commonpb.MetadataFromAnyMap(inputMetadata)
	if err != nil {
		writeBadRequest(w, "INVALID_REQUEST", fmt.Errorf("invalid metadata: %w", err))

		return nil, false
	}

	return ms, true
}

// drainCursor collects all items from a cursor into a slice.
// Closes the cursor and writes an error response on failure, returning false.
func drainCursor[T any](w http.ResponseWriter, r *http.Request, cursor cursor.Cursor[T]) ([]T, bool) {
	defer func() {
		_ = cursor.Close()
	}()

	var items []T

	for {
		item, err := cursor.Next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}

			handleError(w, r, err)

			return nil, false
		}

		items = append(items, item)
	}

	return items, true
}
