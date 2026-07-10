package http

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"

	"github.com/go-chi/chi/v5"

	"github.com/formancehq/ledger/v3/internal/adapter/json"
	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/pkg/cursor"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

const (
	defaultMaxBodySize int64 = 4 << 20 // 4 MB
	defaultBulkMaxSize       = 1000    // max elements per bulk request
)

// maxBodySizeMiddleware limits the request body size using http.MaxBytesReader.
// Requests exceeding the limit receive a 413 Request Entity Too Large response.
func maxBodySizeMiddleware(maxBytes int64) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Body != nil {
				r.Body = http.MaxBytesReader(w, r.Body, maxBytes)
			}

			next.ServeHTTP(w, r)
		})
	}
}

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

// requireCanonicalID extracts, path-unescapes, and non-empty-validates the
// {canonicalId} URL parameter shared by every single-index route (get / status
// / inspect / drop, both bucket- and ledger-scoped).
//
// A canonical index id can contain characters that are reserved in a path
// segment — most importantly the metadata form `metadata:<target>:<key>` where
// <key> is a namespaced metadata key such as `formance.com/reviewed`. The
// slash and colon must be percent-encoded by the client (`%2F`, `%3A`); chi
// routes on r.URL.RawPath and hands back the still-escaped segment via
// URLParam, so ParseCanonical would otherwise see the literal `%2F`/`%3A` and
// reject or mis-parse the id. Unescape here before handing it to the caller so
// every canonical-id route addresses namespaced keys correctly.
func requireCanonicalID(w http.ResponseWriter, r *http.Request) (string, bool) {
	raw := chi.URLParam(r, "canonicalId")
	if raw == "" {
		writeBadRequest(w, "INVALID_REQUEST", errors.New("index id is required"))

		return "", false
	}

	canonical, err := url.PathUnescape(raw)
	if err != nil {
		writeBadRequest(w, "INVALID_REQUEST", fmt.Errorf("invalid index id encoding: %w", err))

		return "", false
	}

	return canonical, true
}

const (
	defaultPageSize uint32 = 100
	maxPageSize     uint32 = 1000
)

// parsePageSize extracts and validates the optional pageSize query parameter.
// Returns defaultPageSize when not specified or zero. Caps at maxPageSize.
func parsePageSize(w http.ResponseWriter, r *http.Request) (uint32, bool) {
	ps := r.URL.Query().Get("pageSize")
	if ps == "" {
		return defaultPageSize, true
	}

	parsed, err := strconv.ParseUint(ps, 10, 32)
	if err != nil {
		writeBadRequest(w, "INVALID_REQUEST", errors.New("invalid pageSize parameter"))

		return 0, false
	}

	size := uint32(parsed)
	if size == 0 {
		size = defaultPageSize
	}

	if size > maxPageSize {
		size = maxPageSize
	}

	return size, true
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
