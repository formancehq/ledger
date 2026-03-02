package http

import (
	"errors"
	"io"
	"net/http"
	"strconv"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/query"
	"github.com/go-chi/chi/v5"
)

// handleListAccounts handles GET /{ledgerName}/accounts to list accounts
func (s *Server) handleListAccounts(w http.ResponseWriter, r *http.Request) {
	ledgerName := chi.URLParam(r, "ledgerName")
	if ledgerName == "" {
		writeBadRequest(w, "INVALID_REQUEST", errors.New("ledger name is required"))
		return
	}

	// Parse query parameters
	var pageSize uint32
	if ps := r.URL.Query().Get("pageSize"); ps != "" {
		parsed, err := strconv.ParseUint(ps, 10, 32)
		if err != nil {
			writeBadRequest(w, "INVALID_REQUEST", errors.New("invalid pageSize parameter"))
			return
		}
		pageSize = uint32(parsed)
	}

	afterAddress := r.URL.Query().Get("after")
	prefix := r.URL.Query().Get("prefix")

	// Build an optional address-prefix filter from query parameter
	var filter *commonpb.QueryFilter
	if prefix != "" {
		filter = &commonpb.QueryFilter{
			Filter: &commonpb.QueryFilter_Address{
				Address: &commonpb.AddressMatch{
					Match: &commonpb.AddressMatch_HardcodedPrefix{HardcodedPrefix: prefix},
				},
			},
		}
	}

	reverse := r.URL.Query().Get("reverse") == "true"

	ctx, profile := query.WithProfile(r.Context())
	cursor, err := s.backend.ListAccounts(ctx, ledgerName, pageSize, afterAddress, filter, reverse)
	if err != nil {
		handleError(w, r, err)
		return
	}
	defer func() {
		_ = cursor.Close()
	}()

	var accounts []*commonpb.Account
	for {
		account, err := cursor.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			handleError(w, r, err)
			return
		}
		accounts = append(accounts, account)
	}

	if wantsHTTPProfile(r) {
		writeProfileHeader(w, profile)
	}
	writeOK(w, accounts)
}
