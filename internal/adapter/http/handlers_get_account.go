package http

import (
	"context"
	"errors"
	"math/big"
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/query"
)

// handleGetAccount handles GET /{ledgerName}/accounts/{address} to retrieve an account.
//
// When `expandVolumes=true` is set, an extra AggregateVolumes scan runs against
// the storage layer (filter: hardcoded_exact = address) and the result is
// folded into `Account.volumes`. The default (no query param) keeps the read
// lightweight — callers who need per-asset input/output/balance must opt in.
// v2 populated volumes unconditionally; v3 chose opt-in because volumes are a
// projection computed on demand, not a first-class stored attribute.
func (s *Server) handleGetAccount(w http.ResponseWriter, r *http.Request) {
	ledgerName, ok := requireLedgerName(w, r)
	if !ok {
		return
	}

	address := chi.URLParam(r, "address")
	if address == "" {
		writeBadRequest(w, "INVALID_REQUEST", errors.New("account address is required"))

		return
	}

	// Verify ledger exists
	_, err := s.backend.GetLedgerByName(r.Context(), ledgerName)
	if err != nil {
		handleError(w, r, err)

		return
	}

	account, err := s.backend.GetAccount(r.Context(), ledgerName, address)
	if err != nil {
		s.logger.WithFields(map[string]any{
			"ledger":  ledgerName,
			"address": address,
			"error":   err,
		}).Errorf("Failed to get account")
		handleError(w, r, err)

		return
	}

	if queryParamBool(r, "expandVolumes") {
		err = s.expandAccountVolumes(r.Context(), ledgerName, address, account)
		if err != nil {
			handleError(w, r, err)

			return
		}
	}

	writeOK(w, account)
}

// expandAccountVolumes runs an exact-match AggregateVolumes on the account
// address and folds the result into `account.Volumes` as a per-asset
// input/output/balance map, matching the shape v2 populated inline.
func (s *Server) expandAccountVolumes(ctx context.Context, ledgerName, address string, account *commonpb.Account) error {
	filter := &commonpb.QueryFilter{
		Filter: &commonpb.QueryFilter_Address{
			Address: &commonpb.AddressMatch{
				Match: &commonpb.AddressMatch_HardcodedExact{HardcodedExact: address},
			},
		},
	}

	result, err := s.backend.AggregateVolumes(ctx, ledgerName, filter, query.AggregateOptions{})
	if err != nil {
		return err
	}

	volumes := make(map[string]*commonpb.VolumesWithBalance, len(result.GetVolumes()))
	for _, v := range result.GetVolumes() {
		input := v.GetInput().ToBigInt()
		output := v.GetOutput().ToBigInt()
		balance := new(big.Int).Sub(input, output)
		volumes[v.GetAsset()] = &commonpb.VolumesWithBalance{
			Input:   input.String(),
			Output:  output.String(),
			Balance: balance.String(),
		}
	}

	account.Volumes = volumes

	return nil
}
