package v2

import (
	"errors"
	"fmt"
	"math/big"
	"net/http"

	"github.com/formancehq/go-libs/v3/api"
	"github.com/formancehq/ledger/internal/api/common"
	ledgerstore "github.com/formancehq/ledger/internal/storage/ledger"
)

type summaryResponse struct {
	Account string   `json:"account"`
	Asset   string   `json:"asset"`
	Count   int64    `json:"count"`
	Sum     *big.Int `json:"sum"`
}

func getTransactionsSummary(w http.ResponseWriter, r *http.Request) {
	account := r.URL.Query().Get("account")
	if account == "" {
		api.BadRequest(w, common.ErrValidation, errors.New("account parameter is required"))
		return
	}

	assetFilter := r.URL.Query().Get("asset")

	// Parse time filters
	startTime, err := getDate(r, "start_time")
	if err != nil {
		api.BadRequest(w, common.ErrValidation, fmt.Errorf("invalid start_time: %w", err))
		return
	}

	endTime, err := getDate(r, "end_time")
	if err != nil {
		api.BadRequest(w, common.ErrValidation, fmt.Errorf("invalid end_time: %w", err))
		return
	}

	if startTime != nil && endTime != nil && startTime.After(*endTime) {
		api.BadRequest(w, common.ErrValidation, errors.New("start_time must be before end_time"))
		return
	}

	ledgerInstance := common.LedgerFromContext(r.Context())
	if ledgerInstance == nil {
		api.InternalServerError(w, r, errors.New("ledger not found in request context"))
		return
	}

	var transactionsSum []ledgerstore.TransactionsSummary
	if startTime == nil && endTime == nil {
		transactionsSum, err = ledgerInstance.GetTransactionsSummary(r.Context(), account)
	} else {
		transactionsSum, err = ledgerInstance.GetTransactionsSummaryWithTimeRange(r.Context(), account, startTime, endTime)
	}
	if err != nil {
		api.InternalServerError(w, r, err)
		return
	}

	// Convert to response format
	response := make([]any, 0, len(transactionsSum))
	for _, ts := range transactionsSum {
		// Apply asset filter if provided
		if assetFilter != "" && assetFilter != ts.Asset {
			continue
		}

		rendered, err := renderTransactionSummary(r, account, ts)
		if err != nil {
			api.InternalServerError(w, r, err)
			return
		}

		response = append(response, rendered)
	}

	api.Ok(w, response)
}
