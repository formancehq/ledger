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

type sumResponse struct {
	Account string   `json:"account"`
	Asset   string   `json:"asset"`
	Sum     *big.Int `json:"sum"`
}

func getTransactionsSum(w http.ResponseWriter, r *http.Request) {
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

	ledgerInstance := common.LedgerFromContext(r.Context())
	if ledgerInstance == nil {
		api.InternalServerError(w, r, errors.New("ledger not found in request context"))
		return
	}

	var transactionsSum []ledgerstore.TransactionsSum
	if startTime == nil && endTime == nil {
		transactionsSum, err = ledgerInstance.GetTransactionsSum(r.Context(), account)
	} else {
		transactionsSum, err = ledgerInstance.GetTransactionsSumWithTimeRange(r.Context(), account, startTime, endTime)
	}
	if err != nil {
		api.InternalServerError(w, r, err)
		return
	}

	// Convert to response format
	response := make([]sumResponse, 0, len(transactionsSum))
	for _, ts := range transactionsSum {
		// Apply asset filter if provided
		if assetFilter != "" && assetFilter != ts.Asset {
			continue
		}

		// Parse the sum from string to big.Int exactly
		sum := new(big.Int)
		if _, ok := sum.SetString(ts.Sum, 10); !ok {
			api.InternalServerError(w, r, fmt.Errorf("invalid sum format: %s", ts.Sum))
			return
		}

		response = append(response, sumResponse{
			Account: account,
			Asset:   ts.Asset,
			Sum:     sum,
		})
	}

	api.Ok(w, response)
}
