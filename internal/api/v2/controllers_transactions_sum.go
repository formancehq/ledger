package v2

import (
	"errors"
	"math/big"
	"net/http"

	"github.com/formancehq/go-libs/v3/api"
	"github.com/formancehq/go-libs/v3/bun/bunpaginate"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/api/common"
	storagecommon "github.com/formancehq/ledger/internal/storage/common"
)

type sumResponse struct {
	Account string   `json:"account"`
	Asset   string   `json:"asset"`
	Sum     *big.Int `json:"sum"`
}

func processPostings(account string, txs *bunpaginate.Cursor[ledger.Transaction], assetFilter string) []sumResponse {
	// Calculate sums per asset
	assetSums := make(map[string]*big.Int)

	for _, tx := range txs.Data {
		for _, posting := range tx.Postings {
			if posting.Source == account {
				// Debit from the account (negative amount)
				if _, ok := assetSums[posting.Asset]; !ok {
					assetSums[posting.Asset] = big.NewInt(0)
				}
				assetSums[posting.Asset] = new(big.Int).Sub(assetSums[posting.Asset], posting.Amount)
			} else if posting.Destination == account {
				// Credit to the account (positive amount)
				if _, ok := assetSums[posting.Asset]; !ok {
					assetSums[posting.Asset] = big.NewInt(0)
				}
				assetSums[posting.Asset] = new(big.Int).Add(assetSums[posting.Asset], posting.Amount)
			}
		}
	}

	// Prepare response
	response := make([]sumResponse, 0, len(assetSums))
	for asset, amount := range assetSums {
		// If a specific asset was requested, only include that asset in the response
		if assetFilter != "" && assetFilter != asset {
			continue
		}
		response = append(response, sumResponse{
			Account: account,
			Asset:   asset,
			Sum:     amount,
		})
	}

	return response
}

func getTransactionsSum(w http.ResponseWriter, r *http.Request) {
	// Get account from query parameters
	account := r.URL.Query().Get("account")
	if account == "" {
		api.BadRequest(w, common.ErrValidation, errors.New("account parameter is required"))
		return
	}

	// Get asset from query parameters (empty means all assets)
	assetFilter := r.URL.Query().Get("asset")

	// Get pagination parameters
	pageSize, err := bunpaginate.GetPageSize(r)
	if err != nil {
		api.BadRequest(w, common.ErrValidation, err)
		return
	}

	// Create pagination query
	order := bunpaginate.Order(bunpaginate.OrderDesc)
	rq := storagecommon.InitialPaginatedQuery[any]{
		PageSize: pageSize,
		Column:   "timestamp",
		Order:    &order,
		Options: storagecommon.ResourceQuery[any]{
			Expand: getExpand(r),
		},
	}

	// Get transactions
	ledgerInstance := common.LedgerFromContext(r.Context())
	if ledgerInstance == nil {
		api.InternalServerError(w, r, errors.New("ledger not found in context"))
		return
	}

	txs, err := ledgerInstance.ListTransactions(r.Context(), rq)
	if err != nil {
		common.HandleCommonPaginationErrors(w, r, err)
		return
	}

	response := processPostings(account, txs, assetFilter)
	// The test expects a single response object in an array
	if len(response) == 0 {
		// If no postings match, return an empty array
		api.Ok(w, []sumResponse{})
		return
	}
	api.Ok(w, response)
}
