package v2

import (
	"errors"
	"math/big"
	"net/http"

	"github.com/formancehq/go-libs/v3/api"
	"github.com/formancehq/go-libs/v3/bun/bunpaginate"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/api/common"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
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
				// When account is the source, it's a debit (decrease balance)
				if _, ok := assetSums[posting.Asset]; !ok {
					assetSums[posting.Asset] = big.NewInt(0)
				}
				assetSums[posting.Asset] = new(big.Int).Sub(assetSums[posting.Asset], posting.Amount)
			} else if posting.Destination == account {
				// When account is the destination, it's a credit (increase balance)
				if _, ok := assetSums[posting.Asset]; !ok {
					assetSums[posting.Asset] = big.NewInt(0)
				}
				assetSums[posting.Asset] = new(big.Int).Add(assetSums[posting.Asset], posting.Amount)
			}
		}
	}

	// If a specific asset was requested, only include that asset in the response
	if assetFilter != "" {
		if amount, ok := assetSums[assetFilter]; ok {
			return []sumResponse{{
				Account: account,
				Asset:   assetFilter,
				Sum:     amount,
			}}
		}
		return []sumResponse{}
	}

	// Prepare response for all assets
	response := make([]sumResponse, 0, len(assetSums))
	for asset, amount := range assetSums {
		response = append(response, sumResponse{
			Account: account,
			Asset:   asset,
			Sum:     amount,
		})
	}

	return response
}

// getPaginatedTransactions fetches a single page of transactions
func getPaginatedTransactions(
	w http.ResponseWriter,
	r *http.Request,
	ledgerInstance ledgercontroller.Controller,
	pageSize uint64,
) (*bunpaginate.Cursor[ledger.Transaction], bool) {
	order := bunpaginate.Order(bunpaginate.OrderDesc)

	// Use Extract to handle pagination, including cursor tokens
	rq, err := storagecommon.Extract[any](r, func() (*storagecommon.InitialPaginatedQuery[any], error) {
		return &storagecommon.InitialPaginatedQuery[any]{
			PageSize: pageSize,
			Column:   "timestamp",
			Order:    &order,
			Options: storagecommon.ResourceQuery[any]{
				Expand: getExpand(r),
			},
		}, nil
	})
	if err != nil {
		api.BadRequest(w, common.ErrValidation, err)
		return nil, false
	}

	txs, err := ledgerInstance.ListTransactions(r.Context(), rq)
	if err != nil {
		common.HandleCommonPaginationErrors(w, r, err)
		return nil, false
	}
	return txs, true
}

func getTransactionsSum(w http.ResponseWriter, r *http.Request) {
	// Get account from query parameters
	account := r.URL.Query().Get("account")
	if account == "" {
		api.BadRequest(w, common.ErrValidation, errors.New("account parameter is required"))
		return
	}

	// Get asset filter if provided
	assetFilter := r.URL.Query().Get("asset")

	// Get transactions
	ledgerInstance := common.LedgerFromContext(r.Context())
	if ledgerInstance == nil {
		api.InternalServerError(w, r, errors.New("ledger not found in context"))
		return
	}

	// Use a reasonable default page size
	const defaultPageSize = 100
	assetSums := make(map[string]*big.Int)
	var cursor *bunpaginate.Cursor[ledger.Transaction]
	var ok bool

	// Process all pages of transactions
	for {
		cursor, ok = getPaginatedTransactions(w, r, ledgerInstance, defaultPageSize)
		if !ok {
			return // Error already handled
		}

		// Process the current page of transactions
		pageSums := processPostings(account, cursor, "") // Don't filter by asset yet

		// Accumulate sums for each asset
		for _, ps := range pageSums {
			if _, exists := assetSums[ps.Asset]; !exists {
				assetSums[ps.Asset] = big.NewInt(0)
			}
			assetSums[ps.Asset] = new(big.Int).Add(assetSums[ps.Asset], ps.Sum)
		}

		// If there are no more pages, break the loop
		if !cursor.HasMore || cursor.Next == "" {
			break
		}
	}

	// Prepare the response
	response := make([]sumResponse, 0, len(assetSums))
	for asset, sum := range assetSums {
		// Skip if this asset doesn't match the filter (if any)
		if assetFilter != "" && assetFilter != asset {
			continue
		}
		response = append(response, sumResponse{
			Account: account,
			Asset:   asset,
			Sum:     sum,
		})
	}

	// If we have an asset filter but no matching asset, return empty array
	if assetFilter != "" && len(response) == 0 {
		api.Ok(w, []sumResponse{})
		return
	}

	// Return the response
	api.Ok(w, response)
}
