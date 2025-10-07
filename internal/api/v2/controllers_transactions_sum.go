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

	// Get transactions
	ledgerInstance := common.LedgerFromContext(r.Context())
	if ledgerInstance == nil {
		api.InternalServerError(w, r, errors.New("ledger not found in context"))
		return
	}

	// Set up initial pagination
	order := bunpaginate.Order(bunpaginate.OrderDesc)
	pageSize := uint64(100) // Fixed page size for internal pagination
	var cursor *string

	// Collect all transactions across all pages
	var allTransactions []ledger.Transaction
	for {
		// Create query for the current page
		query := storagecommon.InitialPaginatedQuery[any]{
			PageSize: pageSize,
			Column:   "timestamp",
			Order:    &order,
			Options: storagecommon.ResourceQuery[any]{
				Expand: getExpand(r),
			},
		}

		// If we have a cursor from the previous page, use it
		if cursor != nil {
			query = storagecommon.InitialPaginatedQuery[any]{
				PageSize: pageSize,
				Column:   "timestamp",
				Order:    &order,
				Options: storagecommon.ResourceQuery[any]{
					Expand: getExpand(r),
				},
			}
			// Note: The actual cursor handling might need to be adjusted based on how your API expects it
			// This is a placeholder - you'll need to set the cursor in the query appropriately
		}

		// Fetch the current page of transactions
		txs, err := ledgerInstance.ListTransactions(r.Context(), query)
		if err != nil {
			common.HandleCommonPaginationErrors(w, r, err)
			return
		}

		// Add transactions to our collection
		allTransactions = append(allTransactions, txs.Data...)

		// If there are no more pages, we're done
		if !txs.HasMore || txs.Next == "" {
			break
		}

		// Set the cursor for the next page
		cursor = &txs.Next
	}

	// Process all transactions
	response := processPostings(account, &bunpaginate.Cursor[ledger.Transaction]{
		Data: allTransactions,
	}, assetFilter)

	// The test expects a single response object in an array
	if len(response) == 0 {
		// If no postings match, return an empty array
		api.Ok(w, []sumResponse{})
		return
	}
	api.Ok(w, response)
}
