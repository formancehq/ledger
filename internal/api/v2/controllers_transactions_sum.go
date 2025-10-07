package v2

import (
	"errors"
	"fmt"
	"math/big"
	"net/http"
	"time"

	"github.com/formancehq/go-libs/v3/api"
	"github.com/formancehq/go-libs/v3/bun/bunpaginate"
	"github.com/formancehq/go-libs/v3/query"
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
	assetSums := make(map[string]*big.Int)

	for _, tx := range txs.Data {
		for _, posting := range tx.Postings {
			if posting.Source == account {
				if _, ok := assetSums[posting.Asset]; !ok {
					assetSums[posting.Asset] = big.NewInt(0)
				}
				assetSums[posting.Asset] = new(big.Int).Sub(assetSums[posting.Asset], posting.Amount)
			} else if posting.Destination == account {
				if _, ok := assetSums[posting.Asset]; !ok {
					assetSums[posting.Asset] = big.NewInt(0)
				}
				assetSums[posting.Asset] = new(big.Int).Add(assetSums[posting.Asset], posting.Amount)
			}
		}
	}

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

// getPaginatedTransactions fetches a page of transactions with pagination support.
func getPaginatedTransactions(
	w http.ResponseWriter,
	r *http.Request,
	ledgerInstance ledgercontroller.Controller,
	pageSize uint64,
) (*bunpaginate.Cursor[ledger.Transaction], bool) {
	order := bunpaginate.Order(bunpaginate.OrderDesc)

	// Extract query parameters
	queryParams := r.URL.Query()
	startTime := queryParams.Get("start_time")
	endTime := queryParams.Get("end_time")


	// Create a new request with the same context to avoid modifying the original
	req := r.Clone(r.Context())

	rq, err := storagecommon.Extract[any](req, func() (*storagecommon.InitialPaginatedQuery[any], error) {
		// Create a slice to hold query conditions
		var conditions []query.Builder

		// Add date range filters if provided
		if startTime != "" {
			t, err := time.Parse(time.RFC3339, startTime)
			if err != nil {
				return nil, fmt.Errorf("invalid start_time format: %w", err)
			}
			conditions = append(conditions, query.Gte("timestamp", t))
		}

		if endTime != "" {
			t, err := time.Parse(time.RFC3339, endTime)
			if err != nil {
				return nil, fmt.Errorf("invalid end_time format: %w", err)
			}
			conditions = append(conditions, query.Lte("timestamp", t))
		}

		// For test compatibility, if no conditions, don't set the Builder
		var builder query.Builder
		if len(conditions) > 0 {
			builder = query.And(conditions...)
		}

		queryOptions := storagecommon.ResourceQuery[any]{
			Expand:  getExpand(req),
			Builder: builder,
		}


		return &storagecommon.InitialPaginatedQuery[any]{
			PageSize: pageSize,
			Column:   "timestamp",
			Order:    &order,
			Options:  queryOptions,
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

	if txs.HasMore && txs.Next != "" {
		q := req.URL.Query()
		q.Set("cursor", txs.Next)
		req.URL.RawQuery = q.Encode()
	}

	return txs, true
}

func getTransactionsSum(w http.ResponseWriter, r *http.Request) {
	account := r.URL.Query().Get("account")
	if account == "" {
		api.BadRequest(w, common.ErrValidation, errors.New("account parameter is required"))
		return
	}

	assetFilter := r.URL.Query().Get("asset")

	ledgerInstance := common.LedgerFromContext(r.Context())
	if ledgerInstance == nil {
		api.InternalServerError(w, r, errors.New("ledger not found in context"))
		return
	}

	const defaultPageSize = 100

	assetSums := make(map[string]*big.Int)

	// Create a new request with the same context to avoid modifying the original
	req := r.Clone(r.Context())

	for {
		cursor, ok := getPaginatedTransactions(w, req, ledgerInstance, defaultPageSize)
		if !ok {
			return // Error already handled
		}

		pageSums := processPostings(account, cursor, "")

		for _, ps := range pageSums {
			if _, exists := assetSums[ps.Asset]; !exists {
				assetSums[ps.Asset] = big.NewInt(0)
			}
			assetSums[ps.Asset] = new(big.Int).Add(assetSums[ps.Asset], ps.Sum)
		}

		if !cursor.HasMore || cursor.Next == "" {
			break
		}

	}

	response := make([]sumResponse, 0, len(assetSums))
	for asset, sum := range assetSums {
		if assetFilter != "" && assetFilter != asset {
			continue
		}
		response = append(response, sumResponse{
			Account: account,
			Asset:   asset,
			Sum:     sum,
		})
	}

	if assetFilter != "" && len(response) == 0 {
		api.Ok(w, []sumResponse{})
		return
	}

	api.Ok(w, response)
}
