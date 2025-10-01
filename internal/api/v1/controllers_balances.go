package v1

import (
	"math/big"
	"net/http"

	"github.com/formancehq/go-libs/pointer"

	"github.com/formancehq/ledger/internal/storage/bunpaginate"

	"github.com/formancehq/go-libs/query"
	"github.com/formancehq/ledger/internal/api/backend"
	sharedapi "github.com/formancehq/ledger/internal/api/sharedapi"
	"github.com/formancehq/ledger/internal/storage/ledgerstore"
)

func buildAggregatedBalancesQuery(r *http.Request) (query.Builder, error) {
	if address := r.URL.Query().Get("address"); address != "" {
		return query.Match("address", address), nil
	}

	return nil, nil
}

func getBalancesAggregated(w http.ResponseWriter, r *http.Request) {

	pitFilter, err := getPITFilter(r)
	if err != nil {
		sharedapi.BadRequest(w, ErrValidation, err)
		return
	}

	queryBuilder, err := buildAggregatedBalancesQuery(r)
	if err != nil {
		sharedapi.BadRequest(w, ErrValidation, err)
		return
	}

	query := ledgerstore.NewGetAggregatedBalancesQuery(*pitFilter, queryBuilder,
		// notes(gfyrag): if pit is not specified, always use insertion date to be backward compatible
		r.URL.Query().Get("pit") == "" || sharedapi.QueryParamBool(r, "useInsertionDate") || sharedapi.QueryParamBool(r, "use_insertion_date"))

	balances, err := backend.LedgerFromContext(r.Context()).GetAggregatedBalances(r.Context(), query)
	if err != nil {
		sharedapi.InternalServerError(w, r, err)
		return
	}

	sharedapi.Ok(w, balances)
}

func getBalances(w http.ResponseWriter, r *http.Request) {
	l := backend.LedgerFromContext(r.Context())

	q, err := bunpaginate.Extract[ledgerstore.GetAccountsQuery](r, func() (*ledgerstore.GetAccountsQuery, error) {
		options, err := getPaginatedQueryOptionsOfPITFilterWithVolumes(r)
		if err != nil {
			return nil, err
		}
		options.QueryBuilder, err = buildAccountsFilterQuery(r)
		return pointer.For(ledgerstore.NewGetAccountsQuery(*options)), nil
	})
	if err != nil {
		sharedapi.BadRequest(w, ErrValidation, err)
		return
	}

	cursor, err := l.GetAccountsWithVolumes(r.Context(), q.WithExpandVolumes())
	if err != nil {
		sharedapi.InternalServerError(w, r, err)
		return
	}

	ret := make([]map[string]map[string]*big.Int, 0)
	for _, item := range cursor.Data {
		e := map[string]map[string]*big.Int{
			item.Address: {},
		}
		for asset, volumes := range item.Volumes {
			e[item.Address][asset] = volumes.Balance()
		}
		ret = append(ret, e)
	}

	sharedapi.RenderCursor(w, bunpaginate.Cursor[map[string]map[string]*big.Int]{
		PageSize: cursor.PageSize,
		HasMore:  cursor.HasMore,
		Previous: cursor.Previous,
		Next:     cursor.Next,
		Data:     ret,
	})
}
