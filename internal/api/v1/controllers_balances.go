package v1

import (
	"math/big"
	"net/http"

	"github.com/formancehq/ledger/internal/api/shared"

	"github.com/formancehq/ledger/internal/engine/command"
	"github.com/formancehq/ledger/internal/storage/ledgerstore"
	"github.com/formancehq/ledger/internal/storage/paginate"
	"github.com/formancehq/ledger/internal/storage/query"
	sharedapi "github.com/formancehq/stack/libs/go-libs/api"
	"github.com/formancehq/stack/libs/go-libs/errorsutil"
	"github.com/pkg/errors"
)

func buildAggregatedBalancesQuery(r *http.Request) (query.Builder, error) {
	if address := r.URL.Query().Get("address"); address != "" {
		return query.Match("address", address), nil
	}

	return nil, nil
}

func getBalancesAggregated(w http.ResponseWriter, r *http.Request) {
	options, err := getPaginatedQueryOptionsOfPITFilter(r)
	if err != nil {
		sharedapi.BadRequest(w, ErrValidation, err)
		return
	}

	query := ledgerstore.NewGetAggregatedBalancesQuery(*options)
	query.Options.QueryBuilder, err = buildAggregatedBalancesQuery(r)

	balances, err := shared.LedgerFromContext(r.Context()).GetAggregatedBalances(r.Context(), query)
	if err != nil {
		ResponseError(w, r, err)
		return
	}

	sharedapi.Ok(w, balances)
}

func getBalances(w http.ResponseWriter, r *http.Request) {
	l := shared.LedgerFromContext(r.Context())

	q := &ledgerstore.GetAccountsQuery{}

	if r.URL.Query().Get(QueryKeyCursor) != "" {
		err := paginate.UnmarshalCursor(r.URL.Query().Get(QueryKeyCursor), q)
		if err != nil {
			ResponseError(w, r, errorsutil.NewError(command.ErrValidation, errors.Errorf("invalid '%s' query param", QueryKeyCursor)))
			return
		}
	} else {
		options, err := getPaginatedQueryOptionsOfPITFilterWithVolumes(r)
		if err != nil {
			sharedapi.BadRequest(w, ErrValidation, err)
			return
		}
		options.QueryBuilder, err = buildAccountsFilterQuery(r)
		q = ledgerstore.NewGetAccountsQuery(*options)
	}

	cursor, err := l.GetAccountsWithVolumes(r.Context(), q)
	if err != nil {
		ResponseError(w, r, err)
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

	sharedapi.RenderCursor(w, sharedapi.Cursor[map[string]map[string]*big.Int]{
		PageSize: cursor.PageSize,
		HasMore:  cursor.HasMore,
		Previous: cursor.Previous,
		Next:     cursor.Next,
		Data:     ret,
	})
}
