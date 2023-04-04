package controllers

import (
	"net/http"

	"github.com/formancehq/ledger/pkg/api/apierrors"
	"github.com/formancehq/ledger/pkg/ledger"
	"github.com/formancehq/ledger/pkg/storage"
	sharedapi "github.com/formancehq/stack/libs/go-libs/api"
	"github.com/formancehq/stack/libs/go-libs/errorsutil"
	"github.com/pkg/errors"
)

func GetBalancesAggregated(w http.ResponseWriter, r *http.Request) {
	l := LedgerFromContext(r.Context())

	balancesQuery := storage.NewBalancesQuery().WithAddressFilter(r.URL.Query().Get("address"))
	balances, err := l.GetBalancesAggregated(r.Context(), balancesQuery)
	if err != nil {
		apierrors.ResponseError(w, r, err)
		return
	}

	sharedapi.Ok(w, balances)
}

func GetBalances(w http.ResponseWriter, r *http.Request) {
	l := LedgerFromContext(r.Context())

	balancesQuery := storage.NewBalancesQuery()

	if r.URL.Query().Get(QueryKeyCursor) != "" {
		if r.URL.Query().Get("after") != "" ||
			r.URL.Query().Get("address") != "" ||
			r.URL.Query().Get(QueryKeyPageSize) != "" {
			apierrors.ResponseError(w, r, errorsutil.NewError(ledger.ErrValidation,
				errors.Errorf("no other query params can be set with '%s'", QueryKeyCursor)))
			return
		}

		err := storage.UnmarshalCursor(r.URL.Query().Get(QueryKeyCursor), &balancesQuery)
		if err != nil {
			apierrors.ResponseError(w, r, errorsutil.NewError(ledger.ErrValidation,
				errors.Errorf("invalid '%s' query param", QueryKeyCursor)))
			return
		}

	} else {
		pageSize, err := getPageSize(r)
		if err != nil {
			apierrors.ResponseError(w, r, err)
			return
		}

		balancesQuery = balancesQuery.
			WithAfterAddress(r.URL.Query().Get("after")).
			WithAddressFilter(r.URL.Query().Get("address")).
			WithPageSize(pageSize)
	}

	cursor, err := l.GetBalances(r.Context(), balancesQuery)
	if err != nil {
		apierrors.ResponseError(w, r, err)
		return
	}

	sharedapi.RenderCursor(w, *cursor)
}
