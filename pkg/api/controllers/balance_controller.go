package controllers

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"

	sharedapi "github.com/formancehq/stack/libs/go-libs/api"
	"github.com/numary/ledger/pkg/api/apierrors"
	"github.com/numary/ledger/pkg/ledger"
	"github.com/numary/ledger/pkg/storage/sqlstorage"
)

type BalanceController struct{}

func NewBalanceController() BalanceController {
	return BalanceController{}
}

func (ctl *BalanceController) GetBalancesAggregated(w http.ResponseWriter, r *http.Request) {
	l := LedgerFromContext(r.Context())

	balancesQuery := ledger.NewBalancesQuery().
		WithAddressFilter(r.URL.Query().Get("address"))
	balances, err := l.GetBalancesAggregated(
		r.Context(), *balancesQuery)
	if err != nil {
		apierrors.ResponseError(w, r, err)
		return
	}

	sharedapi.Ok(w, balances)
}

func (ctl *BalanceController) GetBalances(w http.ResponseWriter, r *http.Request) {
	l := LedgerFromContext(r.Context())

	balancesQuery := ledger.NewBalancesQuery()

	if r.URL.Query().Get(QueryKeyCursor) != "" {
		if r.URL.Query().Get("after") != "" ||
			r.URL.Query().Get("address") != "" ||
			r.URL.Query().Get(QueryKeyPageSize) != "" ||
			r.URL.Query().Get(QueryKeyPageSizeDeprecated) != "" {
			apierrors.ResponseError(w, r, ledger.NewValidationError(
				fmt.Sprintf("no other query params can be set with '%s'", QueryKeyCursor)))
			return
		}

		res, err := base64.RawURLEncoding.DecodeString(r.URL.Query().Get(QueryKeyCursor))
		if err != nil {
			apierrors.ResponseError(w, r, ledger.NewValidationError(
				fmt.Sprintf("invalid '%s' query param", QueryKeyCursor)))
			return
		}

		token := sqlstorage.BalancesPaginationToken{}
		if err := json.Unmarshal(res, &token); err != nil {
			apierrors.ResponseError(w, r, ledger.NewValidationError(
				fmt.Sprintf("invalid '%s' query param", QueryKeyCursor)))
			return
		}

		balancesQuery = balancesQuery.
			WithOffset(token.Offset).
			WithAfterAddress(token.AfterAddress).
			WithAddressFilter(token.AddressRegexpFilter).
			WithPageSize(token.PageSize)

	} else if r.URL.Query().Get(QueryKeyCursorDeprecated) != "" {
		if r.URL.Query().Get("after") != "" ||
			r.URL.Query().Get("address") != "" ||
			r.URL.Query().Get(QueryKeyPageSize) != "" ||
			r.URL.Query().Get(QueryKeyPageSizeDeprecated) != "" {
			apierrors.ResponseError(w, r, ledger.NewValidationError(
				fmt.Sprintf("no other query params can be set with '%s'", QueryKeyCursorDeprecated)))
			return
		}

		res, err := base64.RawURLEncoding.DecodeString(r.URL.Query().Get(QueryKeyCursorDeprecated))
		if err != nil {
			apierrors.ResponseError(w, r, ledger.NewValidationError(
				fmt.Sprintf("invalid '%s' query param", QueryKeyCursorDeprecated)))
			return
		}

		token := sqlstorage.BalancesPaginationToken{}
		if err := json.Unmarshal(res, &token); err != nil {
			apierrors.ResponseError(w, r, ledger.NewValidationError(
				fmt.Sprintf("invalid '%s' query param", QueryKeyCursorDeprecated)))
			return
		}

		balancesQuery = balancesQuery.
			WithOffset(token.Offset).
			WithAfterAddress(token.AfterAddress).
			WithAddressFilter(token.AddressRegexpFilter).
			WithPageSize(token.PageSize)

	} else {
		pageSize, err := getPageSize(w, r)
		if err != nil {
			apierrors.ResponseError(w, r, err)
			return
		}

		balancesQuery = balancesQuery.
			WithAfterAddress(r.URL.Query().Get("after")).
			WithAddressFilter(r.URL.Query().Get("address")).
			WithPageSize(pageSize)
	}

	cursor, err := l.GetBalances(r.Context(), *balancesQuery)
	if err != nil {
		apierrors.ResponseError(w, r, err)
		return
	}

	sharedapi.RenderCursor(w, cursor)
}
