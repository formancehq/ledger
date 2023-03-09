package controllers

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"

	"github.com/formancehq/ledger/pkg/api/apierrors"
	"github.com/formancehq/ledger/pkg/core"
	"github.com/formancehq/ledger/pkg/ledger"
	ledgerstore "github.com/formancehq/ledger/pkg/storage/sqlstorage/ledger"
	sharedapi "github.com/formancehq/stack/libs/go-libs/api"
	"github.com/go-chi/chi/v5"
)

type AccountController struct{}

func NewAccountController() AccountController {
	return AccountController{}
}

func (ctl *AccountController) CountAccounts(w http.ResponseWriter, r *http.Request) {
	l := LedgerFromContext(r.Context())

	accountsQuery := ledger.NewAccountsQuery().
		WithAddressFilter(r.URL.Query().Get("address")).
		WithMetadataFilter(sharedapi.GetQueryMap(r.URL.Query(), "metadata"))

	count, err := l.CountAccounts(r.Context(), *accountsQuery)
	if err != nil {
		apierrors.ResponseError(w, r, err)
		return
	}

	w.Header().Set("Count", fmt.Sprint(count))
}

func (ctl *AccountController) GetAccounts(w http.ResponseWriter, r *http.Request) {
	l := LedgerFromContext(r.Context())

	accountsQuery := ledger.NewAccountsQuery()

	if r.URL.Query().Get(QueryKeyCursor) != "" {
		if r.URL.Query().Get("after") != "" ||
			r.URL.Query().Get("address") != "" ||
			len(sharedapi.GetQueryMap(r.URL.Query(), "metadata")) > 0 ||
			r.URL.Query().Get("balance") != "" ||
			r.URL.Query().Get(QueryKeyBalanceOperator) != "" ||
			r.URL.Query().Get(QueryKeyPageSize) != "" {
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

		token := ledgerstore.AccountsPaginationToken{}
		if err := json.Unmarshal(res, &token); err != nil {
			apierrors.ResponseError(w, r, ledger.NewValidationError(
				fmt.Sprintf("invalid '%s' query param", QueryKeyCursor)))
			return
		}

		accountsQuery = accountsQuery.
			WithOffset(token.Offset).
			WithAfterAddress(token.AfterAddress).
			WithAddressFilter(token.AddressRegexpFilter).
			WithBalanceFilter(token.BalanceFilter).
			WithBalanceOperatorFilter(token.BalanceOperatorFilter).
			WithMetadataFilter(token.MetadataFilter).
			WithPageSize(token.PageSize)

	} else {
		balance := r.URL.Query().Get("balance")
		if balance != "" {
			if _, err := strconv.ParseInt(balance, 10, 64); err != nil {
				apierrors.ResponseError(w, r, ledger.NewValidationError(
					"invalid parameter 'balance', should be a number"))
				return
			}
		}

		balanceOperator, err := getBalanceOperator(w, r)
		if err != nil {
			apierrors.ResponseError(w, r, err)
			return
		}

		pageSize, err := getPageSize(w, r)
		if err != nil {
			apierrors.ResponseError(w, r, err)
			return
		}

		accountsQuery = accountsQuery.
			WithAfterAddress(r.URL.Query().Get("after")).
			WithAddressFilter(r.URL.Query().Get("address")).
			WithBalanceFilter(balance).
			WithBalanceOperatorFilter(balanceOperator).
			WithMetadataFilter(sharedapi.GetQueryMap(r.URL.Query(), "metadata")).
			WithPageSize(pageSize)
	}

	cursor, err := l.GetAccounts(r.Context(), *accountsQuery)
	if err != nil {
		apierrors.ResponseError(w, r, err)
		return
	}

	sharedapi.RenderCursor(w, cursor)
}

func (ctl *AccountController) GetAccount(w http.ResponseWriter, r *http.Request) {
	l := LedgerFromContext(r.Context())

	if !core.ValidateAddress(chi.URLParam(r, "address")) {
		apierrors.ResponseError(w, r, ledger.NewValidationError("invalid account address format"))
		return
	}

	acc, err := l.GetAccount(
		r.Context(),
		chi.URLParam(r, "address"))
	if err != nil {
		apierrors.ResponseError(w, r, err)
		return
	}

	sharedapi.Ok(w, acc)
}

func (ctl *AccountController) PostAccountMetadata(w http.ResponseWriter, r *http.Request) {
	l := LedgerFromContext(r.Context())

	if !core.ValidateAddress(chi.URLParam(r, "address")) {
		apierrors.ResponseError(w, r, ledger.NewValidationError("invalid account address format"))
		return
	}

	var m core.Metadata
	if err := json.NewDecoder(r.Body).Decode(&m); err != nil {
		apierrors.ResponseError(w, r, ledger.NewValidationError("invalid metadata format"))
		return
	}

	if err := l.SaveMeta(r.Context(),
		core.MetaTargetTypeAccount, chi.URLParam(r, "address"), m); err != nil {
		apierrors.ResponseError(w, r, err)
		return
	}

	sharedapi.NoContent(w)
}
