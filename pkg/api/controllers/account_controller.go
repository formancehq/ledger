package controllers

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"

	"github.com/formancehq/ledger/pkg/api/apierrors"
	"github.com/formancehq/ledger/pkg/core"
	"github.com/formancehq/ledger/pkg/ledger/command"
	"github.com/formancehq/ledger/pkg/storage/ledgerstore"
	sharedapi "github.com/formancehq/stack/libs/go-libs/api"
	"github.com/formancehq/stack/libs/go-libs/errorsutil"
	"github.com/formancehq/stack/libs/go-libs/metadata"
	"github.com/go-chi/chi/v5"
	"github.com/pkg/errors"
)

func CountAccounts(w http.ResponseWriter, r *http.Request) {
	l := LedgerFromContext(r.Context())

	accountsQuery := ledgerstore.NewAccountsQuery().
		WithAddressFilter(r.URL.Query().Get("address")).
		WithMetadataFilter(sharedapi.GetQueryMap(r.URL.Query(), "metadata"))

	count, err := l.CountAccounts(r.Context(), accountsQuery)
	if err != nil {
		apierrors.ResponseError(w, r, err)
		return
	}

	w.Header().Set("Count", fmt.Sprint(count))
	sharedapi.NoContent(w)
}

func GetAccounts(w http.ResponseWriter, r *http.Request) {
	l := LedgerFromContext(r.Context())

	accountsQuery := ledgerstore.NewAccountsQuery()

	if r.URL.Query().Get(QueryKeyCursor) != "" {
		if r.URL.Query().Get("after") != "" ||
			r.URL.Query().Get("address") != "" ||
			len(sharedapi.GetQueryMap(r.URL.Query(), "metadata")) > 0 ||
			r.URL.Query().Get("balance") != "" ||
			r.URL.Query().Get(QueryKeyBalanceOperator) != "" ||
			r.URL.Query().Get(QueryKeyPageSize) != "" {
			apierrors.ResponseError(w, r, errorsutil.NewError(command.ErrValidation,
				errors.Errorf("no other query params can be set with '%s'", QueryKeyCursor)))
			return
		}

		err := ledgerstore.UnmarshalCursor(r.URL.Query().Get(QueryKeyCursor), &accountsQuery)
		if err != nil {
			apierrors.ResponseError(w, r, errorsutil.NewError(command.ErrValidation,
				errors.Errorf("invalid '%s' query param", QueryKeyCursor)))
			return
		}
	} else {
		balance := r.URL.Query().Get("balance")
		if balance != "" {
			if _, err := strconv.ParseInt(balance, 10, 64); err != nil {
				apierrors.ResponseError(w, r, errorsutil.NewError(command.ErrValidation,
					errors.New("invalid parameter 'balance', should be a number")))
				return
			}
		}

		pageSize, err := getPageSize(r)
		if err != nil {
			apierrors.ResponseError(w, r, err)
			return
		}

		accountsQuery = accountsQuery.
			WithAfterAddress(r.URL.Query().Get("after")).
			WithAddressFilter(r.URL.Query().Get("address")).
			WithMetadataFilter(sharedapi.GetQueryMap(r.URL.Query(), "metadata")).
			WithPageSize(pageSize)
	}

	cursor, err := l.GetAccounts(r.Context(), accountsQuery)
	if err != nil {
		apierrors.ResponseError(w, r, err)
		return
	}

	sharedapi.RenderCursor(w, *cursor)
}

func GetAccount(w http.ResponseWriter, r *http.Request) {
	l := LedgerFromContext(r.Context())

	acc, err := l.GetAccount(r.Context(), chi.URLParam(r, "address"))
	if err != nil {
		apierrors.ResponseError(w, r, err)
		return
	}

	sharedapi.Ok(w, acc)
}

func PostAccountMetadata(w http.ResponseWriter, r *http.Request) {
	l := LedgerFromContext(r.Context())

	if !core.ValidateAddress(chi.URLParam(r, "address")) {
		apierrors.ResponseError(w, r, errorsutil.NewError(command.ErrValidation,
			errors.New("invalid account address format")))
		return
	}

	var m metadata.Metadata
	if err := json.NewDecoder(r.Body).Decode(&m); err != nil {
		apierrors.ResponseError(w, r, errorsutil.NewError(command.ErrValidation,
			errors.New("invalid metadata format")))
		return
	}

	err := l.SaveMeta(r.Context(), getCommandParameters(r), core.MetaTargetTypeAccount, chi.URLParam(r, "address"), m)
	if err != nil {
		apierrors.ResponseError(w, r, err)
		return
	}

	sharedapi.NoContent(w)
}
