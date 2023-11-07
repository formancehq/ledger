package v2

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/formancehq/ledger/internal/api/shared"

	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/engine/command"
	"github.com/formancehq/ledger/internal/storage/ledgerstore"
	"github.com/formancehq/ledger/internal/storage/paginate"
	sharedapi "github.com/formancehq/stack/libs/go-libs/api"
	"github.com/formancehq/stack/libs/go-libs/collectionutils"
	"github.com/formancehq/stack/libs/go-libs/errorsutil"
	"github.com/formancehq/stack/libs/go-libs/metadata"
	"github.com/go-chi/chi/v5"
	"github.com/pkg/errors"
)

func countAccounts(w http.ResponseWriter, r *http.Request) {
	l := shared.LedgerFromContext(r.Context())

	options, err := getPaginatedQueryOptionsOfPITFilterWithVolumes(r)
	if err != nil {
		sharedapi.BadRequest(w, shared.ErrValidation, err)
		return
	}

	count, err := l.CountAccounts(r.Context(), ledgerstore.NewGetAccountsQuery(*options))
	if err != nil {
		shared.ResponseError(w, r, err)
		return
	}

	w.Header().Set("Count", fmt.Sprint(count))
	sharedapi.NoContent(w)
}

func getAccounts(w http.ResponseWriter, r *http.Request) {
	l := shared.LedgerFromContext(r.Context())

	query := &ledgerstore.GetAccountsQuery{}

	if r.URL.Query().Get(QueryKeyCursor) != "" {
		err := paginate.UnmarshalCursor(r.URL.Query().Get(QueryKeyCursor), query)
		if err != nil {
			shared.ResponseError(w, r, errorsutil.NewError(command.ErrValidation,
				errors.Errorf("invalid '%s' query param", QueryKeyCursor)))
			return
		}
	} else {
		options, err := getPaginatedQueryOptionsOfPITFilterWithVolumes(r)
		if err != nil {
			sharedapi.BadRequest(w, shared.ErrValidation, err)
			return
		}
		query = ledgerstore.NewGetAccountsQuery(*options)
	}

	cursor, err := l.GetAccountsWithVolumes(r.Context(), query)
	if err != nil {
		shared.ResponseError(w, r, err)
		return
	}

	sharedapi.RenderCursor(w, *cursor)
}

func getAccount(w http.ResponseWriter, r *http.Request) {
	l := shared.LedgerFromContext(r.Context())

	query := ledgerstore.NewGetAccountQuery(chi.URLParam(r, "address"))
	if collectionutils.Contains(r.URL.Query()["expand"], "volumes") {
		query = query.WithExpandVolumes()
	}
	if collectionutils.Contains(r.URL.Query()["expand"], "effectiveVolumes") {
		query = query.WithExpandEffectiveVolumes()
	}
	pitFilter, err := getPITFilter(r)
	if err != nil {
		sharedapi.BadRequest(w, shared.ErrValidation, err)
		return
	}
	query.PITFilter = *pitFilter

	acc, err := l.GetAccountWithVolumes(r.Context(), query)
	if err != nil {
		shared.ResponseError(w, r, err)
		return
	}

	sharedapi.Ok(w, acc)
}

func postAccountMetadata(w http.ResponseWriter, r *http.Request) {
	l := shared.LedgerFromContext(r.Context())

	if !ledger.ValidateAddress(chi.URLParam(r, "address")) {
		shared.ResponseError(w, r, errorsutil.NewError(command.ErrValidation,
			errors.New("invalid account address format")))
		return
	}

	var m metadata.Metadata
	if err := json.NewDecoder(r.Body).Decode(&m); err != nil {
		shared.ResponseError(w, r, errorsutil.NewError(command.ErrValidation,
			errors.New("invalid metadata format")))
		return
	}

	err := l.SaveMeta(r.Context(), getCommandParameters(r), ledger.MetaTargetTypeAccount, chi.URLParam(r, "address"), m)
	if err != nil {
		shared.ResponseError(w, r, err)
		return
	}

	sharedapi.NoContent(w)
}

func deleteAccountMetadata(w http.ResponseWriter, r *http.Request) {
	if err := shared.LedgerFromContext(r.Context()).
		DeleteMetadata(
			r.Context(),
			getCommandParameters(r),
			ledger.MetaTargetTypeAccount,
			chi.URLParam(r, "address"),
			chi.URLParam(r, "key"),
		); err != nil {
		shared.ResponseError(w, r, err)
		return
	}

	sharedapi.NoContent(w)
}
