package v2

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/formancehq/stack/libs/core/accounts"
	"github.com/formancehq/stack/libs/go-libs/pointer"

	"github.com/formancehq/stack/libs/go-libs/bun/bunpaginate"
	"github.com/go-chi/chi/v5"

	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/api/backend"
	"github.com/formancehq/ledger/internal/storage/ledgerstore"
	sharedapi "github.com/formancehq/stack/libs/go-libs/api"
	"github.com/formancehq/stack/libs/go-libs/collectionutils"
	"github.com/formancehq/stack/libs/go-libs/metadata"
	"github.com/pkg/errors"
)

func countAccounts(w http.ResponseWriter, r *http.Request) {
	l := backend.LedgerFromContext(r.Context())

	options, err := getPaginatedQueryOptionsOfPITFilterWithVolumes(r)
	if err != nil {
		sharedapi.BadRequest(w, ErrValidation, err)
		return
	}

	count, err := l.CountAccounts(r.Context(), ledgerstore.NewGetAccountsQuery(*options))
	if err != nil {
		switch {
		case ledgerstore.IsErrInvalidQuery(err):
			sharedapi.BadRequest(w, ErrValidation, err)
		default:
			sharedapi.InternalServerError(w, r, err)
		}
		return
	}

	w.Header().Set("Count", fmt.Sprint(count))
	sharedapi.NoContent(w)
}

func getAccounts(w http.ResponseWriter, r *http.Request) {
	l := backend.LedgerFromContext(r.Context())

	query, err := bunpaginate.Extract[ledgerstore.GetAccountsQuery](r, func() (*ledgerstore.GetAccountsQuery, error) {
		options, err := getPaginatedQueryOptionsOfPITFilterWithVolumes(r)
		if err != nil {
			return nil, err
		}
		return pointer.For(ledgerstore.NewGetAccountsQuery(*options)), nil
	})
	if err != nil {
		sharedapi.BadRequest(w, ErrValidation, err)
		return
	}

	cursor, err := l.GetAccountsWithVolumes(r.Context(), *query)
	if err != nil {
		switch {
		case ledgerstore.IsErrInvalidQuery(err):
			sharedapi.BadRequest(w, ErrValidation, err)
		default:
			sharedapi.InternalServerError(w, r, err)
		}
		return
	}

	sharedapi.RenderCursor(w, *cursor)
}

func getAccount(w http.ResponseWriter, r *http.Request) {
	l := backend.LedgerFromContext(r.Context())

	query := ledgerstore.NewGetAccountQuery(chi.URLParam(r, "address"))
	if collectionutils.Contains(r.URL.Query()["expand"], "volumes") {
		query = query.WithExpandVolumes()
	}
	if collectionutils.Contains(r.URL.Query()["expand"], "effectiveVolumes") {
		query = query.WithExpandEffectiveVolumes()
	}
	pitFilter, err := getPITFilter(r)
	if err != nil {
		sharedapi.BadRequest(w, ErrValidation, err)
		return
	}
	query.PITFilter = *pitFilter

	acc, err := l.GetAccountWithVolumes(r.Context(), query)
	if err != nil {
		sharedapi.InternalServerError(w, r, err)
		return
	}

	sharedapi.Ok(w, acc)
}

func postAccountMetadata(w http.ResponseWriter, r *http.Request) {
	l := backend.LedgerFromContext(r.Context())

	if !accounts.ValidateAddress(chi.URLParam(r, "address")) {
		sharedapi.BadRequest(w, ErrValidation, errors.New("invalid account address format"))
		return
	}

	var m metadata.Metadata
	if err := json.NewDecoder(r.Body).Decode(&m); err != nil {
		sharedapi.BadRequest(w, ErrValidation, errors.New("invalid metadata format"))
		return
	}

	err := l.SaveMeta(r.Context(), getCommandParameters(r), ledger.MetaTargetTypeAccount, chi.URLParam(r, "address"), m)
	if err != nil {
		sharedapi.InternalServerError(w, r, err)
		return
	}

	sharedapi.NoContent(w)
}

func deleteAccountMetadata(w http.ResponseWriter, r *http.Request) {
	if err := backend.LedgerFromContext(r.Context()).
		DeleteMetadata(
			r.Context(),
			getCommandParameters(r),
			ledger.MetaTargetTypeAccount,
			chi.URLParam(r, "address"),
			chi.URLParam(r, "key"),
		); err != nil {
		sharedapi.InternalServerError(w, r, err)
		return
	}

	sharedapi.NoContent(w)
}
