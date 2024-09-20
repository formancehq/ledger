package v2

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"

	"github.com/go-chi/chi/v5"

	storageerrors "github.com/formancehq/ledger/internal/storage/sqlutils"

	"github.com/formancehq/go-libs/pointer"
	"github.com/formancehq/stack/libs/core/accounts"

	sharedapi "github.com/formancehq/go-libs/api"
	"github.com/formancehq/go-libs/bun/bunpaginate"
	"github.com/formancehq/go-libs/collectionutils"
	"github.com/formancehq/go-libs/metadata"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/api/backend"
	"github.com/formancehq/ledger/internal/storage/ledgerstore"
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

	param, err := url.PathUnescape(chi.URLParam(r, "address"))
	if err != nil {
		sharedapi.BadRequestWithDetails(w, ErrValidation, err, err.Error())
		return
	}

	query := ledgerstore.NewGetAccountQuery(param)
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
		switch {
		case storageerrors.IsNotFoundError(err):
			sharedapi.NotFound(w, err)
		default:
			sharedapi.InternalServerError(w, r, err)
		}
		return
	}

	sharedapi.Ok(w, acc)
}

func postAccountMetadata(w http.ResponseWriter, r *http.Request) {
	l := backend.LedgerFromContext(r.Context())

	param, err := url.PathUnescape(chi.URLParam(r, "address"))
	if err != nil {
		sharedapi.BadRequestWithDetails(w, ErrValidation, err, err.Error())
		return
	}

	if !accounts.ValidateAddress(param) {
		sharedapi.BadRequest(w, ErrValidation, errors.New("invalid account address format"))
		return
	}

	var m metadata.Metadata
	if err := json.NewDecoder(r.Body).Decode(&m); err != nil {
		sharedapi.BadRequest(w, ErrValidation, errors.New("invalid metadata format"))
		return
	}

	err = l.SaveMeta(r.Context(), getCommandParameters(r), ledger.MetaTargetTypeAccount, chi.URLParam(r, "address"), m)
	if err != nil {
		sharedapi.InternalServerError(w, r, err)
		return
	}

	sharedapi.NoContent(w)
}

func deleteAccountMetadata(w http.ResponseWriter, r *http.Request) {
	param, err := url.PathUnescape(chi.URLParam(r, "address"))
	if err != nil {
		sharedapi.BadRequestWithDetails(w, ErrValidation, err, err.Error())
		return
	}

	if err := backend.LedgerFromContext(r.Context()).
		DeleteMetadata(
			r.Context(),
			getCommandParameters(r),
			ledger.MetaTargetTypeAccount,
			param,
			chi.URLParam(r, "key"),
		); err != nil {
		sharedapi.InternalServerError(w, r, err)
		return
	}

	sharedapi.NoContent(w)
}
