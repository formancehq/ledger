package v2

import (
	"encoding/json"
	"net/http"

	"github.com/formancehq/stack/libs/go-libs/metadata"
	"github.com/go-chi/chi/v5"
	"github.com/pkg/errors"

	"github.com/formancehq/stack/libs/go-libs/pointer"

	"github.com/formancehq/stack/libs/go-libs/bun/bunpaginate"

	"github.com/formancehq/ledger/internal/storage/systemstore"
	sharedapi "github.com/formancehq/stack/libs/go-libs/api"

	"github.com/formancehq/ledger/internal/api/backend"
)

func listLedgers(b backend.Backend) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {

		query, err := bunpaginate.Extract[systemstore.ListLedgersQuery](r, func() (*systemstore.ListLedgersQuery, error) {
			pageSize, err := bunpaginate.GetPageSize(r)
			if err != nil {
				return nil, err
			}

			return pointer.For(systemstore.NewListLedgersQuery(pageSize)), nil
		})
		if err != nil {
			sharedapi.BadRequest(w, ErrValidation, err)
			return
		}

		ledgers, err := b.ListLedgers(r.Context(), *query)
		if err != nil {
			sharedapi.InternalServerError(w, r, err)
			return
		}

		sharedapi.RenderCursor(w, *ledgers)
	}
}

func updateLedgerMetadata(b backend.Backend) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {

		m := metadata.Metadata{}
		if err := json.NewDecoder(r.Body).Decode(&m); err != nil {
			sharedapi.BadRequest(w, "VALIDATION", errors.New("invalid format"))
			return
		}

		if err := b.UpdateLedgerMetadata(r.Context(), chi.URLParam(r, "ledger"), m); err != nil {
			sharedapi.InternalServerError(w, r, err)
			return
		}

		sharedapi.NoContent(w)
	}
}

func deleteLedgerMetadata(b backend.Backend) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if err := b.DeleteLedgerMetadata(r.Context(), chi.URLParam(r, "ledger"), chi.URLParam(r, "key")); err != nil {
			sharedapi.InternalServerError(w, r, err)
			return
		}

		sharedapi.NoContent(w)
	}
}
