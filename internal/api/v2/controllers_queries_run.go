package v2

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/go-chi/chi/v5"
	_ "github.com/pkg/errors"

	"github.com/formancehq/go-libs/v4/api"
	"github.com/formancehq/go-libs/v4/bun/bunpaginate"

	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/api/common"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	"github.com/formancehq/ledger/internal/queries"
	storage "github.com/formancehq/ledger/internal/storage/common"
)

func runQuery(paginationConfig storage.PaginationConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		common.WithBody(w, r, func(payload storage.RunQuery) {
			l := common.LedgerFromContext(r.Context())

			schemaVersion := r.URL.Query().Get("schemaVersion")
			queryId := chi.URLParam(r, "id")

			resource, cursor, err := l.RunQuery(r.Context(), schemaVersion, queryId, payload, paginationConfig)
			if err != nil {
				switch {
				case errors.Is(err, ledgercontroller.ErrQueryValidation{}):
					api.BadRequest(w, common.ErrValidation, err)
				default:
					common.HandleCommonPaginationErrors(w, r, err)
				}
				return
			}

			err = getJsonResponse(r, w, *resource, *cursor)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
		})
	}
}

func getJsonResponse(r *http.Request, w http.ResponseWriter, resource queries.ResourceKind, cursor bunpaginate.Cursor[any]) error {
	renderedCursor := *bunpaginate.MapCursor(&cursor, func(item any) any {
		switch v := item.(type) {
		case ledger.Transaction:
			return renderTransaction(r, v)
		case ledger.Account:
			return renderAccount(r, v)
		case ledger.VolumesWithBalanceByAssetByAccount:
			return renderVolumesWithBalances(r, v)
		case ledger.Log:
			return renderLog(r, v)
		}
		return item
	})
	{
		v := api.BaseResponse[any]{
			Cursor: &renderedCursor,
		}
		s, err := json.Marshal(v)
		if err != nil {
			return err
		}
		var fields map[string]any
		err = json.Unmarshal(s, &fields)
		if err != nil {
			return err
		}
		fields["resource"] = resource

		w.WriteHeader(http.StatusOK)
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(fields); err != nil {
			panic(err)
		}
	}
	return nil
}
