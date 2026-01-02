package v2

import (
	"fmt"
	"net/http"

	"github.com/go-chi/chi/v5"
	_ "github.com/pkg/errors"

	// ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/go-libs/v3/api"
	"github.com/formancehq/go-libs/v3/bun/bunpaginate"

	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/api/common"
)

type RunQueryBody struct {
	Params map[string]string `json:"params,omitempty"`
}

func runQuery(paginationConfig common.PaginationConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		common.WithBody(w, r, func(payload RunQueryBody) {

			l := common.LedgerFromContext(r.Context())

			schemaVersion := r.URL.Query().Get("schemaVersion")
			queryId := chi.URLParam(r, "id")

			cursor, err := l.RunQuery(r.Context(), schemaVersion, queryId, payload.Params)
			if err != nil {
				common.HandleCommonPaginationErrors(w, r, err)
				return
			}

			fmt.Printf("what? %v\n%v\n", cursor, err)

			api.RenderCursor(w, *bunpaginate.MapCursor(cursor, func(item any) any {
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
			}))
		})
	}
}
