package v2

import (
	"errors"
	"github.com/formancehq/go-libs/v2/api"
	"github.com/formancehq/ledger/internal/api/common"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	storagecommon "github.com/formancehq/ledger/internal/storage/common"
	"net/http"
	"strconv"
)

func readVolumes(paginationConfig common.PaginationConfig) http.HandlerFunc {

	return func(w http.ResponseWriter, r *http.Request) {
		l := common.LedgerFromContext(r.Context())

		rq, err := getOffsetPaginatedQuery[ledgercontroller.GetVolumesOptions](r, paginationConfig, func(opts *ledgercontroller.GetVolumesOptions) error {
			groupBy := r.URL.Query().Get("groupBy")
			if groupBy != "" {
				v, err := strconv.ParseInt(groupBy, 10, 64)
				if err != nil {
					return err
				}
				opts.GroupLvl = int(v)
			}

			opts.UseInsertionDate = api.QueryParamBool(r, "insertionDate")

			return nil
		})
		if err != nil {
			api.BadRequest(w, common.ErrValidation, err)
			return
		}

		if r.URL.Query().Get("endTime") != "" {
			rq.Options.PIT, err = getDate(r, "endTime")
			if err != nil {
				api.BadRequest(w, common.ErrValidation, err)
				return
			}
		}

		if r.URL.Query().Get("startTime") != "" {
			rq.Options.OOT, err = getDate(r, "startTime")
			if err != nil {
				api.BadRequest(w, common.ErrValidation, err)
				return
			}
		}

		cursor, err := l.GetVolumesWithBalances(r.Context(), *rq)
		if err != nil {
			switch {
			case errors.Is(err, storagecommon.ErrInvalidQuery{}) || errors.Is(err, ledgercontroller.ErrMissingFeature{}):
				api.BadRequest(w, common.ErrValidation, err)
			default:
				common.HandleCommonErrors(w, r, err)
			}
			return
		}

		api.RenderCursor(w, *cursor)
	}
}
