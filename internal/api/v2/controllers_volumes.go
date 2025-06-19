package v2

import (
	"errors"
	"github.com/formancehq/go-libs/v3/api"
	"github.com/formancehq/go-libs/v3/bun/bunpaginate"
	"github.com/formancehq/go-libs/v3/time"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/api/common"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	storagecommon "github.com/formancehq/ledger/internal/storage/common"
	"net/http"
	"strconv"
)

func readVolumes(paginationConfig common.PaginationConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		l := common.LedgerFromContext(r.Context())

		var groupBy int
		if queryGroupBy := r.URL.Query().Get("groupBy"); queryGroupBy != "" {
			v, err := strconv.ParseInt(queryGroupBy, 10, 64)
			if err != nil {
				api.BadRequest(w, common.ErrValidation, err)
				return
			}
			groupBy = int(v)
		}

		// Kept for compatibility with old version of the ledger
		// the parameters used should bt pit and oot now
		var (
			pit *time.Time
			oot *time.Time
			err error
		)
		if r.URL.Query().Get("endTime") != "" {
			pit, err = getDate(r, "endTime")
			if err != nil {
				api.BadRequest(w, common.ErrValidation, err)
				return
			}
		}

		if r.URL.Query().Get("startTime") != "" {
			oot, err = getDate(r, "startTime")
			if err != nil {
				api.BadRequest(w, common.ErrValidation, err)
				return
			}
		}

		rq, err := getPaginatedQuery[ledgercontroller.GetVolumesOptions](
			r,
			paginationConfig,
			"account",
			bunpaginate.OrderAsc,
			func(rq *storagecommon.ResourceQuery[ledgercontroller.GetVolumesOptions]) {
				if groupBy > 0 {
					rq.Opts.GroupLvl = groupBy
				}
				if pit != nil {
					rq.PIT = pit
				}
				if oot != nil {
					rq.OOT = oot
				}

				rq.Opts.UseInsertionDate = api.QueryParamBool(r, "insertionDate")
			},
		)
		if err != nil {
			api.BadRequest(w, common.ErrValidation, err)
			return
		}

		cursor, err := l.GetVolumesWithBalances(r.Context(), rq)
		if err != nil {
			switch {
			case errors.Is(err, storagecommon.ErrInvalidQuery{}) || errors.Is(err, ledgercontroller.ErrMissingFeature{}):
				api.BadRequest(w, common.ErrValidation, err)
			default:
				common.HandleCommonErrors(w, r, err)
			}
			return
		}

		api.RenderCursor(w, *bunpaginate.MapCursor(cursor, func(volumes ledger.VolumesWithBalanceByAssetByAccount) any {
			return renderVolumesWithBalances(r, volumes)
		}))
	}
}
