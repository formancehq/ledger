package v2

import (
	"net/http"
	"strconv"

	"errors"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"

	"github.com/formancehq/go-libs/v2/api"
	"github.com/formancehq/ledger/internal/api/common"
)

func readVolumes(w http.ResponseWriter, r *http.Request) {

	l := common.LedgerFromContext(r.Context())

	rq, err := getOffsetPaginatedQuery[ledgercontroller.GetVolumesOptions](r, func(opts *ledgercontroller.GetVolumesOptions) error {
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
		case errors.Is(err, ledgercontroller.ErrInvalidQuery{}) || errors.Is(err, ledgercontroller.ErrMissingFeature{}):
			api.BadRequest(w, common.ErrValidation, err)
		default:
			common.HandleCommonErrors(w, r, err)
		}
		return
	}

	api.RenderCursor(w, *cursor)

}
