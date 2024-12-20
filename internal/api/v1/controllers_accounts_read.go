package v1

import (
	"github.com/formancehq/go-libs/v2/query"
	"net/http"
	"net/url"

	"github.com/formancehq/go-libs/v2/api"
	"github.com/formancehq/go-libs/v2/metadata"
	"github.com/formancehq/go-libs/v2/platform/postgres"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/api/common"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	"github.com/go-chi/chi/v5"
)

func getAccount(w http.ResponseWriter, r *http.Request) {
	l := common.LedgerFromContext(r.Context())

	address, err := url.PathUnescape(chi.URLParam(r, "address"))
	if err != nil {
		api.BadRequestWithDetails(w, common.ErrValidation, err, err.Error())
		return
	}

	acc, err := l.GetAccount(r.Context(), ledgercontroller.ResourceQuery[any]{
		Builder: query.Match("address", address),
		Expand: []string{"volumes"},
	})
	if err != nil {
		switch {
		case postgres.IsNotFoundError(err):
			acc = &ledger.Account{
				Address:          address,
				Metadata:         metadata.Metadata{},
				Volumes:          ledger.VolumesByAssets{},
				EffectiveVolumes: ledger.VolumesByAssets{},
			}
		default:
			common.HandleCommonErrors(w, r, err)
			return
		}
	}

	api.Ok(w, accountWithVolumesAndBalances(*acc))
}
