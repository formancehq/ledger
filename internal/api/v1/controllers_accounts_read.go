package v1

import (
	"net/http"
	"net/url"

	"github.com/formancehq/go-libs/api"
	"github.com/formancehq/go-libs/metadata"
	"github.com/formancehq/go-libs/platform/postgres"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/api/common"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	"github.com/go-chi/chi/v5"
)

func getAccount(w http.ResponseWriter, r *http.Request) {
	l := common.LedgerFromContext(r.Context())

	address, err := url.PathUnescape(chi.URLParam(r, "address"))
	if err != nil {
		api.BadRequestWithDetails(w, ErrValidation, err, err.Error())
		return
	}

	query := ledgercontroller.NewGetAccountQuery(address)
	query = query.WithExpandVolumes()

	acc, err := l.GetAccount(r.Context(), query)
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
