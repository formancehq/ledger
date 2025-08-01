package v2

import (
	"net/http"
	"net/url"

	"github.com/formancehq/ledger/internal/controller/ledger"

	"github.com/formancehq/go-libs/v3/api"
	"github.com/formancehq/go-libs/v3/metadata"
	"github.com/formancehq/ledger/internal/api/common"
	"github.com/go-chi/chi/v5"
)

func addAccountMetadata(w http.ResponseWriter, r *http.Request) {
	l := common.LedgerFromContext(r.Context())

	address, err := url.PathUnescape(chi.URLParam(r, "address"))
	if err != nil {
		api.BadRequestWithDetails(w, common.ErrValidation, err, err.Error())
		return
	}

	common.WithBody(w, r, func(m metadata.Metadata) {
		_, err = l.SaveAccountMetadata(r.Context(), getCommandParameters(r, ledger.SaveAccountMetadata{
			Address:  address,
			Metadata: m,
		}))
		if err != nil {
			common.HandleCommonWriteErrors(w, r, err)
			return
		}

		api.NoContent(w)
	})
}
