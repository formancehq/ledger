package v1

import (
	"encoding/json"
	"net/http"
	"net/url"

	"github.com/formancehq/ledger/internal/controller/ledger"
	"github.com/formancehq/ledger/pkg/accounts"

	"errors"

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

	if !accounts.ValidateAddress(address) {
		api.BadRequest(w, common.ErrValidation, errors.New("invalid account address format"))
		return
	}

	var m metadata.Metadata
	if err := json.NewDecoder(r.Body).Decode(&m); err != nil {
		api.BadRequest(w, common.ErrValidation, errors.New("invalid metadata format"))
		return
	}

	_, err = l.SaveAccountMetadata(r.Context(), getCommandParameters(r, ledger.SaveAccountMetadata{
		Address:  address,
		Metadata: m,
	}))
	if err != nil {
		common.HandleCommonWriteErrors(w, r, err)
		return
	}

	api.NoContent(w)
}
