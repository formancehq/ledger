package v1

import (
	"encoding/json"
	"github.com/formancehq/ledger/pkg/accounts"
	"net/http"
	"net/url"

	"github.com/formancehq/go-libs/api"
	"github.com/formancehq/go-libs/metadata"
	"github.com/formancehq/ledger/internal/api/common"
	"github.com/go-chi/chi/v5"
	"github.com/pkg/errors"
)

func addAccountMetadata(w http.ResponseWriter, r *http.Request) {
	l := common.LedgerFromContext(r.Context())
	param, err := url.PathUnescape(chi.URLParam(r, "address"))
	if err != nil {
		api.BadRequestWithDetails(w, ErrValidation, err, err.Error())
		return
	}

	if !accounts.ValidateAddress(param) {
		api.BadRequest(w, ErrValidation, errors.New("invalid account address format"))
		return
	}

	var m metadata.Metadata
	if err := json.NewDecoder(r.Body).Decode(&m); err != nil {
		api.BadRequest(w, ErrValidation, errors.New("invalid metadata format"))
		return
	}

	err = l.SaveAccountMetadata(r.Context(), getCommandParameters(r), param, m)
	if err != nil {
		api.InternalServerError(w, r, err)
		return
	}

	api.NoContent(w)
}
