package v2

import (
	"encoding/json"
	"net/http"

	"github.com/formancehq/go-libs/contextutil"

	sharedapi "github.com/formancehq/go-libs/api"
	"github.com/formancehq/ledger/internal/api/backend"
)

func bulkHandler(w http.ResponseWriter, r *http.Request) {
	b := Bulk{}
	if err := json.NewDecoder(r.Body).Decode(&b); err != nil {
		sharedapi.BadRequest(w, ErrValidation, err)
		return
	}

	w.Header().Set("Content-Type", "application/json")

	ctx, _ := contextutil.Detached(r.Context())
	ret, errorsInBulk, err := ProcessBulk(ctx, backend.LedgerFromContext(r.Context()), b, sharedapi.QueryParamBool(r, "continueOnFailure"))
	if err != nil || errorsInBulk {
		w.WriteHeader(http.StatusBadRequest)
	}

	if err := json.NewEncoder(w).Encode(sharedapi.BaseResponse[[]Result]{
		Data: &ret,
	}); err != nil {
		panic(err)
	}
}
