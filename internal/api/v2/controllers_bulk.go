package v2

import (
	"encoding/json"
	"github.com/formancehq/ledger/internal/api/bulk"
	"github.com/formancehq/ledger/internal/api/shared"
	sharedapi "github.com/formancehq/stack/libs/go-libs/api"
	"net/http"
)

func bulkHandler(w http.ResponseWriter, r *http.Request) {
	b := bulk.Bulk{}
	if err := json.NewDecoder(r.Body).Decode(&b); err != nil {
		shared.ResponseError(w, r, err)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	ret, errorsInBulk, err := bulk.ProcessBulk(r.Context(), shared.LedgerFromContext(r.Context()), b, sharedapi.QueryParamBool(r, "continueOnFailure"))
	if err != nil || errorsInBulk {
		w.WriteHeader(http.StatusBadRequest)
	}

	if err := json.NewEncoder(w).Encode(sharedapi.BaseResponse[[]bulk.Result]{
		Data: &ret,
	}); err != nil {
		panic(err)
	}
}
