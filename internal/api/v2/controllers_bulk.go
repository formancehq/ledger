package v2

import (
	"context"
	"encoding/json"
	"net/http"

	"errors"

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

	ret, errorsInBulk, err := ProcessBulk(r.Context(), backend.LedgerFromContext(r.Context()), b, sharedapi.QueryParamBool(r, "continueOnFailure"))
	if errors.Is(err, context.DeadlineExceeded) {
		return
	}
	if err != nil || errorsInBulk {
		w.WriteHeader(http.StatusBadRequest)
	}

	if err := json.NewEncoder(w).Encode(sharedapi.BaseResponse[[]Result]{
		Data: &ret,
	}); err != nil {
		panic(err)
	}
}
