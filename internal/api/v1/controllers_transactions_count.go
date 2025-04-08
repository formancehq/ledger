package v1

import (
	"fmt"
	"net/http"

	"github.com/formancehq/go-libs/v3/api"
	"github.com/formancehq/ledger/internal/api/common"
)

func countTransactions(w http.ResponseWriter, r *http.Request) {

	rq, err := getResourceQuery[any](r)
	if err != nil {
		return
	}
	rq.Builder = buildGetTransactionsQuery(r)

	count, err := common.LedgerFromContext(r.Context()).CountTransactions(r.Context(), *rq)
	if err != nil {
		common.HandleCommonErrors(w, r, err)
		return
	}

	w.Header().Set("Count", fmt.Sprint(count))
	api.NoContent(w)
}
