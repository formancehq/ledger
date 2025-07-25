package v2

import (
	"github.com/formancehq/ledger/internal/api/bulking"
	ledgerstore "github.com/formancehq/ledger/internal/storage/ledger"
	"net/http"

	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"

	"errors"

	"github.com/formancehq/go-libs/v3/api"

	"github.com/formancehq/ledger/internal/api/common"
)

func createTransaction(w http.ResponseWriter, r *http.Request) {
	common.WithBody(w, r, func(payload bulking.TransactionRequest) {
		l := common.LedgerFromContext(r.Context())

		if len(payload.Postings) > 0 && payload.Script.Plain != "" {
			api.BadRequest(w, common.ErrValidation, errors.New("cannot pass postings and numscript in the same request"))
			return
		}

		if len(payload.Postings) == 0 && payload.Script.Plain == "" {
			api.BadRequest(w, common.ErrNoPostings, errors.New("you need to pass either a posting array or a numscript script"))
			return
		}
		// nodes(gfyrag): parameter 'force' initially sent using a query param
		// while we still support the feature, we can also send the 'force' parameter
		// in the request payload.
		// it allows to leverage the feature on bulk endpoint
		payload.Force = payload.Force || api.QueryParamBool(r, "force")

		createTransaction, err := payload.ToCore()
		if err != nil {
			api.BadRequest(w, common.ErrValidation, err)
			return
		}

		_, res, err := l.CreateTransaction(r.Context(), getCommandParameters(r, *createTransaction))
		if err != nil {
			switch {
			case errors.Is(err, &ledgercontroller.ErrInsufficientFunds{}):
				api.BadRequest(w, common.ErrInsufficientFund, err)
			case errors.Is(err, &ledgercontroller.ErrInvalidVars{}) || errors.Is(err, ledgercontroller.ErrCompilationFailed{}):
				api.BadRequest(w, common.ErrCompilationFailed, err)
			case errors.Is(err, &ledgercontroller.ErrMetadataOverride{}):
				api.BadRequest(w, common.ErrMetadataOverride, err)
			case errors.Is(err, ledgercontroller.ErrNoPostings):
				api.BadRequest(w, common.ErrNoPostings, err)
			case errors.Is(err, ledgerstore.ErrTransactionReferenceConflict{}):
				api.WriteErrorResponse(w, http.StatusConflict, common.ErrConflict, err)
			case errors.Is(err, ledgercontroller.ErrParsing{}):
				api.BadRequest(w, common.ErrInterpreterParse, err)
			case errors.Is(err, ledgercontroller.ErrRuntime{}):
				api.BadRequest(w, common.ErrInterpreterRuntime, err)
			default:
				common.HandleCommonWriteErrors(w, r, err)
			}
			return
		}

		api.Ok(w, renderTransaction(r, res.Transaction))
	})
}
