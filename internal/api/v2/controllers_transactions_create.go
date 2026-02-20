package v2

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/formancehq/go-libs/v4/api"
	"github.com/formancehq/numscript"

	"github.com/formancehq/ledger/internal/api/bulking"
	"github.com/formancehq/ledger/internal/api/common"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	ledgerstore "github.com/formancehq/ledger/internal/storage/ledger"
)

func createTransaction(w http.ResponseWriter, r *http.Request) {
	common.WithBody(w, r, func(payload bulking.TransactionRequest) {
		l := common.LedgerFromContext(r.Context())

		txType := []string{}
		if len(payload.Postings) > 0 {
			txType = append(txType, "postings")
		}
		if payload.Script.Plain != "" {
			txType = append(txType, "numscript")
		}
		if payload.Script.Template != "" {
			txType = append(txType, "template")
		}
		if len(txType) > 1 {
			api.BadRequest(w, common.ErrValidation, fmt.Errorf("cannot pass %v and %v in the same request", txType[0], txType[1]))
			return
		} else if len(txType) == 0 {
			api.BadRequest(w, common.ErrNoPostings, errors.New("you must pass either a posting array, a numscript script, or a template"))
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

		_, res, idempotencyHit, err := l.CreateTransaction(r.Context(), getCommandParameters(r, *createTransaction))
		if err != nil {
			switch {
			case errors.Is(err, &ledgercontroller.ErrInsufficientFunds{}), errors.Is(err, numscript.MissingFundsErr{}):
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
		if idempotencyHit {
			w.Header().Set("Idempotency-Hit", "true")
		}

		api.Ok(w, renderTransaction(r, res.Transaction))
	})
}
