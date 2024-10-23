package v2

import (
	"encoding/json"
	"net/http"

	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"

	"errors"

	"github.com/formancehq/go-libs/v2/api"

	"github.com/formancehq/ledger/internal/api/common"
)

func createTransaction(w http.ResponseWriter, r *http.Request) {
	l := common.LedgerFromContext(r.Context())

	payload := TransactionRequest{}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		api.BadRequest(w, ErrValidation, errors.New("invalid transaction format"))
		return
	}

	if len(payload.Postings) > 0 && payload.Script.Plain != "" {
		api.BadRequest(w, ErrValidation, errors.New("cannot pass postings and numscript in the same request"))
		return
	}

	if len(payload.Postings) == 0 && payload.Script.Plain == "" {
		api.BadRequest(w, ErrNoPostings, errors.New("you need to pass either a posting array or a numscript script"))
		return
	}
	runScript, err := payload.ToRunScript(api.QueryParamBool(r, "force"))
	if err != nil {
		api.BadRequest(w, ErrValidation, err)
		return
	}

	res, err := l.CreateTransaction(r.Context(), getCommandParameters(r, *runScript))
	if err != nil {
		switch {
		case errors.Is(err, &ledgercontroller.ErrInsufficientFunds{}):
			api.BadRequest(w, ErrInsufficientFund, err)
		case errors.Is(err, &ledgercontroller.ErrInvalidVars{}) || errors.Is(err, ledgercontroller.ErrCompilationFailed{}):
			api.BadRequest(w, ErrCompilationFailed, err)
		case errors.Is(err, &ledgercontroller.ErrMetadataOverride{}):
			api.BadRequest(w, ErrMetadataOverride, err)
		case errors.Is(err, ledgercontroller.ErrNoPostings):
			api.BadRequest(w, ErrNoPostings, err)
		case errors.Is(err, ledgercontroller.ErrTransactionReferenceConflict{}):
			api.WriteErrorResponse(w, http.StatusConflict, ErrConflict, err)
		case errors.Is(err, ledgercontroller.ErrInvalidIdempotencyInput{}):
			api.BadRequest(w, ErrValidation, err)
		case errors.Is(err, ledgercontroller.ErrParsing{}):
			api.BadRequest(w, ErrInterpreterParse, err)
		case errors.Is(err, ledgercontroller.ErrRuntime{}):
			api.BadRequest(w, ErrInterpreterRuntime, err)
		default:
			common.HandleCommonErrors(w, r, err)
		}
		return
	}

	api.Ok(w, res.Transaction)
}
