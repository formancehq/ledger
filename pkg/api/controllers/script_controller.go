package controllers

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/formancehq/ledger/pkg/api/apierrors"
	"github.com/formancehq/ledger/pkg/core"
	"github.com/formancehq/ledger/pkg/ledger"
	sharedapi "github.com/formancehq/stack/libs/go-libs/api"
	"github.com/formancehq/stack/libs/go-libs/logging"
)

type ScriptResponse struct {
	sharedapi.ErrorResponse
	Transaction *core.ExpandedTransaction `json:"transaction,omitempty"`
}

type ScriptController struct{}

func NewScriptController() ScriptController {
	return ScriptController{}
}

func (ctl *ScriptController) PostScript(w http.ResponseWriter, r *http.Request) {
	l := LedgerFromContext(r.Context())

	var script core.ScriptData
	if err := json.NewDecoder(r.Body).Decode(&script); err != nil {
		panic(err)
	}

	value := r.URL.Query().Get("preview")
	preview := strings.ToUpper(value) == "YES" || strings.ToUpper(value) == "TRUE" || value == "1"

	res := ScriptResponse{}
	execRes, err := l.ExecuteScript(r.Context(), preview, script)
	if err != nil {
		var (
			code    = apierrors.ErrInternal
			message string
		)
		switch e := err.(type) {
		case *ledger.ScriptError:
			code = e.Code
			message = e.Message
		case *ledger.ConflictError:
			code = apierrors.ErrConflict
			message = e.Error()
		default:
			logging.FromContext(r.Context()).Errorf(
				"internal errors executing script: %s", err)
		}
		res.ErrorResponse = sharedapi.ErrorResponse{
			ErrorCode:    code,
			ErrorMessage: message,
		}
		if message != "" {
			res.Details = apierrors.EncodeLink(message)
		}
	}
	res.Transaction = &execRes

	sharedapi.RawOk(w, res)
}
