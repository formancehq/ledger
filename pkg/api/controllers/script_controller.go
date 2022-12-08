package controllers

import (
	"net/http"
	"strings"

	"github.com/formancehq/go-libs/sharedapi"
	"github.com/formancehq/go-libs/sharedlogging"
	"github.com/gin-gonic/gin"
	"github.com/numary/ledger/pkg"
	"github.com/numary/ledger/pkg/api/apierrors"
	"github.com/numary/ledger/pkg/core"
	"github.com/numary/ledger/pkg/ledger"
)

type ScriptResponse struct {
	sharedapi.ErrorResponse
	Details     string                    `json:"details,omitempty"`
	Transaction *core.ExpandedTransaction `json:"transaction,omitempty"`
}

type ScriptController struct{}

func NewScriptController() ScriptController {
	return ScriptController{}
}

func (ctl *ScriptController) PostScript(c *gin.Context) {
	l, _ := c.Get("ledger")

	var script core.ScriptData
	if err := c.ShouldBindJSON(&script); err != nil {
		panic(err)
	}

	value, ok := c.GetQuery("preview")
	preview := ok && (strings.ToUpper(value) == "YES" || strings.ToUpper(value) == "TRUE" || value == "1")

	res := ScriptResponse{}
	txs, err := l.(*ledger.Ledger).Execute(c.Request.Context(), preview, script)
	if err != nil {
		var (
			code    = apierrors.ErrInternal
			message string
		)
		scriptError, ok := err.(*ledger.ScriptError)
		if ok {
			code = scriptError.Code
			message = scriptError.Message
		} else {
			sharedlogging.GetLogger(c.Request.Context()).Errorf(
				"internal error executing script (id: %s): %s",
				c.GetString(string(pkg.ContextKeyID)), err)
		}
		res.ErrorResponse = sharedapi.ErrorResponse{
			ErrorCode:    code,
			ErrorMessage: message,
		}
		if message != "" {
			res.Details = apierrors.EncodeLink(message)
		}
	}
	if len(txs) > 0 {
		res.Transaction = &txs[0]
	}

	c.JSON(http.StatusOK, res)
}
