package controllers

import (
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/numary/go-libs/sharedapi"
	"github.com/numary/go-libs/sharedlogging"
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

	var script core.Script
	if err := c.ShouldBindJSON(&script); err != nil {
		panic(err)
	}

	value, ok := c.GetQuery("preview")
	preview := ok && (strings.ToUpper(value) == "YES" || strings.ToUpper(value) == "TRUE" || value == "1")

	fn := l.(*ledger.Ledger).Execute
	if preview {
		fn = l.(*ledger.Ledger).ExecutePreview
	}

	res := ScriptResponse{}
	tx, err := fn(c.Request.Context(), script)
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
			sharedlogging.GetLogger(c.Request.Context()).Errorf("internal errors executing script: %s", err)
		}
		res.ErrorResponse = sharedapi.ErrorResponse{
			ErrorCode:    code,
			ErrorMessage: message,
		}
		if message != "" {
			res.Details = apierrors.EncodeLink(message)
		}
	}
	if tx != nil {
		res.Transaction = tx
	}

	c.JSON(http.StatusOK, res)
}
