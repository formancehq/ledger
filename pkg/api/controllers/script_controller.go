package controllers

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/numary/go-libs/sharedapi"
	"github.com/numary/go-libs/sharedlogging"
	"github.com/numary/ledger/pkg/core"
	"github.com/numary/ledger/pkg/ledger"
)

type ScriptResponse struct {
	sharedapi.ErrorResponse
	Link        string            `json:"details,omitempty"`
	Transaction *core.Transaction `json:"transaction,omitempty"`
}

func EncodeLink(errStr string) string {
	errStr = strings.ReplaceAll(errStr, "\n", "\r\n")
	payload, err := json.Marshal(gin.H{
		"error": errStr,
	})
	if err != nil {
		panic(err)
	}
	payloadB64 := base64.StdEncoding.EncodeToString(payload)
	return fmt.Sprintf("https://play.numscript.org/?payload=%v", payloadB64)
}

// ScriptController -
type ScriptController struct {
	BaseController
}

// NewScriptController -
func NewScriptController() ScriptController {
	return ScriptController{}
}

func (ctl *ScriptController) PostScript(c *gin.Context) {
	l, _ := c.Get("ledger")

	var script core.Script
	if err := c.ShouldBind(&script); err != nil {
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
			code    = ErrInternal
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
			res.Link = EncodeLink(message)
		}
	}
	if tx != nil {
		res.Transaction = tx
	}

	c.JSON(http.StatusOK, res)
}
