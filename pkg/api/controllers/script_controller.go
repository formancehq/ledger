package controllers

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/numary/ledger/pkg/core"
	"github.com/numary/ledger/pkg/ledger"
)

// ScriptController -
type ScriptController struct {
	BaseController
}

// NewScriptController -
func NewScriptController() ScriptController {
	return ScriptController{}
}

// PostScript godoc
// @Summary Execute Numscript
// @Description Execute a Numscript and create the transaction if any
// @Tags script
// @Schemes
// @Param ledger path string true "ledger"
// @Param script body core.Script true "script"
// @Accept json
// @Produce json
// @Success 200 {object} controllers.BaseResponse
// @Router /{ledger}/script [post]
func (ctl *ScriptController) PostScript(c *gin.Context) {
	l, _ := c.Get("ledger")

	var script core.Script
	c.ShouldBind(&script)

	txs, err := l.(*ledger.Ledger).Execute(c, script)

	if err != nil {
		errStr := err.Error()
		errStr = strings.ReplaceAll(errStr, "\n", "\r\n")
		payload, err := json.Marshal(gin.H{
			"error": errStr,
		})
		if err != nil {
			panic(err)
		}
		payloadB64 := base64.StdEncoding.EncodeToString(payload)
		link := fmt.Sprintf("https://play.numscript.org/?payload=%v", payloadB64)

		c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{
			"ok":      false,
			"err":     errStr,
			"details": link,
		})
	} else {
		ctl.response(c, http.StatusOK, txs)
	}
}
