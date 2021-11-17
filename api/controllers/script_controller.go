package controllers

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/numary/ledger/core"
	"github.com/numary/ledger/ledger"
)

// ScriptController -
type ScriptController struct {
	Controllers
}

// NewScriptController -
func NewScriptController() *ScriptController {
	return &ScriptController{}
}

// CreateScriptController -
func CreateScriptController() *ScriptController {
	return NewScriptController()
}

// PostScript -
func (ctl *ScriptController) PostScript(c *gin.Context) {
	l, _ := c.Get("ledger")

	var script core.Script
	c.ShouldBind(&script)

	err := l.(*ledger.Ledger).Execute(script)

	res := gin.H{
		"ok": err == nil,
	}

	if err != nil {
		err_str := err.Error()
		err_str = strings.ReplaceAll(err_str, "\n", "\r\n")
		payload, err := json.Marshal(gin.H{
			"error": err_str,
		})
		if err != nil {
			log.Fatal(err)
		}
		payload_b64 := base64.StdEncoding.EncodeToString([]byte(payload))
		link := fmt.Sprintf("https://play.numscript.org/?payload=%v", payload_b64)
		res["err"] = err_str
		res["details"] = link
	}

	c.JSON(200, res)
}
