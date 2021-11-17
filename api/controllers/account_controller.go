package controllers

import (
	"github.com/gin-gonic/gin"
	"github.com/numary/ledger/core"
	"github.com/numary/ledger/ledger"
	"github.com/numary/ledger/ledger/query"
)

// AccountController -
type AccountController struct {
	Controllers
}

// NewAccountController -
func NewAccountController() *AccountController {
	return &AccountController{}
}

// CreateAccountController -
func CreateAccountController() *AccountController {
	return NewAccountController()
}

// GetAccounts -
func (ctl *AccountController) GetAccounts(c *gin.Context) {
	l, _ := c.Get("ledger")

	cursor, err := l.(*ledger.Ledger).FindAccounts(
		query.After(c.Query("after")),
	)

	res := gin.H{
		"ok":     err == nil,
		"cursor": cursor,
	}

	if err != nil {
		res["err"] = err.Error()
	}

	c.JSON(200, res)
}

// GetAddress -
func (ctl *AccountController) GetAddress(c *gin.Context) {
	l, _ := c.Get("ledger")

	acc, err := l.(*ledger.Ledger).GetAccount(c.Param("address"))

	res := gin.H{
		"ok":      err == nil,
		"account": acc,
	}

	if err != nil {
		res["err"] = err.Error()
	}

	c.JSON(200, res)
}

// GetAccountMetadata -
func (ctl *AccountController) GetAccountMetadata(c *gin.Context) {
	l, _ := c.Get("ledger")

	var m core.Metadata
	c.ShouldBind(&m)

	err := l.(*ledger.Ledger).SaveMeta(
		"account",
		c.Param("accountId"),
		m,
	)

	res := gin.H{
		"ok": err == nil,
	}

	if err != nil {
		res["err"] = err.Error()
	}

	c.JSON(200, res)
}
