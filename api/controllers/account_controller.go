package controllers

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/numary/ledger/core"
	"github.com/numary/ledger/ledger"
	"github.com/numary/ledger/ledger/query"
)

// AccountController -
type AccountController struct {
	BaseController
}

// NewAccountController -
func NewAccountController() AccountController {
	return AccountController{}
}

// GetAccounts -
func (ctl *AccountController) GetAccounts(c *gin.Context) {
	l, _ := c.Get("ledger")
	cursor, err := l.(*ledger.Ledger).FindAccounts(
		query.After(c.Query("after")),
	)
	if err != nil {
		ctl.responseError(
			c,
			http.StatusInternalServerError,
			err,
		)
		return
	}
	ctl.response(
		c,
		http.StatusOK,
		cursor,
	)
}

// GetAccount -
func (ctl *AccountController) GetAccount(c *gin.Context) {
	l, _ := c.Get("ledger")
	acc, err := l.(*ledger.Ledger).GetAccount(c.Param("address"))
	if err != nil {
		ctl.responseError(
			c,
			http.StatusInternalServerError,
			err,
		)
		return
	}
	ctl.response(
		c,
		http.StatusOK,
		acc,
	)
}

// PostAccountMetadata -
func (ctl *AccountController) PostAccountMetadata(c *gin.Context) {
	l, _ := c.Get("ledger")
	var m core.Metadata
	c.ShouldBind(&m)
	err := l.(*ledger.Ledger).SaveMeta(
		"account",
		c.Param("address"),
		m,
	)
	if err != nil {
		ctl.responseError(
			c,
			http.StatusInternalServerError,
			err,
		)
		return
	}
	ctl.response(
		c,
		http.StatusOK,
		nil,
	)
}
