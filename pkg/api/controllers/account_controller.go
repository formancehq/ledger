package controllers

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/numary/ledger/pkg/core"
	"github.com/numary/ledger/pkg/ledger"
	"github.com/numary/ledger/pkg/ledger/query"
)

// AccountController -
type AccountController struct {
	BaseController
}

// NewAccountController -
func NewAccountController() AccountController {
	return AccountController{}
}

func (ctl *AccountController) CountAccounts(c *gin.Context) {
	l, _ := c.Get("ledger")

	count, err := l.(*ledger.Ledger).CountAccounts(
		c.Request.Context(),
		query.After(c.Query("after")),
		query.Address(c.Query("address")),
		func(q *query.Query) {
			q.Params["metadata"] = c.QueryMap("metadata")
		},
	)
	if err != nil {
		ResponseError(c, err)
		return
	}
	c.Header("Count", fmt.Sprint(count))
}

func (ctl *AccountController) GetAccounts(c *gin.Context) {
	l, _ := c.Get("ledger")

	cursor, err := l.(*ledger.Ledger).FindAccounts(
		c.Request.Context(),
		query.After(c.Query("after")),
		query.Address(c.Query("address")),
		func(q *query.Query) {
			q.Params["metadata"] = c.QueryMap("metadata")
		},
	)
	if err != nil {
		ResponseError(c, err)
		return
	}
	ctl.response(
		c,
		http.StatusOK,
		cursor,
	)
}

func (ctl *AccountController) GetAccount(c *gin.Context) {
	l, _ := c.Get("ledger")
	acc, err := l.(*ledger.Ledger).GetAccount(c.Request.Context(), c.Param("address"))
	if err != nil {
		ResponseError(c, err)
		return
	}
	ctl.response(
		c,
		http.StatusOK,
		acc,
	)
}

func (ctl *AccountController) PostAccountMetadata(c *gin.Context) {
	l, _ := c.Get("ledger")
	var m core.Metadata
	if err := c.ShouldBind(&m); err != nil {
		panic(err)
	}

	addr := c.Param("address")
	if !core.ValidateAddress(addr) {
		ResponseError(c, errors.New("invalid address"))
		return
	}

	err := l.(*ledger.Ledger).SaveMeta(c.Request.Context(), core.MetaTargetTypeAccount, addr, m)
	if err != nil {
		ResponseError(c, err)
		return
	}
	ctl.noContent(c)
}
