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
	ledgerValue, exists := c.Get("ledger")
	if !exists {
		ResponseError(c, &ledger.ValidationError{
			Msg: "missing ledger key"})
		return
	}

	l, ok := ledgerValue.(*ledger.Ledger)
	if !ok {
		ResponseError(c, &ledger.ValidationError{
			Msg: fmt.Sprintf("invalid value type for ledger key: %T", ledgerValue)})
		return
	}

	count, err := l.CountAccounts(
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
	ledgerValue, exists := c.Get("ledger")
	if !exists {
		ResponseError(c, &ledger.ValidationError{
			Msg: "missing ledger key"})
		return
	}

	l, ok := ledgerValue.(*ledger.Ledger)
	if !ok {
		ResponseError(c, &ledger.ValidationError{
			Msg: fmt.Sprintf("invalid value type for ledger key: %T", ledgerValue)})
		return
	}

	cursor, err := l.FindAccounts(
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

	ctl.response(c, http.StatusOK, cursor)
}

func (ctl *AccountController) GetAccount(c *gin.Context) {
	ledgerValue, exists := c.Get("ledger")
	if !exists {
		ResponseError(c, &ledger.ValidationError{
			Msg: "missing ledger key"})
		return
	}

	l, ok := ledgerValue.(*ledger.Ledger)
	if !ok {
		ResponseError(c, &ledger.ValidationError{
			Msg: fmt.Sprintf("invalid value type for ledger key: %T", ledgerValue)})
		return
	}

	acc, err := l.GetAccount(c.Request.Context(), c.Param("address"))
	if err != nil {
		ResponseError(c, err)
		return
	}

	ctl.response(c, http.StatusOK, acc)
}

func (ctl *AccountController) PostAccountMetadata(c *gin.Context) {
	ledgerValue, exists := c.Get("ledger")
	if !exists {
		ResponseError(c, &ledger.ValidationError{
			Msg: "missing ledger key"})
		return
	}

	l, ok := ledgerValue.(*ledger.Ledger)
	if !ok {
		ResponseError(c, &ledger.ValidationError{
			Msg: fmt.Sprintf("invalid value type for ledger key: %T", ledgerValue)})
		return
	}

	var m core.Metadata
	if err := c.Bind(&m); err != nil {
		return
	}

	addr := c.Param("address")
	if !core.ValidateAddress(addr) {
		ResponseError(c, errors.New("invalid address"))
		return
	}

	if err := l.SaveMeta(c.Request.Context(), core.MetaTargetTypeAccount, addr, m); err != nil {
		ResponseError(c, err)
		return
	}

	ctl.noContent(c)
}
