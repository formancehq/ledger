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

type AccountController struct {
	BaseController
}

func NewAccountController() AccountController {
	return AccountController{}
}

func (ctl *AccountController) CountAccounts(c *gin.Context) {
	l, _ := c.Get("ledger")

	count, err := l.(*ledger.Ledger).CountAccounts(
		c.Request.Context(),
		query.SetAddressRegexpFilter(c.Query("address")),
		query.SetMetadataFilter(c.QueryMap("metadata")),
	)
	if err != nil {
		ResponseError(c, err)
		return
	}

	c.Header("Count", fmt.Sprint(count))
}

func (ctl *AccountController) GetAccounts(c *gin.Context) {
	l, _ := c.Get("ledger")
	afterAddress := c.Query("after")
	addressRegexpFilter := c.Query("address")
	metadataFilter := c.QueryMap("metadata")

	cursor, err := l.(*ledger.Ledger).GetAccounts(
		c.Request.Context(),
		query.SetAfterAddress(afterAddress),
		query.SetAddressRegexpFilter(addressRegexpFilter),
		query.SetMetadataFilter(metadataFilter),
	)
	if err != nil {
		ResponseError(c, err)
		return
	}

	ctl.response(c, http.StatusOK, cursor)
}

func (ctl *AccountController) GetAccount(c *gin.Context) {
	l, _ := c.Get("ledger")

	acc, err := l.(*ledger.Ledger).GetAccount(
		c.Request.Context(),
		c.Param("address"))
	if err != nil {
		ResponseError(c, err)
		return
	}

	ctl.response(c, http.StatusOK, acc)
}

func (ctl *AccountController) PostAccountMetadata(c *gin.Context) {
	l, _ := c.Get("ledger")

	var m core.Metadata
	if err := c.ShouldBindJSON(&m); err != nil {
		ResponseError(c, err)
		return
	}

	addr := c.Param("address")
	if !core.ValidateAddress(addr) {
		ResponseError(c, errors.New("invalid address"))
		return
	}

	if err := l.(*ledger.Ledger).SaveMeta(c.Request.Context(), core.MetaTargetTypeAccount, addr, m); err != nil {
		ResponseError(c, err)
		return
	}

	ctl.noContent(c)
}
