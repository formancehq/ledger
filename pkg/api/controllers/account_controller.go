package controllers

import (
	"errors"
	"github.com/numary/ledger/pkg/core"
	"github.com/numary/ledger/pkg/ledger"
	"github.com/numary/ledger/pkg/ledger/query"
	"net/http"

	"github.com/gin-gonic/gin"
)

// AccountController -
type AccountController struct {
	BaseController
}

// NewAccountController -
func NewAccountController() AccountController {
	return AccountController{}
}

func (ctl *AccountController) GetAccounts(c *gin.Context) {
	l, _ := c.Get("ledger")
	cursor, err := l.(*ledger.Ledger).FindAccounts(
		c.Request.Context(),
		query.After(c.Query("after")),
	)
	if err != nil {
		ctl.responseError(c, http.StatusInternalServerError, ErrInternal, err)
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
		ctl.responseError(c, http.StatusInternalServerError, ErrInternal, err)
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
	c.ShouldBind(&m)

	addr := c.Param("address")
	if !core.ValidateAddress(addr) {
		ctl.responseError(c, http.StatusBadRequest, ErrValidation, errors.New("invalid address"))
		return
	}

	err := l.(*ledger.Ledger).SaveMeta(
		c.Request.Context(),
		"account",
		addr,
		m,
	)
	if err != nil {
		ctl.responseError(c, http.StatusInternalServerError, ErrInternal, err)
		return
	}
	ctl.noContent(c)
}
