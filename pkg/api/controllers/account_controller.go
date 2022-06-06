package controllers

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/numary/ledger/pkg/core"
	"github.com/numary/ledger/pkg/ledger"
	"github.com/numary/ledger/pkg/ledger/query"
	"github.com/numary/ledger/pkg/storage/sqlstorage"
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
		query.Address(c.Query("address")),
		query.Metadata(c.QueryMap("metadata")),
	)
	if err != nil {
		ResponseError(c, err)
		return
	}

	c.Header("Count", fmt.Sprint(count))
}

func (ctl *AccountController) GetAccounts(c *gin.Context) {
	l, _ := c.Get("ledger")

	after := c.Query("after")
	if c.Query("pagination_token") != "" {
		res, err := base64.RawURLEncoding.DecodeString(c.Query("pagination_token"))
		if err != nil {
			ResponseError(c, ledger.NewValidationError("invalid query value 'pagination_token'"))
			return
		}
		t := sqlstorage.PaginationToken{}
		if err = json.Unmarshal(res, &t); err != nil {
			ResponseError(c, ledger.NewValidationError("invalid query value 'pagination_token'"))
			return
		}
		after = strconv.FormatUint(t.ID, 10)
	}

	cursor, err := l.(*ledger.Ledger).GetAccounts(
		c.Request.Context(),
		query.After(after),
		query.Address(c.Query("address")),
		query.Metadata(c.QueryMap("metadata")),
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
