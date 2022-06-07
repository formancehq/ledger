package controllers

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

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
	paginationToken := c.Query("pagination_token")
	afterAddress := c.Query("after")
	addressRegexpFilter := c.Query("address")
	metadataFilter := c.QueryMap("metadata")

	if paginationToken != "" {
		if afterAddress != "" || addressRegexpFilter != "" || len(metadataFilter) != 0 {
			ResponseError(c, ledger.NewValidationError(
				"no other query params can be set with 'pagination_token'"))
			return
		}

		res, err := base64.RawURLEncoding.DecodeString(paginationToken)
		if err != nil {
			ResponseError(c, ledger.NewValidationError("invalid query value 'pagination_token'"))
			return
		}
		t := sqlstorage.AccPaginationToken{}
		if err = json.Unmarshal(res, &t); err != nil {
			ResponseError(c, ledger.NewValidationError("invalid query value 'pagination_token'"))
			return
		}

		cursor, err := l.(*ledger.Ledger).GetAccounts(
			c.Request.Context(),
			query.SetOffset(t.Offset),
			query.SetAfterAddress(t.AfterAddress),
			query.SetAddressRegexpFilter(t.AddressRegexpFilter),
			query.SetMetadataFilter(t.MetadataFilter),
		)
		if err != nil {
			ResponseError(c, err)
			return
		}

		ctl.response(c, http.StatusOK, cursor)
		return
	}

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
