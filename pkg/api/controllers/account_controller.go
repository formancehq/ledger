package controllers

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/numary/go-libs/sharedapi"
	"github.com/numary/ledger/pkg/core"
	"github.com/numary/ledger/pkg/ledger"
	"github.com/numary/ledger/pkg/ledger/query"
	"github.com/numary/ledger/pkg/storage/sqlstorage"
)

type AccountController struct{}

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

	var cursor sharedapi.Cursor[core.Account]
	var err error

	if c.Query("pagination_token") != "" {
		if c.Query("after") != "" ||
			c.Query("address") != "" ||
			len(c.QueryMap("metadata")) > 0 {
			ResponseError(c, ledger.NewValidationError(
				"no other query params can be set with 'pagination_token'"))
			return
		}

		res, decErr := base64.RawURLEncoding.DecodeString(c.Query("pagination_token"))
		if decErr != nil {
			ResponseError(c, ledger.NewValidationError(
				"invalid query value 'pagination_token'"))
			return
		}

		token := sqlstorage.AccPaginationToken{}
		if err = json.Unmarshal(res, &token); err != nil {
			ResponseError(c, ledger.NewValidationError(
				"invalid query value 'pagination_token'"))
			return
		}

		cursor, err = l.(*ledger.Ledger).GetAccounts(c.Request.Context(),
			query.SetOffset(token.Offset),
			query.SetAfterAddress(token.AfterAddress),
			query.SetAddressRegexpFilter(token.AddressRegexpFilter),
			query.SetMetadataFilter(token.MetadataFilter),
		)
	} else {
		cursor, err = l.(*ledger.Ledger).GetAccounts(c.Request.Context(),
			query.SetAfterAddress(c.Query("after")),
			query.SetAddressRegexpFilter(c.Query("address")),
			query.SetMetadataFilter(c.QueryMap("metadata")),
		)
	}

	if err != nil {
		ResponseError(c, err)
		return
	}

	respondWithCursor[core.Account](c, http.StatusOK, cursor)
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

	respondWithData[core.Account](c, http.StatusOK, acc)
}

func (ctl *AccountController) PostAccountMetadata(c *gin.Context) {
	l, _ := c.Get("ledger")

	var m core.Metadata
	if err := c.ShouldBindJSON(&m); err != nil {
		ResponseError(c, err)
		return
	}

	if !core.ValidateAddress(c.Param("address")) {
		ResponseError(c, errors.New("invalid address"))
		return
	}

	if err := l.(*ledger.Ledger).SaveMeta(c.Request.Context(),
		core.MetaTargetTypeAccount, c.Param("address"), m); err != nil {
		ResponseError(c, err)
		return
	}

	respondWithNoContent(c)
}
