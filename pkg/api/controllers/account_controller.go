package controllers

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/numary/go-libs/sharedapi"
	"github.com/numary/ledger/pkg/core"
	"github.com/numary/ledger/pkg/ledger"
	"github.com/numary/ledger/pkg/storage"
	"github.com/numary/ledger/pkg/storage/sqlstorage"
	"net/http"
	"strconv"
)

type AccountController struct{}

func NewAccountController() AccountController {
	return AccountController{}
}

func (ctl *AccountController) CountAccounts(c *gin.Context) {
	l, _ := c.Get("ledger")

	accountsQuery := storage.NewAccountsQuery().
		WithAddressFilter(c.Query("address")).
		WithMetadataFilter(c.QueryMap("metadata"))

	count, err := l.(*ledger.Ledger).CountAccounts(c.Request.Context(), *accountsQuery)
	if err != nil {
		ResponseError(c, err)
		return
	}

	c.Header("Count", fmt.Sprint(count))
}

func (ctl *AccountController) GetAccounts(c *gin.Context) {
	l, _ := c.Get("ledger")

	var cursor sharedapi.Cursor[core.Account]
	var accountsQuery *storage.AccountsQuery
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

		accountsQuery = storage.NewAccountsQuery().
			WithOffset(token.Offset).
			WithAfterAddress(token.AfterAddress).
			WithAddressFilter(token.AddressRegexpFilter).
			WithBalanceFilter(token.BalanceFilter).
			WithBalanceOperatorFilter(token.BalanceOperatorFilter).
			WithMetadataFilter(token.MetadataFilter)

	} else {
		balance := c.Query("balance")
		if balance != "" {
			if _, err := strconv.ParseInt(balance, 10, 64); err != nil {
				ResponseError(c, ledger.NewValidationError(
					"invalid parameter 'balance', should be a number"))
				return
			}
		}

		var balanceOperator = storage.DefaultBalanceOperator
		if balanceOperatorStr := c.Query("balance_operator"); balanceOperatorStr != "" {
			var ok bool
			if balanceOperator, ok = storage.NewBalanceOperator(balanceOperatorStr); !ok {
				ResponseError(c, ledger.NewValidationError(
					"invalid parameter 'balance_operator', should be one of 'e, gt, gte, lt, lte'"))
				return
			}
		}

		accountsQuery = storage.NewAccountsQuery().
			WithAfterAddress(c.Query("after")).
			WithAddressFilter(c.Query("address")).
			WithBalanceFilter(balance).
			WithBalanceOperatorFilter(balanceOperator).
			WithMetadataFilter(c.QueryMap("metadata"))
	}

	cursor, err = l.(*ledger.Ledger).GetAccounts(c.Request.Context(), *accountsQuery)

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
