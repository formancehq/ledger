package controllers

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/numary/go-libs/sharedapi"
	"github.com/numary/ledger/pkg/core"
	"github.com/numary/ledger/pkg/ledger"
	"github.com/numary/ledger/pkg/storage"
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
		storage.SetAddressRegexpFilter(c.Query("address")),
		storage.SetMetadataFilter(c.QueryMap("metadata")),
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
			len(c.QueryMap("metadata")) > 0 ||
			c.Query("balance") != "" ||
			c.Query("balance_operator") != "" {
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
			storage.SetOffset(token.Offset),
			storage.SetAfterAddress(token.AfterAddress),
			storage.SetAddressRegexpFilter(token.AddressRegexpFilter),
			storage.SetMetadataFilter(token.MetadataFilter),
			storage.SetBalanceFilter(token.BalanceFilter),
			storage.SetBalanceOperatorFilter(token.BalanceOperatorFilter),
		)
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

		cursor, err = l.(*ledger.Ledger).GetAccounts(c.Request.Context(),
			storage.SetAfterAddress(c.Query("after")),
			storage.SetAddressRegexpFilter(c.Query("address")),
			storage.SetMetadataFilter(c.QueryMap("metadata")),
			storage.SetBalanceFilter(balance),
			storage.SetBalanceOperatorFilter(balanceOperator),
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

	if !core.ValidateAddress(c.Param("address")) {
		ResponseError(c, ledger.NewValidationError("invalid account address format"))
		return
	}

	acc, err := l.(*ledger.Ledger).GetAccount(
		c.Request.Context(),
		c.Param("address"))
	if err != nil {
		ResponseError(c, err)
		return
	}

	respondWithData[*core.AccountWithVolumes](c, http.StatusOK, acc)
}

func (ctl *AccountController) PostAccountMetadata(c *gin.Context) {
	l, _ := c.Get("ledger")

	if !core.ValidateAddress(c.Param("address")) {
		ResponseError(c, ledger.NewValidationError("invalid account address format"))
		return
	}

	var m core.Metadata
	if err := c.ShouldBindJSON(&m); err != nil {
		ResponseError(c, ledger.NewValidationError("invalid metadata format"))
		return
	}

	if err := l.(*ledger.Ledger).SaveMeta(c.Request.Context(),
		core.MetaTargetTypeAccount, c.Param("address"), m); err != nil {
		ResponseError(c, err)
		return
	}

	respondWithNoContent(c)
}
