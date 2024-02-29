package controllers

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/numary/ledger/pkg/api/apierrors"
	"github.com/numary/ledger/pkg/core"
	"github.com/numary/ledger/pkg/ledger"
	"github.com/numary/ledger/pkg/storage/sqlstorage"
)

type AccountController struct{}

func NewAccountController() AccountController {
	return AccountController{}
}

func (ctl *AccountController) CountAccounts(c *gin.Context) {
	l, _ := c.Get("ledger")

	accountsQuery := ledger.NewAccountsQuery().
		WithAddressFilter(c.Query("address")).
		WithMetadataFilter(c.QueryMap("metadata"))

	count, err := l.(*ledger.Ledger).CountAccounts(c.Request.Context(), *accountsQuery)
	if err != nil {
		apierrors.ResponseError(c, err)
		return
	}

	c.Header("Count", fmt.Sprint(count))
}

func (ctl *AccountController) GetAccounts(c *gin.Context) {
	l, _ := c.Get("ledger")

	accountsQuery := ledger.NewAccountsQuery()

	if c.Query(QueryKeyCursor) != "" {
		if c.Query("after") != "" ||
			c.Query("address") != "" ||
			len(c.QueryMap("metadata")) > 0 ||
			c.Query("balance") != "" ||
			c.Query(QueryKeyBalanceOperator) != "" ||
			c.Query(QueryKeyBalanceOperatorDeprecated) != "" ||
			c.Query(QueryKeyPageSize) != "" ||
			c.Query(QueryKeyPageSizeDeprecated) != "" {
			apierrors.ResponseError(c, ledger.NewValidationError(
				fmt.Sprintf("no other query params can be set with '%s'", QueryKeyCursor)))
			return
		}

		res, err := base64.RawURLEncoding.DecodeString(c.Query(QueryKeyCursor))
		if err != nil {
			apierrors.ResponseError(c, ledger.NewValidationError(
				fmt.Sprintf("invalid '%s' query param", QueryKeyCursor)))
			return
		}

		token := sqlstorage.AccPaginationToken{}
		if err := json.Unmarshal(res, &token); err != nil {
			apierrors.ResponseError(c, ledger.NewValidationError(
				fmt.Sprintf("invalid '%s' query param", QueryKeyCursor)))
			return
		}

		accountsQuery = accountsQuery.
			WithOffset(token.Offset).
			WithAfterAddress(token.AfterAddress).
			WithAddressFilter(token.AddressRegexpFilter).
			WithBalanceFilter(token.BalanceFilter).
			WithBalanceOperatorFilter(token.BalanceOperatorFilter).
			WithMetadataFilter(token.MetadataFilter).
			WithPageSize(token.PageSize)

	} else if c.Query(QueryKeyCursorDeprecated) != "" {
		if c.Query("after") != "" ||
			c.Query("address") != "" ||
			len(c.QueryMap("metadata")) > 0 ||
			c.Query("balance") != "" ||
			c.Query(QueryKeyBalanceOperator) != "" ||
			c.Query(QueryKeyBalanceOperatorDeprecated) != "" ||
			c.Query(QueryKeyPageSize) != "" ||
			c.Query(QueryKeyPageSizeDeprecated) != "" {
			apierrors.ResponseError(c, ledger.NewValidationError(
				fmt.Sprintf("no other query params can be set with '%s'", QueryKeyCursorDeprecated)))
			return
		}

		res, err := base64.RawURLEncoding.DecodeString(c.Query(QueryKeyCursorDeprecated))
		if err != nil {
			apierrors.ResponseError(c, ledger.NewValidationError(
				fmt.Sprintf("invalid '%s' query param", QueryKeyCursorDeprecated)))
			return
		}

		token := sqlstorage.AccPaginationToken{}
		if err := json.Unmarshal(res, &token); err != nil {
			apierrors.ResponseError(c, ledger.NewValidationError(
				fmt.Sprintf("invalid '%s' query param", QueryKeyCursorDeprecated)))
			return
		}

		accountsQuery = accountsQuery.
			WithOffset(token.Offset).
			WithAfterAddress(token.AfterAddress).
			WithAddressFilter(token.AddressRegexpFilter).
			WithBalanceFilter(token.BalanceFilter).
			WithBalanceOperatorFilter(token.BalanceOperatorFilter).
			WithMetadataFilter(token.MetadataFilter).
			WithPageSize(token.PageSize)

	} else {
		balance := c.Query("balance")
		if balance != "" {
			if _, err := strconv.ParseInt(balance, 10, 64); err != nil {
				apierrors.ResponseError(c, ledger.NewValidationError(
					"invalid parameter 'balance', should be a number"))
				return
			}
		}

		balanceOperator, err := getBalanceOperator(c)
		if err != nil {
			apierrors.ResponseError(c, err)
			return
		}

		pageSize, err := getPageSize(c)
		if err != nil {
			apierrors.ResponseError(c, err)
			return
		}

		accountsQuery = accountsQuery.
			WithAfterAddress(c.Query("after")).
			WithAddressFilter(c.Query("address")).
			WithBalanceFilter(balance).
			WithBalanceOperatorFilter(balanceOperator).
			WithBalanceAssetFilter(c.Query("balanceAsset")).
			WithMetadataFilter(c.QueryMap("metadata")).
			WithPageSize(pageSize)
	}

	cursor, err := l.(*ledger.Ledger).GetAccounts(c.Request.Context(), *accountsQuery)
	if err != nil {
		apierrors.ResponseError(c, err)
		return
	}

	respondWithCursor[core.Account](c, http.StatusOK, cursor)
}

func (ctl *AccountController) GetAccount(c *gin.Context) {
	l, _ := c.Get("ledger")

	if !core.ValidateAddress(c.Param("address")) {
		apierrors.ResponseError(c, ledger.NewValidationError("invalid account address format"))
		return
	}

	acc, err := l.(*ledger.Ledger).GetAccount(
		c.Request.Context(),
		c.Param("address"))
	if err != nil {
		apierrors.ResponseError(c, err)
		return
	}

	respondWithData[*core.AccountWithVolumes](c, http.StatusOK, acc)
}

func (ctl *AccountController) PostAccountMetadata(c *gin.Context) {
	l, _ := c.Get("ledger")

	if !core.ValidateAddress(c.Param("address")) {
		apierrors.ResponseError(c, ledger.NewValidationError("invalid account address format"))
		return
	}

	var m core.Metadata
	if err := c.ShouldBindJSON(&m); err != nil {
		apierrors.ResponseError(c, ledger.NewValidationError("invalid metadata format"))
		return
	}

	if err := l.(*ledger.Ledger).SaveMeta(c.Request.Context(),
		core.MetaTargetTypeAccount, c.Param("address"), m); err != nil {
		apierrors.ResponseError(c, err)
		return
	}

	respondWithNoContent(c)
}
