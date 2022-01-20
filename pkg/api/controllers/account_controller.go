package controllers

import (
	"net/http"

	"github.com/numary/ledger/pkg/core"
	"github.com/numary/ledger/pkg/ledger"
	"github.com/numary/ledger/pkg/ledger/query"

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

// GetAccounts godoc
// @Summary List all accounts
// @Schemes
// @Param ledger path string true "ledger"
// @Param after query string true "pagination cursor, will return accounts after given address (in descending order)"
// @Accept json
// @Produce json
// @Success 200 {object} controllers.BaseResponse{cursor=query.Cursor{data=[]core.Account}}
// @Router /{ledger}/accounts [get]
func (ctl *AccountController) GetAccounts(c *gin.Context) {
	l, _ := c.Get("ledger")
	cursor, err := l.(*ledger.Ledger).FindAccounts(
		c.Request.Context(),
		query.After(c.Query("after")),
	)
	if err != nil {
		ctl.responseError(
			c,
			http.StatusInternalServerError,
			err,
		)
		return
	}
	ctl.response(
		c,
		http.StatusOK,
		cursor,
	)
}

// GetAccount godoc
// @Summary Get account by address
// @Schemes
// @Param ledger path string true "ledger"
// @Param accountId path string true "accountId"
// @Accept json
// @Produce json
// @Success 200 {object} controllers.BaseResponse{account=core.Account}
// @Router /{ledger}/accounts/{accountId} [get]
func (ctl *AccountController) GetAccount(c *gin.Context) {
	l, _ := c.Get("ledger")
	acc, err := l.(*ledger.Ledger).GetAccount(c.Request.Context(), c.Param("address"))
	if err != nil {
		ctl.responseError(
			c,
			http.StatusInternalServerError,
			err,
		)
		return
	}
	ctl.response(
		c,
		http.StatusOK,
		acc,
	)
}

// PostAccountMetadata godoc
// @Summary Add metadata to account
// @Schemes
// @Param ledger path string true "ledger"
// @Param accountId path string true "accountId"
// @Accept json
// @Produce json
// @Success 200 {object} controllers.BaseResponse
// @Router /{ledger}/accounts/{accountId}/metadata [post]
func (ctl *AccountController) PostAccountMetadata(c *gin.Context) {
	l, _ := c.Get("ledger")
	var m core.Metadata
	c.ShouldBind(&m)
	err := l.(*ledger.Ledger).SaveMeta(
		c.Request.Context(),
		"account",
		c.Param("address"),
		m,
	)
	if err != nil {
		ctl.responseError(
			c,
			http.StatusInternalServerError,
			err,
		)
		return
	}
	ctl.response(
		c,
		http.StatusOK,
		nil,
	)
}
