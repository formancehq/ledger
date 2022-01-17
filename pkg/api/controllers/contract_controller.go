package controllers

import (
	"github.com/gin-gonic/gin"
	"github.com/numary/ledger/pkg/core"
	"github.com/numary/ledger/pkg/ledger"
	"github.com/pborman/uuid"
	"net/http"
)

type ContractController struct {
	BaseController
}

func NewContractController() *ContractController {
	return &ContractController{}
}

// PostContract godoc
// @Summary Post Contract
// @Description Create a new contract
// @Tags contracts
// @Schemes
// @Param ledger path string true "ledger"
// @Accept json
// @Produce json
// @Success 200 {object} controllers.BaseResponse
// @Failure 404 {object} controllers.BaseResponse
// @Router /{ledger}/contracts [post]
func (ctl *ContractController) PostContract(c *gin.Context) {
	l, _ := c.Get("ledger")

	contract := &core.Contract{}
	err := c.ShouldBind(contract)
	if err != nil {
		ctl.responseError(c, http.StatusBadRequest, err)
		return
	}
	contract.ID = uuid.New()

	err = l.(*ledger.Ledger).SaveContract(c.Request.Context(), *contract)
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
		contract,
	)
}

// DeleteContract godoc
// @Summary Delete Contract
// @Description Delete a contract
// @Tags contracts
// @Schemes
// @Param ledger path string true "ledger"
// @Param contractId path string true "contractId"
// @Accept json
// @Produce json
// @Success 200 {object} controllers.BaseResponse
// @Failure 404 {object} controllers.BaseResponse
// @Router /{ledger}/contracts/{contractId} [delete]
func (ctl *ContractController) DeleteContract(c *gin.Context) {
	l, _ := c.Get("ledger")

	err := l.(*ledger.Ledger).DeleteContract(c.Request.Context(), c.Param("contractId"))
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
		http.StatusNoContent,
		nil,
	)
}

// GetContracts godoc
// @Summary Get Contracts
// @Description Get all contracts
// @Tags contracts
// @Schemes
// @Param ledger path string true "ledger"
// @Accept json
// @Produce json
// @Success 200 {object} controllers.BaseResponse
// @Failure 404 {object} controllers.BaseResponse
// @Router /{ledger}/contracts [get]
func (ctl *ContractController) GetContracts(c *gin.Context) {
	l, _ := c.Get("ledger")

	contracts, err := l.(*ledger.Ledger).FindContracts(c.Request.Context())
	if err != nil {
		ctl.responseError(c, http.StatusInternalServerError, err)
		return
	}
	ctl.response(
		c,
		http.StatusOK,
		contracts,
	)
}
