package controllers

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/numary/ledger/pkg/core"
	"github.com/numary/ledger/pkg/ledger"
)

type MappingController struct{}

func NewMappingController() MappingController {
	return MappingController{}
}

func (ctl *MappingController) PutMapping(c *gin.Context) {
	l, _ := c.Get("ledger")

	mapping := &core.Mapping{}
	err := c.ShouldBind(mapping)
	if err != nil {
		ResponseError(c, err)
		return
	}

	err = l.(*ledger.Ledger).SaveMapping(c.Request.Context(), *mapping)
	if err != nil {
		ResponseError(c, err)
		return
	}
	respondWithData[*core.Mapping](c, http.StatusOK, mapping)
}

func (ctl *MappingController) GetMapping(c *gin.Context) {
	l, _ := c.Get("ledger")

	mapping, err := l.(*ledger.Ledger).LoadMapping(c.Request.Context())
	if err != nil {
		ResponseError(c, err)
		return
	}
	respondWithData[*core.Mapping](c, http.StatusOK, mapping)
}
