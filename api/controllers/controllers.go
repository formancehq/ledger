package controllers

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/numary/ledger/ledger/query"
	"go.uber.org/fx"
)

var Module = fx.Options(
	fx.Provide(NewTransactionController),
)

// Controllers -
type Controllers struct {
}

func (ctl *Controllers) responseResource(c *gin.Context, status int, data interface{}, resourceFormat interface{}) {
	response, err := ctl.toResource(data, resourceFormat)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, err)
		return
	}
	c.JSON(status, response)
}

func (ctl *Controllers) responseCollection(c *gin.Context, status int, cursor query.Cursor) {
	c.JSON(status, cursor)
}

func (ctl *Controllers) responseError(c *gin.Context, status int, err error) {
	c.AbortWithStatusJSON(status, gin.H{
		"error":   true,
		"code":    status,
		"message": err.Error(),
	})
}

func (ctl *Controllers) toResource(data interface{}, toResource interface{}) (interface{}, error) {
	if toResource == nil {
		return nil, errors.New("toResource is nil")
	}
	b, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}
	if err = json.Unmarshal(b, toResource); err != nil {
		return nil, err
	}
	return toResource, nil
}
