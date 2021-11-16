package controllers

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/numary/ledger/ledger/query"
)

// BaseController -
type BaseController struct {
}

// NewBaseController -
func NewBaseController() *BaseController {
	return &BaseController{}
}

// CreateBaseController -
func CreateBaseController() *BaseController {
	return NewBaseController()
}

func (ctl *BaseController) responseResource(c *gin.Context, status int, data interface{}, resourceFormat interface{}) {
	response, err := ctl.toResource(data, resourceFormat)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, err)
		return
	}
	c.JSON(status, response)
}

func (ctl *BaseController) responseCollection(c *gin.Context, status int, cursor query.Cursor) {
	c.JSON(status, cursor)
}

func (ctl *BaseController) responseError(c *gin.Context, status int, err error) {
	c.AbortWithStatusJSON(status, gin.H{
		"error":   true,
		"code":    status,
		"message": err.Error(),
	})
}

func (ctl *BaseController) toResource(data interface{}, toFormat interface{}) (interface{}, error) {
	if toFormat == nil {
		return nil, errors.New("toFormat is nil")
	}
	b, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}
	if err = json.Unmarshal(b, toFormat); err != nil {
		return nil, err
	}
	return toFormat, nil
}
