package controllers

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"
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

func (ctl *BaseController) success(c *gin.Context, status int, data interface{}, responseFormat interface{}) {
	response, err := ctl.toResponse(data, responseFormat)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, err)
		return
	}
	c.JSON(status, response)
}

func (ctl *BaseController) toResponse(data interface{}, toFormat interface{}) (interface{}, error) {
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
