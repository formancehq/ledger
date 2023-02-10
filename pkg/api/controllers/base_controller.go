package controllers

import (
	"net/http"

	"github.com/formancehq/stack/libs/go-libs/api"
	"github.com/gin-gonic/gin"
)

func respondWithNoContent(c *gin.Context) {
	c.Status(http.StatusNoContent)
}

func respondWithCursor[T any](c *gin.Context, status int, data api.Cursor[T]) {
	c.JSON(status, api.BaseResponse[T]{
		Cursor: &data,
	})
}

func respondWithData[T any](c *gin.Context, status int, data T) {
	c.JSON(status, api.BaseResponse[T]{
		Data: &data,
	})
}
