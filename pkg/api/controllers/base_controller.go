package controllers

import (
	"net/http"

	"github.com/formancehq/go-libs/sharedapi"
	"github.com/gin-gonic/gin"
)

func respondWithNoContent(c *gin.Context) {
	c.Status(http.StatusNoContent)
}

func respondWithCursor[T any](c *gin.Context, status int, data sharedapi.Cursor[T]) {
	c.JSON(status, sharedapi.BaseResponse[T]{
		Cursor: &data,
	})
}

func respondWithData[T any](c *gin.Context, status int, data T) {
	c.JSON(status, sharedapi.BaseResponse[T]{
		Data: &data,
	})
}
