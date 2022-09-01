package controllers

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/numary/go-libs/sharedapi"
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
