package middlewares

import (
	"github.com/gin-gonic/gin"
	"github.com/numary/ledger/pkg/storage"
)

func Transaction() func(c *gin.Context) {
	return func(c *gin.Context) {
		c.Request = c.Request.WithContext(storage.TransactionalContext(c.Request.Context()))
		defer storage.RollbackTransaction(c.Request.Context())

		c.Next()
		if c.Writer.Status() >= 200 && c.Writer.Status() < 300 {
			if err := storage.CommitTransaction(c.Request.Context()); err != nil {
				c.Error(err)
			}
		}
	}
}
