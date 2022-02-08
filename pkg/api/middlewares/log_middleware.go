package middlewares

import (
	"github.com/gin-gonic/gin"
	"github.com/numary/ledger/pkg/logging"
	"time"
)

func Log(logger logging.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		c.Next()
		latency := time.Now().Sub(start)
		logger.WithFields(map[string]interface{}{
			"status":     c.Writer.Status(),
			"method":     c.Request.Method,
			"path":       c.Request.URL.Path,
			"ip":         c.ClientIP(),
			"latency":    latency,
			"user_agent": c.Request.UserAgent(),
		}).Info(c.Request.Context(), "Request")
	}
}
