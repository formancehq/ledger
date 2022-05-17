package middlewares

import (
	"time"

	"github.com/gin-gonic/gin"
	"github.com/numary/go-libs/sharedlogging"
)

func Log() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		c.Next()
		latency := time.Since(start)
		sharedlogging.GetLogger(c.Request.Context()).WithFields(map[string]interface{}{
			"status":     c.Writer.Status(),
			"method":     c.Request.Method,
			"path":       c.Request.URL.Path,
			"ip":         c.ClientIP(),
			"latency":    latency,
			"user_agent": c.Request.UserAgent(),
		}).Info("Request")
	}
}
