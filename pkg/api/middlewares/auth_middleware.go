package middlewares

import (
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
)

func Auth(httpBasic string) gin.HandlerFunc {
	return func(c *gin.Context) {
		if auth := httpBasic; auth != "" {
			segment := strings.Split(auth, ":")
			username, password, ok := c.Request.BasicAuth()
			if !ok {
				c.AbortWithStatus(http.StatusForbidden)
				return
			}
			if segment[0] != username || segment[1] != password {
				c.AbortWithStatus(http.StatusForbidden)
				return
			}
		}
	}
}
