package middlewares

import (
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
)

// AuthMiddleware struct
type AuthMiddleware struct {
	HTTPBasic string
}

// NewAuthMiddleware
func NewAuthMiddleware(httpBasic string) AuthMiddleware {
	return AuthMiddleware{
		HTTPBasic: httpBasic,
	}
}

// AuthMiddleware
func (m AuthMiddleware) AuthMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		if auth := m.HTTPBasic; auth != "" {
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
