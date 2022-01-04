package middlewares

import (
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
func (m AuthMiddleware) AuthMiddleware(engine *gin.Engine) gin.HandlerFunc {
	return func(c *gin.Context) {
		if auth := m.HTTPBasic; auth != "" {
			segment := strings.Split(auth, ":")
			engine.Use(gin.BasicAuth(gin.Accounts{
				segment[0]: segment[1],
			}))
		}
	}
}
