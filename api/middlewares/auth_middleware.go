package middlewares

import (
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/spf13/viper"
)

// AuthMiddleware struct
type AuthMiddleware struct{}

// NewAuthMiddleware
func NewAuthMiddleware() AuthMiddleware {
	return AuthMiddleware{}
}

// AuthMiddleware
func (m AuthMiddleware) AuthMiddleware(engine *gin.Engine) gin.HandlerFunc {
	return func(c *gin.Context) {
		if auth := viper.Get("server.http.basic_auth"); auth != nil {
			segment := strings.Split(auth.(string), ":")
			engine.Use(gin.BasicAuth(gin.Accounts{
				segment[0]: segment[1],
			}))
		}
	}
}
