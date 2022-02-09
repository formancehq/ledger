package middlewares

import (
	"github.com/gin-gonic/gin"
	"github.com/numary/ledger/pkg/api/controllers"
	"github.com/numary/ledger/pkg/ledger"
	"github.com/numary/ledger/pkg/logging"
	"net/http"
)

// LedgerMiddleware struct
type LedgerMiddleware struct {
	resolver *ledger.Resolver
	logger   logging.Logger
}

// NewLedgerMiddleware
func NewLedgerMiddleware(
	resolver *ledger.Resolver,
	logger logging.Logger,
) LedgerMiddleware {
	return LedgerMiddleware{
		resolver: resolver,
		logger:   logger,
	}
}

// LedgerMiddleware
func (m *LedgerMiddleware) LedgerMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		name := c.Param("ledger")

		if name == "" {
			return
		}

		l, err := m.resolver.GetLedger(c.Request.Context(), name)
		if err != nil {
			statusCode := http.StatusBadRequest
			res := controllers.ErrorResponse{
				ErrorCode:    controllers.ErrInternal,
				ErrorMessage: err.Error(),
			}
			switch {
			case ledger.IsUnavailableStoreError(err):
				statusCode = http.StatusServiceUnavailable
			}
			c.AbortWithStatusJSON(statusCode, res)
			return
		}
		defer func() {
			err := l.Close(c.Request.Context())
			if err != nil {
				m.logger.Warn(c.Request.Context(), "error closing ledger: %s", err)
			}
		}()
		c.Set("ledger", l)

		c.Next()
	}
}
