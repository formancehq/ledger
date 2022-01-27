package middlewares

import (
	"github.com/gin-gonic/gin"
	"github.com/numary/ledger/pkg/ledger"
	"github.com/numary/ledger/pkg/logging"
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
			c.JSON(400, gin.H{
				"error":         true,
				"error_code":    400,
				"error_message": err.Error(),
			})
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
