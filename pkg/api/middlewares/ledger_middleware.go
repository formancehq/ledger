package middlewares

import (
	"github.com/davecgh/go-spew/spew"
	"github.com/gin-gonic/gin"
	"github.com/numary/go-libs/sharedlogging"
	"github.com/numary/ledger/pkg/api/controllers"
	"github.com/numary/ledger/pkg/ledger"
)

// LedgerMiddleware struct
type LedgerMiddleware struct {
	resolver *ledger.Resolver
}

// NewLedgerMiddleware
func NewLedgerMiddleware(
	resolver *ledger.Resolver,
) LedgerMiddleware {
	return LedgerMiddleware{
		resolver: resolver,
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
			spew.Dump(err)
			controllers.ResponseError(c, err)
			return
		}
		defer func() {
			err := l.Close(c.Request.Context())
			if err != nil {
				sharedlogging.GetLogger(c.Request.Context()).Errorf("error closing ledger: %s", err)
			}
		}()
		c.Set("ledger", l)

		c.Next()
	}
}
