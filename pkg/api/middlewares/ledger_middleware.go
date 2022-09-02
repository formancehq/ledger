package middlewares

import (
	"github.com/gin-gonic/gin"
	"github.com/numary/go-libs/sharedlogging"
	"github.com/numary/ledger/pkg/api/apierrors"
	"github.com/numary/ledger/pkg/ledger"
)

type LedgerMiddleware struct {
	resolver *ledger.Resolver
}

func NewLedgerMiddleware(
	resolver *ledger.Resolver,
) LedgerMiddleware {
	return LedgerMiddleware{
		resolver: resolver,
	}
}

func (m *LedgerMiddleware) LedgerMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		name := c.Param("ledger")

		if name == "" {
			return
		}

		l, err := m.resolver.GetLedger(c.Request.Context(), name)
		if err != nil {
			apierrors.ResponseError(c, err)
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
