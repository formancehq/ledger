package middlewares

import (
	"github.com/formancehq/go-libs/sharedlogging"
	"github.com/gin-gonic/gin"
	"github.com/numary/ledger/pkg/api/apierrors"
	"github.com/numary/ledger/pkg/ledger"
	"github.com/numary/ledger/pkg/opentelemetry"
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

		ctx, span := opentelemetry.Start(c.Request.Context(), "Ledger access")
		defer span.End()

		l, err := m.resolver.GetLedger(ctx, name)
		if err != nil {
			apierrors.ResponseError(c, err)
			return
		}
		defer func() {
			err := l.Close(ctx)
			if err != nil {
				sharedlogging.GetLogger(ctx).Errorf("error closing ledger: %s", err)
			}
		}()
		c.Set("ledger", l)

		c.Request = c.Request.WithContext(ctx)
		c.Next()
	}
}
