package middlewares

import (
	"context"
	"fmt"

	"github.com/formancehq/go-libs/sharedlogging"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/numary/ledger/pkg"
	"github.com/numary/ledger/pkg/api/apierrors"
	"github.com/numary/ledger/pkg/ledger"
	"github.com/numary/ledger/pkg/opentelemetry"
)

type LedgerMiddleware struct {
	resolver *ledger.Resolver
}

func NewLedgerMiddleware(resolver *ledger.Resolver) LedgerMiddleware {
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

		contextKeyID := uuid.NewString()
		id := span.SpanContext().SpanID()
		if id == [8]byte{} {
			sharedlogging.GetLogger(ctx).Debugf(
				"ledger middleware SpanID is empty, new id generated %s", contextKeyID)
		} else {
			contextKeyID = fmt.Sprint(id)
		}
		ctx = context.WithValue(ctx, pkg.ContextKeyID, contextKeyID)
		c.Header(string(pkg.ContextKeyID), contextKeyID)

		l, err := m.resolver.GetLedger(ctx, name)
		if err != nil {
			apierrors.ResponseError(c, err)
			return
		}
		defer l.Close(ctx)
		c.Set("ledger", l)

		c.Request = c.Request.WithContext(ctx)
		c.Next()
	}
}
