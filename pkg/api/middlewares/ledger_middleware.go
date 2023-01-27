package middlewares

import (
	"github.com/gin-gonic/gin"
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

		//contextKeyID := uuid.NewString()
		//id := span.SpanContext().SpanID()
		//if id == [8]byte{} {
		//	logging.GetLogger(ctx).Debugf(
		//		"ledger middleware SpanID is empty, new id generated %s", contextKeyID)
		//} else {
		//	contextKeyID = fmt.Sprint(id)
		//}
		//ctx = context.WithValue(ctx, pkg.KeyContextID, contextKeyID)
		//c.Header(string(pkg.KeyContextID), contextKeyID)
		//
		//loggerFactory := logging.StaticLoggerFactory(
		//	contextlogger.New(ctx, logging.GetLogger(ctx)))
		//logging.SetFactory(loggerFactory)

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
