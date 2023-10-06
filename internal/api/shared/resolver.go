package shared

import (
	"math/rand"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"

	"github.com/formancehq/ledger/internal/api/backend"
	"github.com/formancehq/ledger/internal/opentelemetry/tracer"
	"github.com/formancehq/stack/libs/go-libs/logging"
	"github.com/go-chi/chi/v5"
)

var (
	r  *rand.Rand
	mu sync.Mutex
)

func init() {
	r = rand.New(rand.NewSource(time.Now().UnixNano()))
}

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

func randomTraceID(n int) string {
	mu.Lock()
	defer mu.Unlock()

	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[r.Intn(len(letterRunes))]
	}
	return string(b)
}

func LedgerMiddleware(
	resolver backend.Backend,
	excludePathFromSchemaCheck []string,
) func(handler http.Handler) http.Handler {
	return func(handler http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			name := chi.URLParam(r, "ledger")
			if name == "" {
				w.WriteHeader(http.StatusNotFound)
				return
			}

			ctx, span := tracer.Start(r.Context(), name)
			defer span.End()

			r = r.WithContext(ctx)

			loggerFields := map[string]any{
				"ledger": name,
			}
			if span.SpanContext().TraceID().IsValid() {
				loggerFields["trace-id"] = span.SpanContext().TraceID().String()
			} else {
				loggerFields["trace-id"] = randomTraceID(10)
			}

			r = r.WithContext(logging.ContextWithFields(r.Context(), loggerFields))

			l, err := resolver.GetLedger(r.Context(), name)
			if err != nil {
				ResponseError(w, r, err)
				return
			}

			excluded := false
			for _, path := range excludePathFromSchemaCheck {
				if strings.HasSuffix(r.URL.Path, path) {
					excluded = true
					break
				}
			}

			if !excluded {
				isUpToDate, err := l.IsDatabaseUpToDate(ctx)
				if err != nil {
					ResponseError(w, r, err)
					return
				}
				if !isUpToDate {
					ResponseError(w, r, errors.New("outdated schema"))
					return
				}
			}

			handler.ServeHTTP(w, r.WithContext(ContextWithLedger(r.Context(), l)))
		})
	}
}
