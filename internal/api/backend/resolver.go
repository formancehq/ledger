package backend

import (
	"math/rand"
	"net/http"
	"strings"
	"sync"
	"time"

	sharedapi "github.com/formancehq/stack/libs/go-libs/api"

	"github.com/pkg/errors"

	"github.com/formancehq/ledger/internal/opentelemetry/tracer"
	"github.com/formancehq/stack/libs/go-libs/logging"
	"github.com/go-chi/chi/v5"
)

var (
	r  *rand.Rand
	mu sync.Mutex
)

const (
	ErrOutdatedSchema = "OUTDATED_SCHEMA"
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
	resolver Backend,
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
				sharedapi.BadRequest(w, sharedapi.ErrorInternal, err)
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
					sharedapi.BadRequest(w, sharedapi.ErrorInternal, err)
					return
				}
				if !isUpToDate {
					sharedapi.BadRequest(w, ErrOutdatedSchema, errors.New("You need to upgrade your ledger schema to the last version"))
					return
				}
			}

			handler.ServeHTTP(w, r.WithContext(ContextWithLedger(r.Context(), l)))
		})
	}
}
