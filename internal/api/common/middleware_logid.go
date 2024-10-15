package common

import (
	"math/rand"
	"net/http"
	"sync"
	"time"

	"go.opentelemetry.io/otel/trace"

	"github.com/formancehq/go-libs/v2/logging"
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

func LogID() func(handler http.Handler) http.Handler {
	return func(handler http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			loggerFields := map[string]any{}
			if span := trace.SpanFromContext(r.Context()); span.SpanContext().TraceID().IsValid() {
				loggerFields["trace-id"] = span.SpanContext().TraceID().String()
			} else {
				loggerFields["trace-id"] = randomTraceID(10)
			}

			r = r.WithContext(logging.ContextWithFields(r.Context(), loggerFields))

			handler.ServeHTTP(w, r)
		})
	}
}
