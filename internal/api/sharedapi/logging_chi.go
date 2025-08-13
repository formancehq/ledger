package api

import (
	"fmt"
	"net/http"
	"time"

	"github.com/formancehq/go-libs/logging"
	"github.com/go-chi/chi/v5/middleware"
)

type chiLogEntry struct {
	r *http.Request
}

func (c *chiLogEntry) Write(status, bytes int, _ http.Header, elapsed time.Duration, extra interface{}) {
	fields := map[string]any{
		"status":  status,
		"bytes":   bytes,
		"elapsed": elapsed,
	}
	if extra != nil {
		fields["extra"] = extra
	}
	logging.FromContext(c.r.Context()).
		WithFields(fields).
		Infof("%s %s", c.r.Method, c.r.URL.Path)
}

func (c *chiLogEntry) Panic(v interface{}, stack []byte) {
	panic(fmt.Sprintf("%s\n%s", v, stack))
}

var _ middleware.LogEntry = (*chiLogEntry)(nil)

type chiLogFormatter struct{}

func (c chiLogFormatter) NewLogEntry(r *http.Request) middleware.LogEntry {
	return &chiLogEntry{
		r: r,
	}
}

var _ middleware.LogFormatter = (*chiLogFormatter)(nil)

func NewLogFormatter() *chiLogFormatter {
	return &chiLogFormatter{}
}
