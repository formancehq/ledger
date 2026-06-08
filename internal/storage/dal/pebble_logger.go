package dal

import (
	"fmt"

	"github.com/cockroachdb/pebble/v2"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
)

// PebbleLogger adapts a logging.Logger to pebble.Logger.
// Pebble's Infof messages (WAL replay, compaction stats, etc.) fire per
// background event and would drown out useful Debug output, so they are routed
// to Trace level.
type PebbleLogger struct {
	logger logging.Logger
}

func NewPebbleLogger(logger logging.Logger) pebble.Logger {
	return &PebbleLogger{logger: logger}
}

func (l *PebbleLogger) Infof(format string, args ...any) {
	if l.logger.Enabled(logging.TraceLevel) {
		l.logger.Tracef(format, args...)
	}
}

func (l *PebbleLogger) Errorf(format string, args ...any) {
	l.logger.Errorf(format, args...)
}

func (l *PebbleLogger) Fatalf(format string, args ...any) {
	panic(fmt.Sprintf(format, args...))
}

// DiscardPebbleLogger returns a pebble.Logger that silences all output.
func DiscardPebbleLogger() pebble.Logger {
	return &discardPebbleLogger{}
}

type discardPebbleLogger struct{}

func (*discardPebbleLogger) Infof(string, ...any)  {}
func (*discardPebbleLogger) Errorf(string, ...any) {}

func (*discardPebbleLogger) Fatalf(format string, args ...any) {
	panic(fmt.Sprintf(format, args...))
}
