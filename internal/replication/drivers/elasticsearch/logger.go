package elasticsearch

import (
	"github.com/olivere/elastic/v7"
)

type Logger struct {
	fn func(fmt string, args ...any)
}

func (l Logger) Printf(format string, v ...interface{}) {
	l.fn(format, v...)
}

var _ elastic.Logger = (*Logger)(nil)

func newLogger(fn func(fmt string, args ...any)) Logger {
	return Logger{
		fn: fn,
	}
}
