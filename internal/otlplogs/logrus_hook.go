package otlplogs

import (
	"context"
	"fmt"

	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/log"
)

type otelLogrusHook struct {
	Logger log.Logger
}

func (h *otelLogrusHook) Levels() []logrus.Level {
	return logrus.AllLevels
}

func (h *otelLogrusHook) Fire(e *logrus.Entry) error {
	attrs := make([]log.KeyValue, 0, len(e.Data)+2)

	for k, v := range e.Data {
		attrs = append(attrs, log.String(k, fmt.Sprint(v)))
	}

	attrs = append(attrs, log.String("log.level", e.Level.String()))

	rec := log.Record{}
	rec.SetBody(log.StringValue(e.Message))
	rec.AddAttributes(attrs...)

	h.Logger.Emit(context.Background(), rec)
	return nil
}
