package publish

import (
	"context"
	"encoding/json"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/google/uuid"
)

const (
	// NOTE: this const is also copid inside the circuit breaker package
	// (to prevent a circular dependency). If you change it here, change it
	// there as well.
	otelContextKey = "otel-context"
)

func NewMessage(ctx context.Context, m EventMessage) *message.Message {
	data, err := json.Marshal(m)
	if err != nil {
		panic(err)
	}

	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	otelContext, _ := json.Marshal(carrier)

	msg := message.NewMessage(uuid.NewString(), data)
	msg.SetContext(ctx)
	msg.Metadata[otelContextKey] = string(otelContext)

	return msg
}

type EventMessage struct {
	Date    time.Time `json:"date"`
	App     string    `json:"app"`
	Version string    `json:"version"`
	Type    string    `json:"type"`
	Payload any       `json:"payload"`
}

func UnmarshalMessage(msg *message.Message) (trace.Span, *EventMessage, error) {
	ev := &EventMessage{}
	if err := json.Unmarshal(msg.Payload, ev); err != nil {
		return nil, nil, err
	}
	carrier := propagation.MapCarrier{}
	ctx := context.TODO()
	if err := json.Unmarshal([]byte(msg.Metadata[otelContextKey]), &carrier); err == nil {
		ctx = otel.GetTextMapPropagator().Extract(msg.Context(), carrier)
	}
	return trace.SpanFromContext(ctx), ev, nil
}
