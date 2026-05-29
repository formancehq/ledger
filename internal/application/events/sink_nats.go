//go:build nats

package events

import (
	"context"
	"fmt"
	"strings"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/eventspb"
)

func init() {
	registerSinkFactory("nats", func(sc *commonpb.SinkConfig, format Format) (Sink, error) {
		s := sc.GetType().(*commonpb.SinkConfig_Nats)

		return NewNATSSink(NATSSinkConfig{
			URL:    s.Nats.GetUrl(),
			Topic:  s.Nats.GetTopic(),
			Format: format,
		})
	})
}

// NATSSinkConfig holds configuration for the NATS JetStream sink.
type NATSSinkConfig struct {
	URL    string
	Topic  string
	Format Format
}

// NATSSink publishes events to NATS JetStream.
type NATSSink struct {
	conn   *nats.Conn
	js     jetstream.JetStream
	topic  string
	format Format
}

// NewNATSSink creates a new NATS JetStream sink.
func NewNATSSink(cfg NATSSinkConfig) (*NATSSink, error) {
	conn, err := nats.Connect(cfg.URL)
	if err != nil {
		return nil, fmt.Errorf("connecting to NATS: %w", err)
	}

	js, err := jetstream.New(conn)
	if err != nil {
		conn.Close()

		return nil, fmt.Errorf("creating JetStream context: %w", err)
	}

	return &NATSSink{
		conn:   conn,
		js:     js,
		topic:  cfg.Topic,
		format: cfg.Format,
	}, nil
}

func (s *NATSSink) Publish(ctx context.Context, events []*eventspb.Event) error {
	for _, event := range events {
		data, err := SerializeEvent(event, s.format)
		if err != nil {
			return fmt.Errorf("serializing event seq=%d: %w", event.GetLogSequence(), err)
		}

		subject := s.subject(event)
		if _, err := s.js.Publish(ctx, subject, data); err != nil {
			return fmt.Errorf("publishing event seq=%d to %s: %w", event.GetLogSequence(), subject, err)
		}
	}

	return nil
}

func (s *NATSSink) Close() error {
	s.conn.Close()

	return nil
}

// subject returns the NATS subject for the given event.
// Format: {topic}.{ledger}.{type} (e.g., "ledger-events.orders.COMMITTED_TRANSACTION").
func (s *NATSSink) subject(event *eventspb.Event) string {
	ledger := event.GetLedger()
	if ledger == "" {
		ledger = "_system"
	}

	eventType := strings.ToLower(event.GetType().String())

	return fmt.Sprintf("%s.%s.%s", s.topic, ledger, eventType)
}
