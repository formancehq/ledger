package nats

import (
	"github.com/nats-io/nats.go"
)

type Connection interface {
	// QueueSubscribe subscribes to a NATS subject, equivalent to default Subscribe if queuegroup not supplied.
	QueueSubscribe(string, string, nats.MsgHandler) (*nats.Subscription, error)
	// PublishMsg sends the provided NATS message to the broker.
	PublishMsg(*nats.Msg) error
	// Drain will end all active subscription interest and attempt to wait for in-flight messages to process before closing.
	Drain() error
	// Close will close the connection
	Close()
}

// jsConnection mimics the core NATS publish/subscribe API
// so that NATS and jetstream can use the same client orchestration.
type jsConnection struct {
	conn *nats.Conn
	js   nats.JetStreamContext
	cfg  JetStreamConfig
}

// Subscribe subscribes to a JetStream subject
func (j jsConnection) QueueSubscribe(s string, q string, handler nats.MsgHandler) (*nats.Subscription, error) {
	opts := j.cfg.SubscribeOptions

	if durable := j.cfg.CalculateDurableName(s); durable != "" {
		opts = append(opts, nats.Durable(durable))
	} else {
		// find & bind stream based on subscription subject
		opts = append(opts, nats.BindStream(""))
	}

	return j.js.QueueSubscribe(s, q, handler, opts...)
}

// PublishMsg publishes a message to JetStream
func (j jsConnection) PublishMsg(msg *nats.Msg) (err error) {
	publishOpts := j.cfg.PublishOptions

	if j.cfg.TrackMsgId {
		if msgID := msg.Header.Get(WatermillUUIDHdr); msgID != "" {
			publishOpts = append(publishOpts, nats.MsgId(msgID))
		}
	}

	_, err = j.js.PublishMsg(msg, publishOpts...)

	return
}

// Drain will remove all subscription interest and attempt to wait until all messages have finished processing to close and return.
func (j jsConnection) Drain() error {
	return j.conn.Drain()
}

// Close will close the connection
func (j jsConnection) Close() {
	j.conn.Close()
}
