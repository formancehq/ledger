package nats

import "github.com/nats-io/nats.go"

// JetStreamConfig contains configuration settings specific to running in JetStream mode
type JetStreamConfig struct {
	// Disabled controls whether JetStream semantics should be used
	Disabled bool

	// AutoProvision indicates the application should create the configured stream if missing on the broker
	AutoProvision bool

	// ConnectOptions contains JetStream-specific options to be used when establishing context
	ConnectOptions []nats.JSOpt

	// SubscribeOptions contains options to be used when establishing subscriptions
	SubscribeOptions []nats.SubOpt

	// PublishOptions contains options to be sent on every publish operation
	PublishOptions []nats.PubOpt

	// TrackMsgId uses the Nats.MsgId option with the msg UUID to prevent duplication (needed for exactly once processing)
	TrackMsgId bool

	// AckAsync enables asynchronous acknowledgement
	AckAsync bool

	// DurablePrefix is the prefix used by to derive the durable name from the topic.
	//
	// By default the prefix will be used on its own to form the durable name.  This only allows use
	// of a single subscription per configuration.  For more flexibility provide a DurableCalculator
	// that will receive durable prefix + topic.
	//
	// Subscriptions may also specify a “durable name” which will survive client restarts.
	// Durable subscriptions cause the server to track the last acknowledged message
	// sequence number for a client and durable name. When the client restarts/resubscribes,
	// and uses the same client ID and durable name, the server will resume delivery beginning
	// with the earliest unacknowledged message for this durable subscription.
	//
	// Doing this causes the JetStream server to track
	// the last acknowledged message for that ClientID + Durable.
	DurablePrefix string

	// DurableCalculator is a custom function used to derive a durable name from a topic + durable prefix
	DurableCalculator DurableCalculator
}

type DurableCalculator = func(string, string) string

func (c JetStreamConfig) CalculateDurableName(topic string) string {
	if c.DurableCalculator != nil {
		return c.DurableCalculator(c.DurablePrefix, topic)
	} else if c.DurablePrefix != "" {
		return c.DurablePrefix //TODO: should we try to do anything with topic by default?
	}
	return ""
}

func (c JetStreamConfig) ShouldAutoProvision() bool {
	return !c.Disabled && c.AutoProvision
}
