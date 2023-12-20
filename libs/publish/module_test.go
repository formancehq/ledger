package publish

import (
	"context"
	"fmt"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	tracesdk "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	"io"
	"os"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/formancehq/stack/libs/go-libs/logging"
	natsServer "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/stretchr/testify/require"
	"go.uber.org/fx"
	"go.uber.org/fx/fxtest"
)

func createRedpandaServer(t *testing.T) string {

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "docker.redpanda.com/vectorized/redpanda",
		Tag:        "v22.3.11",
		Tty:        true,
		Cmd: []string{
			"redpanda", "start",
			"--smp", "1",
			"--overprovisioned",
			"--kafka-addr", "PLAINTEXT://0.0.0.0:9092",
			"--advertise-kafka-addr", "PLAINTEXT://localhost:9092",
			"--pandaproxy-addr", "0.0.0.0:8082",
			"--advertise-pandaproxy-addr", "localhost:8082",
		},
		PortBindings: map[docker.Port][]docker.PortBinding{
			"9092/tcp": {{
				HostIP:   "0.0.0.0",
				HostPort: "9092",
			}},
			"9644/tcp": {{
				HostIP:   "0.0.0.0",
				HostPort: "9644",
			}},
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, pool.Purge(resource))
	})

	stdout := io.Discard
	stderr := io.Discard
	if testing.Verbose() {
		stdout = os.Stdout
		stderr = os.Stderr
	}
	exitCode, err := resource.Exec([]string{
		"rpk",
		"cluster",
		"config",
		"set",
		"auto_create_topics_enabled",
		"true",
	}, dockertest.ExecOptions{
		StdOut: stdout,
		StdErr: stderr,
	})
	require.NoError(t, err)
	require.Equal(t, 0, exitCode)

	return "9092"
}

func TestModule(t *testing.T) {
	t.Parallel()

	tracerProvider := tracesdk.NewTracerProvider()
	otel.SetTracerProvider(tracerProvider)
	otel.SetTextMapPropagator(propagation.TraceContext{})

	type moduleTestCase struct {
		name         string
		setup        func(t *testing.T) fx.Option
		topicMapping map[string]string
		topic        string
	}

	testCases := []moduleTestCase{
		{
			name: "go-channels",
			setup: func(t *testing.T) fx.Option {
				return GoChannelModule()
			},
			topic: "topic",
		},
		{
			name: "kafka",
			setup: func(t *testing.T) fx.Option {
				port := createRedpandaServer(t)
				return fx.Options(
					kafkaModule("client-id", "consumer-group", fmt.Sprintf("localhost:%s", port)),
					fx.Replace(sarama.V0_11_0_0),
					ProvideSaramaOption(
						WithProducerReturnSuccess(),
						WithConsumerReturnErrors(),
						WithConsumerOffsetsInitial(sarama.OffsetOldest),
					),
				)
			},
			topic: "topic",
		},
		{
			name: "http",
			setup: func(t *testing.T) fx.Option {
				return fx.Options(
					httpModule("localhost:8888"),
				)
			},
			topicMapping: map[string]string{
				"*": "http://localhost:8888",
			},
			topic: "/",
		},
		{
			name: "nats",
			setup: func(t *testing.T) fx.Option {
				server, err := natsServer.NewServer(&natsServer.Options{
					Host:      "0.0.0.0",
					Port:      4322,
					JetStream: true,
					StoreDir:  os.TempDir(),
				})
				require.NoError(t, err)

				server.Start()
				require.Eventually(t, server.Running, 3*time.Second, 10*time.Millisecond)

				t.Cleanup(server.Shutdown)

				return fx.Options(
					NatsModule("nats://127.0.0.1:4322", "testing", nats.Name("example")),
				)
			},
			topicMapping: map[string]string{},
			topic:        "topic",
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			var (
				publisher      message.Publisher
				router         *message.Router
				messageHandled = make(chan *message.Message, 1)
			)
			options := []fx.Option{
				Module(tc.topicMapping),
				tc.setup(t),
				fx.Populate(&publisher, &router),
				fx.Supply(fx.Annotate(logging.Testing(), fx.As(new(logging.Logger)))),
				fx.Invoke(func(r *message.Router, subscriber message.Subscriber) {
					r.AddNoPublisherHandler("testing", tc.topic, subscriber, func(msg *message.Message) error {
						messageHandled <- msg
						close(messageHandled)
						return nil
					})
				}),
			}
			if !testing.Verbose() {
				options = append(options, fx.NopLogger)
			}
			app := fxtest.New(t, options...)
			app.RequireStart()
			defer func() {
				app.RequireStop()
			}()

			<-router.Running()

			tracer := otel.Tracer("main")
			ctx, span := tracer.Start(context.TODO(), "main")
			t.Cleanup(func() {
				span.End()
			})
			require.True(t, trace.SpanFromContext(ctx).SpanContext().IsValid())
			msg := NewMessage(ctx, EventMessage{})
			require.NoError(t, publisher.Publish(tc.topic, msg))

			select {
			case msg := <-messageHandled:
				span, event, err := UnmarshalMessage(msg)
				require.NoError(t, err)
				require.NotNil(t, event)
				require.NotNil(t, ctx)
				require.True(t, span.SpanContext().IsValid())
			case <-time.After(10 * time.Second):
				t.Fatal("timeout waiting message")
			}
		})
	}
}
