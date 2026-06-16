//go:build e2e && nats

package cluster

import (
	"context"
	"encoding/json"
	"math/big"
	"time"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/formancehq/ledger/v3/pkg/actions"
	"github.com/formancehq/ledger/v3/tests/e2e/testutil"
)

var _ = Describe("Events Sinks NATS", Ordered, func() {
	var (
		ctx    context.Context
		client servicepb.BucketServiceClient

		ns       *natsserver.Server
		natsConn *nats.Conn
		js       jetstream.JetStream
	)

	const (
		httpPort = 9400
		grpcPort = 8400
		topic    = "ledger-e2e-events"
	)

	BeforeAll(func() {
		// Start embedded NATS server with JetStream
		opts := &natsserver.Options{
			Host:               "127.0.0.1",
			Port:               -1, // random port
			JetStream:          true,
			StoreDir:           GinkgoT().TempDir(),
			JetStreamMaxMemory: 64 * 1024 * 1024,
			JetStreamMaxStore:  128 * 1024 * 1024,
		}

		var err error
		ns, err = natsserver.NewServer(opts)
		Expect(err).To(Succeed())

		ns.Start()
		Expect(ns.ReadyForConnections(5 * time.Second)).To(BeTrue(), "NATS server should become ready")

		// Create JetStream stream
		natsConn, err = nats.Connect(ns.ClientURL())
		Expect(err).To(Succeed())

		js, err = jetstream.New(natsConn)
		Expect(err).To(Succeed())

		_, err = js.CreateStream(context.Background(), jetstream.StreamConfig{
			Name:     "LEDGER_EVENTS",
			Subjects: []string{topic + ".>"},
		})
		Expect(err).To(Succeed())

		// Start single-node ledger server
		ctx, client, _ = testutil.SetupSingleNode(httpPort, grpcPort)
	})

	AfterAll(func() {
		if natsConn != nil {
			natsConn.Close()
		}
		if ns != nil {
			ns.Shutdown()
			ns.WaitForShutdown()
		}
	})

	It("Should deliver events to NATS when transactions are created", func() {
		// Create a JetStream consumer to receive all events
		cons, err := js.CreateConsumer(ctx, "LEDGER_EVENTS", jetstream.ConsumerConfig{
			Name:          "e2e-consumer",
			FilterSubject: topic + ".>",
			AckPolicy:     jetstream.AckExplicitPolicy,
		})
		Expect(err).To(Succeed())

		// Add NATS sink via Apply
		_, err = client.Apply(ctx, &servicepb.ApplyRequest{
			Envelopes: servicepb.UnsignedEnvelopes(
				addEventsSinkAction(&commonpb.SinkConfig{
					Name:         "nats-e2e",
					Format:       "json",
					BatchSize:    10,
					BatchDelayMs: 50,
					Type: &commonpb.SinkConfig_Nats{
						Nats: &commonpb.NatsSinkConfig{
							Url:   ns.ClientURL(),
							Topic: topic,
						},
					},
				}),
			),
		})
		Expect(err).To(Succeed())

		// Create a ledger
		_, err = client.Apply(ctx, &servicepb.ApplyRequest{
			Envelopes: servicepb.UnsignedEnvelopes(
				actions.CreateLedgerAction("nats-test", nil),
			),
		})
		Expect(err).To(Succeed())

		// Create a transaction (force=true to bypass balance checks)
		_, err = client.Apply(ctx, &servicepb.ApplyRequest{
			Envelopes: servicepb.UnsignedEnvelopes(
				actions.CreateForceTransactionAction("nats-test",
					[]*commonpb.Posting{
						actions.NewPosting("world", "bank", big.NewInt(1000), "USD"),
					},
					nil,
				),
			),
		})
		Expect(err).To(Succeed())

		// Fetch events from NATS — expect at least CREATED_LEDGER + COMMITTED_TRANSACTION
		var msgs []jetstream.Msg
		Eventually(func(g Gomega) {
			batch, err := cons.Fetch(10, jetstream.FetchMaxWait(500*time.Millisecond))
			g.Expect(err).To(Succeed())
			for msg := range batch.Messages() {
				msgs = append(msgs, msg)
			}
			g.Expect(len(msgs)).To(BeNumerically(">=", 2))
		}).Within(10 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())

		// Find CREATED_LEDGER and COMMITTED_TRANSACTION among received events
		var (
			foundCreatedLedger      bool
			foundCommittedTx        bool
		)
		for _, msg := range msgs {
			var evt map[string]any
			Expect(json.Unmarshal(msg.Data(), &evt)).To(Succeed())

			evtType, _ := evt["type"].(string)
			evtLedger, _ := evt["ledger"].(string)

			if evtType == "CREATED_LEDGER" && evtLedger == "nats-test" {
				foundCreatedLedger = true
			}

			if evtType == "COMMITTED_TRANSACTION" && evtLedger == "nats-test" {
				foundCommittedTx = true
			}
		}
		Expect(foundCreatedLedger).To(BeTrue(), "should receive CREATED_LEDGER event for nats-test")
		Expect(foundCommittedTx).To(BeTrue(), "should receive COMMITTED_TRANSACTION event for nats-test")

		// Verify sink status shows a healthy cursor
		Eventually(func(g Gomega) {
			resp, err := client.GetEventsSinks(ctx, &servicepb.GetEventsSinksRequest{})
			g.Expect(err).To(Succeed())
			g.Expect(resp.Sinks).To(HaveLen(1))
			g.Expect(resp.Sinks[0].Name).To(Equal("nats-e2e"))

			// Cursor should be > 0 indicating events were processed
			if len(resp.SinkStatuses) > 0 {
				for _, st := range resp.SinkStatuses {
					if st.SinkName == "nats-e2e" {
						g.Expect(st.Error).To(BeNil(), "sink should have no error")
					}
				}
			}
		}).Within(5 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())
	})
})
