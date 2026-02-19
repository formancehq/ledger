//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"math/big"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	chmodule "github.com/testcontainers/testcontainers-go/modules/clickhouse"
)

var _ = Describe("Events Sinks ClickHouse", Ordered, func() {
	var (
		ctx    context.Context
		client servicepb.BucketServiceClient

		chDSN string
	)

	const (
		httpPort = 9500
		grpcPort = 8500
		table    = "ledger_events"
	)

	BeforeAll(func() {
		// Start ClickHouse container
		container, err := chmodule.Run(context.Background(), "clickhouse/clickhouse-server:24-alpine")
		Expect(err).To(Succeed())

		DeferCleanup(func() {
			Expect(container.Terminate(context.Background())).To(Succeed())
		})

		chDSN, err = container.ConnectionString(context.Background())
		Expect(err).To(Succeed())

		// Start single-node ledger server
		ctx, client, _ = setupSingleNode(httpPort, grpcPort)
	})

	It("Should deliver events to ClickHouse when transactions are created", func() {
		// Add ClickHouse sink via Apply
		_, err := client.Apply(ctx, &servicepb.ApplyRequest{
			Requests: []*servicepb.Request{
				addEventsSinkAction(&commonpb.SinkConfig{
					Name:         "ch-e2e",
					Format:       "json",
					BatchSize:    10,
					BatchDelayMs: 50,
					Type: &commonpb.SinkConfig_Clickhouse{
						Clickhouse: &commonpb.ClickHouseSinkConfig{
							Dsn:   chDSN,
							Table: table,
						},
					},
				}),
			},
		})
		Expect(err).To(Succeed())

		// Create a ledger
		_, err = client.Apply(ctx, &servicepb.ApplyRequest{
			Requests: []*servicepb.Request{
				createLedgerAction("ch-test", nil),
			},
		})
		Expect(err).To(Succeed())

		// Create a transaction (force=true to bypass balance checks)
		_, err = client.Apply(ctx, &servicepb.ApplyRequest{
			Requests: []*servicepb.Request{
				createForceTransactionAction("ch-test",
					[]*commonpb.Posting{
						newPosting("world", "bank", big.NewInt(1000), "USD"),
					},
					nil,
				),
			},
		})
		Expect(err).To(Succeed())

		// Query ClickHouse and verify events arrived
		opts, chErr := clickhouse.ParseDSN(chDSN)
		Expect(chErr).To(Succeed())

		conn, chErr := clickhouse.Open(opts)
		Expect(chErr).To(Succeed())
		defer func() { _ = conn.Close() }()

		type eventRow struct {
			LogSequence uint64
			Type        string
			Ledger      string
			Date        time.Time
			Data        string
		}

		var rows []eventRow
		Eventually(func(g Gomega) {
			result, err := conn.Query(context.Background(),
				fmt.Sprintf("SELECT log_sequence, type, ledger, date, data FROM %s WHERE ledger = 'ch-test' ORDER BY log_sequence", table))
			g.Expect(err).To(Succeed())
			defer result.Close()

			rows = nil
			for result.Next() {
				var row eventRow
				g.Expect(result.Scan(&row.LogSequence, &row.Type, &row.Ledger, &row.Date, &row.Data)).To(Succeed())
				rows = append(rows, row)
			}
			g.Expect(result.Err()).To(Succeed())
			g.Expect(len(rows)).To(BeNumerically(">=", 2))
		}).Within(10 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())

		// Find CREATED_LEDGER and COMMITTED_TRANSACTION among received events
		var (
			foundCreatedLedger bool
			foundCommittedTx   bool
		)
		for _, row := range rows {
			if row.Type == "created_ledger" && row.Ledger == "ch-test" {
				foundCreatedLedger = true
			}
			if row.Type == "committed_transaction" && row.Ledger == "ch-test" {
				foundCommittedTx = true
			}
		}
		Expect(foundCreatedLedger).To(BeTrue(), "should find CREATED_LEDGER event for ch-test")
		Expect(foundCommittedTx).To(BeTrue(), "should find COMMITTED_TRANSACTION event for ch-test")

		// Verify sink status shows healthy
		Eventually(func(g Gomega) {
			resp, err := client.GetEventsSinks(ctx, &servicepb.GetEventsSinksRequest{})
			g.Expect(err).To(Succeed())
			g.Expect(resp.Sinks).To(HaveLen(1))
			g.Expect(resp.Sinks[0].Name).To(Equal("ch-e2e"))

			if len(resp.SinkStatuses) > 0 {
				for _, st := range resp.SinkStatuses {
					if st.SinkName == "ch-e2e" {
						g.Expect(st.Error).To(BeNil(), "sink should have no error")
					}
				}
			}
		}).Within(5 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())
	})
})
