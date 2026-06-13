//go:build e2e && databricks

package cluster

import (
	"context"
	"database/sql"
	"fmt"
	"math/big"
	"os"
	"time"

	_ "github.com/databricks/databricks-sql-go"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/formancehq/ledger/v3/pkg/actions"
	"github.com/formancehq/ledger/v3/tests/e2e/testutil"
)

var _ = Describe("Events Sinks Databricks", Ordered, func() {
	var (
		ctx    context.Context
		client servicepb.BucketServiceClient

		dbHost     string
		dbHTTPPath string
		dbToken    string
		dbCatalog  string
		dbSchema   string
	)

	const (
		httpPort = 9600
		grpcPort = 8600
		table    = "ledger_events_e2e"
	)

	BeforeAll(func() {
		dbHost = os.Getenv("DATABRICKS_HOST")
		dbHTTPPath = os.Getenv("DATABRICKS_HTTP_PATH")
		dbToken = os.Getenv("DATABRICKS_TOKEN")
		dbCatalog = os.Getenv("DATABRICKS_CATALOG")
		dbSchema = os.Getenv("DATABRICKS_SCHEMA")

		if dbHost == "" || dbHTTPPath == "" || dbToken == "" || dbCatalog == "" || dbSchema == "" {
			Skip("Databricks e2e test requires DATABRICKS_HOST, DATABRICKS_HTTP_PATH, DATABRICKS_TOKEN, DATABRICKS_CATALOG, and DATABRICKS_SCHEMA environment variables")
		}

		// Start single-node ledger server
		ctx, client, _ = testutil.SetupSingleNode(httpPort, grpcPort)
	})

	AfterAll(func() {
		// Clean up test table
		if dbHost == "" {
			return
		}

		dsn := fmt.Sprintf("token:%s@%s:443%s?catalog=%s&schema=%s",
			dbToken, dbHost, dbHTTPPath, dbCatalog, dbSchema,
		)

		db, err := sql.Open("databricks", dsn)
		if err != nil {
			return
		}
		defer func() { _ = db.Close() }()

		qualifiedTable := fmt.Sprintf("%s.%s.%s", dbCatalog, dbSchema, table)
		_, _ = db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", qualifiedTable))
	})

	It("Should deliver events to Databricks when transactions are created", func() {
		// Add Databricks sink via Apply
		_, err := client.Apply(ctx, &servicepb.ApplyRequest{
			Requests: []*servicepb.Request{
				addEventsSinkAction(&commonpb.SinkConfig{
					Name:         "databricks-e2e",
					BatchSize:    10,
					BatchDelayMs: 500,
					Type: &commonpb.SinkConfig_Databricks{
						Databricks: &commonpb.DatabricksSinkConfig{
							ServerHostname: dbHost,
							HttpPath:       dbHTTPPath,
							Auth:           &commonpb.DatabricksSinkConfig_Token{Token: dbToken},
							Catalog:        dbCatalog,
							Schema:         dbSchema,
							Table:          table,
							Port:           443,
						},
					},
				}),
			},
		})
		Expect(err).To(Succeed())

		// Create a ledger
		_, err = client.Apply(ctx, &servicepb.ApplyRequest{
			Requests: []*servicepb.Request{
				actions.CreateLedgerAction("db-test", nil),
			},
		})
		Expect(err).To(Succeed())

		// Create a transaction (force=true to bypass balance checks)
		_, err = client.Apply(ctx, &servicepb.ApplyRequest{
			Requests: []*servicepb.Request{
				actions.CreateForceTransactionAction("db-test",
					[]*commonpb.Posting{
						actions.NewPosting("world", "bank", big.NewInt(1000), "USD"),
					},
					nil,
				),
			},
		})
		Expect(err).To(Succeed())

		// Query Databricks and verify events arrived
		dsn := fmt.Sprintf("token:%s@%s:443%s?catalog=%s&schema=%s",
			dbToken, dbHost, dbHTTPPath, dbCatalog, dbSchema,
		)

		db, dbErr := sql.Open("databricks", dsn)
		Expect(dbErr).To(Succeed())
		defer func() { _ = db.Close() }()

		qualifiedTable := fmt.Sprintf("%s.%s.%s", dbCatalog, dbSchema, table)

		type eventRow struct {
			LogSequence int64
			Type        string
			Ledger      string
			Date        time.Time
			Data        string
		}

		var rows []eventRow
		Eventually(func(g Gomega) {
			result, err := db.QueryContext(context.Background(),
				fmt.Sprintf("SELECT log_sequence, type, ledger, date, data FROM %s WHERE ledger = 'db-test' ORDER BY log_sequence", qualifiedTable))
			if err != nil {
				g.Expect(err).To(Succeed())
				return
			}
			defer func() { _ = result.Close() }()

			rows = nil
			for result.Next() {
				var row eventRow
				g.Expect(result.Scan(&row.LogSequence, &row.Type, &row.Ledger, &row.Date, &row.Data)).To(Succeed())
				rows = append(rows, row)
			}
			g.Expect(result.Err()).To(Succeed())
			g.Expect(len(rows)).To(BeNumerically(">=", 2))
		}).Within(30 * time.Second).ProbeEvery(2 * time.Second).Should(Succeed())

		// Find CREATED_LEDGER and COMMITTED_TRANSACTION among received events
		var (
			foundCreatedLedger bool
			foundCommittedTx   bool
		)
		for _, row := range rows {
			if row.Type == "created_ledger" && row.Ledger == "db-test" {
				foundCreatedLedger = true
			}
			if row.Type == "committed_transaction" && row.Ledger == "db-test" {
				foundCommittedTx = true
			}
		}
		Expect(foundCreatedLedger).To(BeTrue(), "should find CREATED_LEDGER event for db-test")
		Expect(foundCommittedTx).To(BeTrue(), "should find COMMITTED_TRANSACTION event for db-test")

		// Verify sink status shows healthy
		Eventually(func(g Gomega) {
			resp, err := client.GetEventsSinks(ctx, &servicepb.GetEventsSinksRequest{})
			g.Expect(err).To(Succeed())
			g.Expect(resp.Sinks).To(HaveLen(1))
			g.Expect(resp.Sinks[0].Name).To(Equal("databricks-e2e"))

			if len(resp.SinkStatuses) > 0 {
				for _, st := range resp.SinkStatuses {
					if st.SinkName == "databricks-e2e" {
						g.Expect(st.Error).To(BeNil(), "sink should have no error")
					}
				}
			}
		}).Within(10 * time.Second).ProbeEvery(1 * time.Second).Should(Succeed())
	})
})
