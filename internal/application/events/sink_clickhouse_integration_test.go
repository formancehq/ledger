package events_test

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/formancehq/go-libs/v3/logging"
	libtime "github.com/formancehq/go-libs/v3/time"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/application/events"
	"github.com/formancehq/ledger-v3-poc/internal/query"
	"github.com/stretchr/testify/require"
)

// openTestClickHouseConn opens a ClickHouse connection with the required settings
// for querying the structured JSON column.
func openTestClickHouseConn(t *testing.T, dsn string) clickhouse.Conn {
	t.Helper()

	opts, err := clickhouse.ParseDSN(dsn)
	require.NoError(t, err)

	if opts.Settings == nil {
		opts.Settings = make(clickhouse.Settings)
	}
	opts.Settings["allow_experimental_json_type"] = true
	opts.Settings["allow_experimental_variant_type"] = true
	opts.Settings["output_format_json_quote_64bit_integers"] = false

	conn, err := clickhouse.Open(opts)
	require.NoError(t, err)

	t.Cleanup(func() { _ = conn.Close() })

	return conn
}

// queryClickHouseEvents queries all rows from the given ClickHouse table.
// The JSON column is read as a string via toJSONString().
func queryClickHouseEvents(t *testing.T, dsn, table string) []struct {
	LogSequence uint64
	Type        string
	Ledger      string
	Date        time.Time
	Data        string
} {
	t.Helper()
	ctx := context.Background()

	conn := openTestClickHouseConn(t, dsn)

	rows, err := conn.Query(ctx, fmt.Sprintf(
		"SELECT log_sequence, type, ledger, date, toJSONString(data) FROM %s ORDER BY log_sequence", table))
	require.NoError(t, err)
	defer func() { _ = rows.Close() }()

	var results []struct {
		LogSequence uint64
		Type        string
		Ledger      string
		Date        time.Time
		Data        string
	}

	for rows.Next() {
		var row struct {
			LogSequence uint64
			Type        string
			Ledger      string
			Date        time.Time
			Data        string
		}
		require.NoError(t, rows.Scan(&row.LogSequence, &row.Type, &row.Ledger, &row.Date, &row.Data))
		results = append(results, row)
	}
	require.NoError(t, rows.Err())

	return results
}

func TestClickHouseSinkIntegration_PublishAndConsume(t *testing.T) {
	t.Parallel()

	dsn := sharedClickHouseDSN
	table := uniqueTopic("ledger_events")

	store := newTestStore(t)
	proposer := &directProposer{store: store}
	logger := logging.Testing()

	registerLedger(t, store, "orders")
	now := libtime.Now()

	appendTestLogs(t, store,
		&commonpb.Log{
			Sequence: 1,
			Payload: &commonpb.LogPayload{
				Type: &commonpb.LogPayload_CreateLedger{
					CreateLedger: &commonpb.CreateLedgerLog{
						Info: &commonpb.LedgerInfo{Name: "orders", CreatedAt: commonpb.NewTimestamp(now)},
					},
				},
			},
		},
		&commonpb.Log{
			Sequence: 2,
			Payload: &commonpb.LogPayload{
				Type: &commonpb.LogPayload_Apply{
					Apply: &commonpb.ApplyLedgerLog{
						LedgerName: "orders",
						Log: commonpb.NewLedgerLog(&commonpb.LedgerLogPayload{
							Payload: &commonpb.LedgerLogPayload_CreatedTransaction{
								CreatedTransaction: &commonpb.CreatedTransaction{
									Transaction: commonpb.NewTransaction().
										WithPostings(commonpb.NewPosting("world", "bank", "USD", big.NewInt(1000))).
										WithID(1).WithTimestamp(now),
								},
							},
						}).WithID(1).WithDate(now),
					},
				},
			},
		},
	)

	sink, err := events.NewClickHouseSink(context.Background(), events.ClickHouseSinkConfig{
		DSN:   dsn,
		Table: table,
	})
	require.NoError(t, err)
	defer func() { _ = sink.Close() }()

	cfg := events.DefaultEmitterConfig()
	cfg.BatchSize = 10
	emitter := events.NewEmitter(store, sink, "ch-sink", proposer, logger, cfg)
	emitter.Start()

	require.Eventually(t, func() bool {
		cursor, err := query.ReadSinkCursor(store,"ch-sink")
		return err == nil && cursor >= 2
	}, 10*time.Second, 10*time.Millisecond, "emitter should process all logs")

	emitter.Stop()

	// Query ClickHouse and verify rows
	rows := queryClickHouseEvents(t, dsn, table)
	require.Len(t, rows, 2)

	// Verify CREATED_LEDGER event
	require.Equal(t, uint64(1), rows[0].LogSequence)
	require.Equal(t, "created_ledger", rows[0].Type)
	require.Equal(t, "orders", rows[0].Ledger)

	// Verify JSON data contains structured fields
	var evt1 map[string]any
	require.NoError(t, json.Unmarshal([]byte(rows[0].Data), &evt1))
	require.Equal(t, "orders", evt1["ledgerName"])

	// Verify COMMITTED_TRANSACTION event
	require.Equal(t, uint64(2), rows[1].LogSequence)
	require.Equal(t, "committed_transaction", rows[1].Type)
	require.Equal(t, "orders", rows[1].Ledger)

	var evt2 map[string]any
	require.NoError(t, json.Unmarshal([]byte(rows[1].Data), &evt2))
	tx, ok := evt2["transaction"].(map[string]any)
	require.True(t, ok, "data should contain transaction object")
	require.EqualValues(t, 1, tx["id"])

	postings, ok := tx["postings"].([]any)
	require.True(t, ok, "transaction should contain postings array")
	require.Len(t, postings, 1)

	posting := postings[0].(map[string]any)
	require.Equal(t, "world", posting["source"])
	require.Equal(t, "bank", posting["destination"])
	require.Equal(t, "USD", posting["asset"])
}

func TestClickHouseSinkIntegration_TypedSubColumnQueries(t *testing.T) {
	t.Parallel()

	dsn := sharedClickHouseDSN
	table := uniqueTopic("ledger_events_typed")

	store := newTestStore(t)
	proposer := &directProposer{store: store}
	logger := logging.Testing()

	registerLedger(t, store, "analytics")
	now := libtime.Now()

	appendTestLogs(t, store,
		&commonpb.Log{
			Sequence: 1,
			Payload: &commonpb.LogPayload{
				Type: &commonpb.LogPayload_CreateLedger{
					CreateLedger: &commonpb.CreateLedgerLog{
						Info: &commonpb.LedgerInfo{Name: "analytics", CreatedAt: commonpb.NewTimestamp(now)},
					},
				},
			},
		},
		&commonpb.Log{
			Sequence: 2,
			Payload: &commonpb.LogPayload{
				Type: &commonpb.LogPayload_Apply{
					Apply: &commonpb.ApplyLedgerLog{
						LedgerName: "analytics",
						Log: commonpb.NewLedgerLog(&commonpb.LedgerLogPayload{
							Payload: &commonpb.LedgerLogPayload_CreatedTransaction{
								CreatedTransaction: &commonpb.CreatedTransaction{
									Transaction: commonpb.NewTransaction().
										WithPostings(commonpb.NewPosting("world", "merchant", "EUR", big.NewInt(4200))).
										WithID(1).WithTimestamp(now),
								},
							},
						}).WithID(1).WithDate(now),
					},
				},
			},
		},
	)

	sink, err := events.NewClickHouseSink(context.Background(), events.ClickHouseSinkConfig{
		DSN:   dsn,
		Table: table,
	})
	require.NoError(t, err)
	defer func() { _ = sink.Close() }()

	cfg := events.DefaultEmitterConfig()
	cfg.BatchSize = 10
	emitter := events.NewEmitter(store, sink, "typed-sub", proposer, logger, cfg)
	emitter.Start()

	require.Eventually(t, func() bool {
		cursor, err := query.ReadSinkCursor(store,"typed-sub")
		return err == nil && cursor >= 2
	}, 10*time.Second, 10*time.Millisecond, "emitter should process logs")

	emitter.Stop()

	ctx := context.Background()
	conn := openTestClickHouseConn(t, dsn)

	// Query typed sub-columns: ledgerName for CREATED_LEDGER event
	var ledgerName string
	row := conn.QueryRow(ctx, fmt.Sprintf(
		"SELECT data.ledgerName FROM %s WHERE type = 'created_ledger' LIMIT 1", table))
	require.NoError(t, row.Scan(&ledgerName))
	require.Equal(t, "analytics", ledgerName)

	// Query transaction sub-columns: transaction.id (UInt64) and posting details
	var txID uint64
	row = conn.QueryRow(ctx, fmt.Sprintf(
		"SELECT data.transaction.id FROM %s WHERE type = 'committed_transaction' LIMIT 1", table))
	require.NoError(t, row.Scan(&txID))
	require.EqualValues(t, 1, txID)

	// Query posting source/destination via array sub-column access
	// ClickHouse JSON array syntax: data.transaction.postings.source[1]
	// (access sub-column first, then index into the array)
	var (
		postingSource string
		postingDest   string
		postingAsset  string
	)
	row = conn.QueryRow(ctx, fmt.Sprintf(
		"SELECT data.transaction.postings.source[1], data.transaction.postings.destination[1], data.transaction.postings.asset[1] FROM %s WHERE type = 'committed_transaction' LIMIT 1", table))
	require.NoError(t, row.Scan(&postingSource, &postingDest, &postingAsset))
	require.Equal(t, "world", postingSource)
	require.Equal(t, "merchant", postingDest)
	require.Equal(t, "EUR", postingAsset)
}

func TestClickHouseSinkIntegration_AutoCreateTable(t *testing.T) {
	t.Parallel()

	dsn := sharedClickHouseDSN
	table := uniqueTopic("auto_created_table")

	sink, err := events.NewClickHouseSink(context.Background(), events.ClickHouseSinkConfig{
		DSN:   dsn,
		Table: table,
	})
	require.NoError(t, err)
	defer func() { _ = sink.Close() }()

	// Verify the table exists and is empty
	rows := queryClickHouseEvents(t, dsn, table)
	require.Empty(t, rows)
}
