package events_test

import (
	"context"
	"encoding/hex"
	"fmt"
	"math/big"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/formancehq/go-libs/v3/logging"
	libtime "github.com/formancehq/go-libs/v3/time"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/eventspb"
	"github.com/formancehq/ledger-v3-poc/internal/service/events"
	"github.com/stretchr/testify/require"
	chmodule "github.com/testcontainers/testcontainers-go/modules/clickhouse"
	"google.golang.org/protobuf/encoding/protojson"
)

// startTestClickHouse starts a ClickHouse container via testcontainers and returns the DSN.
func startTestClickHouse(t *testing.T) string {
	t.Helper()
	ctx := context.Background()

	container, err := chmodule.Run(ctx, "clickhouse/clickhouse-server:24-alpine")
	require.NoError(t, err)

	t.Cleanup(func() {
		require.NoError(t, container.Terminate(context.Background()))
	})

	dsn, err := container.ConnectionString(ctx)
	require.NoError(t, err)

	return dsn
}

// queryClickHouseEvents queries all rows from the given ClickHouse table.
func queryClickHouseEvents(t *testing.T, dsn, table string) []struct {
	LogSequence uint64
	Type        string
	Ledger      string
	Date        time.Time
	Data        string
} {
	t.Helper()
	ctx := context.Background()

	opts, err := clickhouse.ParseDSN(dsn)
	require.NoError(t, err)

	conn, err := clickhouse.Open(opts)
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	rows, err := conn.Query(ctx, fmt.Sprintf("SELECT log_sequence, type, ledger, date, data FROM %s ORDER BY log_sequence", table))
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

	dsn := startTestClickHouse(t)
	const table = "ledger_events"

	// Set up emitter with real ClickHouseSink
	store := newTestStore(t)
	proposer := &directProposer{store: store}
	logger := logging.Testing()

	registerLedger(t, store, "orders", 1)
	now := libtime.Now()

	appendTestLogs(t, store,
		&commonpb.Log{
			Sequence: 1,
			Payload: &commonpb.LogPayload{
				Type: &commonpb.LogPayload_CreateLedger{
					CreateLedger: &commonpb.CreateLedgerLog{
						Info: &commonpb.LedgerInfo{Name: "orders", CreatedAt: commonpb.NewTimestamp(now), Id: 1},
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
		DSN:    dsn,
		Table:  table,
		Format: events.FormatJSON,
	})
	require.NoError(t, err)
	defer func() { _ = sink.Close() }()

	cfg := events.DefaultEmitterConfig()
	cfg.BatchSize = 10
	emitter := events.NewEmitter(store, sink, "ch-sink", proposer, logger, cfg)
	emitter.Start()

	// Wait for cursor to advance
	require.Eventually(t, func() bool {
		cursor, err := store.GetSinkCursor("ch-sink")
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

	// Verify JSON data can be deserialized
	var evt1 eventspb.Event
	require.NoError(t, protojson.Unmarshal([]byte(rows[0].Data), &evt1))
	require.Equal(t, eventspb.EventType_CREATED_LEDGER, evt1.Type)

	// Verify COMMITTED_TRANSACTION event
	require.Equal(t, uint64(2), rows[1].LogSequence)
	require.Equal(t, "committed_transaction", rows[1].Type)
	require.Equal(t, "orders", rows[1].Ledger)

	var evt2 eventspb.Event
	require.NoError(t, protojson.Unmarshal([]byte(rows[1].Data), &evt2))
	require.Equal(t, eventspb.EventType_COMMITTED_TRANSACTION, evt2.Type)
}

func TestClickHouseSinkIntegration_ProtobufFormat(t *testing.T) {
	t.Parallel()

	dsn := startTestClickHouse(t)
	const table = "ledger_events_proto"

	store := newTestStore(t)
	proposer := &directProposer{store: store}
	logger := logging.Testing()

	registerLedger(t, store, "payments", 1)
	now := libtime.Now()

	appendTestLogs(t, store,
		&commonpb.Log{
			Sequence: 1,
			Payload: &commonpb.LogPayload{
				Type: &commonpb.LogPayload_Apply{
					Apply: &commonpb.ApplyLedgerLog{
						LedgerName: "payments",
						Log: commonpb.NewLedgerLog(&commonpb.LedgerLogPayload{
							Payload: &commonpb.LedgerLogPayload_CreatedTransaction{
								CreatedTransaction: &commonpb.CreatedTransaction{
									Transaction: commonpb.NewTransaction().
										WithPostings(commonpb.NewPosting("world", "merchant", "EUR", big.NewInt(500))).
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
		DSN:    dsn,
		Table:  table,
		Format: events.FormatProto,
	})
	require.NoError(t, err)
	defer func() { _ = sink.Close() }()

	cfg := events.DefaultEmitterConfig()
	cfg.BatchSize = 10
	emitter := events.NewEmitter(store, sink, "proto-sink", proposer, logger, cfg)
	emitter.Start()

	require.Eventually(t, func() bool {
		cursor, err := store.GetSinkCursor("proto-sink")
		return err == nil && cursor >= 1
	}, 10*time.Second, 10*time.Millisecond, "emitter should process log")

	emitter.Stop()

	rows := queryClickHouseEvents(t, dsn, table)
	require.Len(t, rows, 1)

	require.Equal(t, "committed_transaction", rows[0].Type)
	require.Equal(t, "payments", rows[0].Ledger)

	// Verify hex-encoded protobuf data can be round-tripped
	raw, err := hex.DecodeString(rows[0].Data)
	require.NoError(t, err)

	var evt eventspb.Event
	require.NoError(t, evt.UnmarshalVT(raw))
	require.Equal(t, eventspb.EventType_COMMITTED_TRANSACTION, evt.Type)
	require.Equal(t, "payments", evt.Ledger)
	require.Equal(t, uint64(1), evt.LogSequence)
	require.NotNil(t, evt.Log, "event should carry the full Log")
}

func TestClickHouseSinkIntegration_AutoCreateTable(t *testing.T) {
	t.Parallel()

	dsn := startTestClickHouse(t)
	const table = "auto_created_table"

	// Creating a new sink should auto-create the table
	sink, err := events.NewClickHouseSink(context.Background(), events.ClickHouseSinkConfig{
		DSN:    dsn,
		Table:  table,
		Format: events.FormatJSON,
	})
	require.NoError(t, err)
	defer func() { _ = sink.Close() }()

	// Verify the table exists and is empty
	rows := queryClickHouseEvents(t, dsn, table)
	require.Empty(t, rows)
}
