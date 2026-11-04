//go:build clickhouse

package events

import (
	"context"
	"encoding/json"
	"math/big"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/eventspb"
)

// The native driver interfaces are not mockgen-managed. Capture the actual
// Publish arguments at that external boundary; this does not emulate a server.
type amountBatch struct {
	driver.Batch

	rows [][]any
	sent bool
}

func (b *amountBatch) Append(values ...any) error {
	b.rows = append(b.rows, values)

	return nil
}

func (b *amountBatch) Send() error {
	b.sent = true

	return nil
}

type amountConn struct {
	driver.Conn

	batch *amountBatch
}

func (c *amountConn) PrepareBatch(context.Context, string, ...driver.PrepareBatchOption) (driver.Batch, error) {
	return c.batch, nil
}

func TestClickHousePublishExactAmounts(t *testing.T) {
	t.Parallel()
	for _, amount := range []string{
		"0", "9007199254740993", "18446744073709551615", "18446744073709551617",
		"115792089237316195423570985008687907853269984665640564039457584007913129639935",
	} {
		for _, reverted := range []bool{false, true} {
			name := "committed/" + amount
			key := "transaction"
			if reverted {
				name = "reverted/" + amount
				key = "revertTransaction"
			}
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				value, ok := new(big.Int).SetString(amount, 10)
				require.True(t, ok)
				tx := commonpb.NewTransaction().WithID(18446744073709551615).
					WithPostings(commonpb.NewPosting("world", "users:001", "USD/2", value))
				tx.Postings[0].Color = "reserved"
				tx.Metadata = map[string]*commonpb.MetadataValue{"amount": commonpb.NewStringValue(amount)}
				payload := &commonpb.LedgerLogPayload{Payload: &commonpb.LedgerLogPayload_CreatedTransaction{
					CreatedTransaction: &commonpb.CreatedTransaction{Transaction: tx,
						AccountMetadata: map[string]*commonpb.MetadataMap{"users:001": {Values: map[string]*commonpb.MetadataValue{"name": commonpb.NewStringValue("Alice")}}}},
				}}
				eventType := commonpb.EventType_COMMITTED_TRANSACTION
				if reverted {
					payload.Payload = &commonpb.LedgerLogPayload_RevertedTransaction{RevertedTransaction: &commonpb.RevertedTransaction{
						RevertedTransactionId: 18446744073709551615, RevertTransaction: tx,
					}}
					eventType = commonpb.EventType_REVERTED_TRANSACTION
				}
				event := &eventspb.Event{Type: eventType, Ledger: "orders", LogSequence: 42,
					Date: &commonpb.Timestamp{Data: 1700000100},
					Log: &commonpb.Log{Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_Apply{
						Apply: &commonpb.ApplyLedgerLog{Log: &commonpb.LedgerLog{Data: payload}},
					}}},
				}
				before, err := event.MarshalVT()
				require.NoError(t, err)
				shared, err := eventToSinkJSON(event)
				require.NoError(t, err)
				batch := &amountBatch{}
				sink := &ClickHouseSink{conn: &amountConn{batch: batch}, table: "ledger_events"}
				require.NoError(t, sink.Publish(t.Context(), []*eventspb.Event{event}))
				require.True(t, batch.sent)
				require.Len(t, batch.rows, 1)
				require.Equal(t, uint64(42), batch.rows[0][0])
				require.Equal(t, "orders", batch.rows[0][2])
				var actual, expected map[string]json.RawMessage
				require.NoError(t, json.Unmarshal([]byte(batch.rows[0][4].(string)), &actual))
				require.NoError(t, json.Unmarshal(shared, &expected))
				var transaction map[string]json.RawMessage
				require.NoError(t, json.Unmarshal(expected[key], &transaction))
				var postings []map[string]json.RawMessage
				require.NoError(t, json.Unmarshal(transaction["postings"], &postings))
				require.Equal(t, amount, string(postings[0]["amount"]), "shared analytical format must keep numeric amounts")
				postings[0]["amount"] = json.RawMessage(`"` + amount + `"`)
				transaction["postings"], err = json.Marshal(postings)
				require.NoError(t, err)
				expected[key], err = json.Marshal(transaction)
				require.NoError(t, err)
				want, err := json.Marshal(expected)
				require.NoError(t, err)
				got, err := json.Marshal(actual)
				require.NoError(t, err)
				require.JSONEq(t, string(want), string(got))
				// JSONEq decodes numbers through float64; assert the sensitive tokens
				// separately so an ID or amount rounded above 2^53 cannot pass.
				require.NoError(t, json.Unmarshal(actual[key], &transaction))
				require.Equal(t, "18446744073709551615", string(transaction["id"]))
				require.NoError(t, json.Unmarshal(transaction["postings"], &postings))
				require.Equal(t, `"`+amount+`"`, string(postings[0]["amount"]))
				if reverted {
					require.Equal(t, "18446744073709551615", string(actual["revertedTransactionId"]))
				}
				after, err := event.MarshalVT()
				require.NoError(t, err)
				require.Equal(t, before, after, "transport conversion must not mutate its source event")
			})
		}
	}
}

func TestClickHousePublishWithoutPostings(t *testing.T) {
	t.Parallel()
	for _, payload := range []*commonpb.LogPayload{
		nil,
		{Type: &commonpb.LogPayload_CreateLedger{CreateLedger: &commonpb.CreatedLedgerLog{Name: "orders"}}},
		{Type: &commonpb.LogPayload_Apply{Apply: &commonpb.ApplyLedgerLog{Log: &commonpb.LedgerLog{
			Data: &commonpb.LedgerLogPayload{Payload: &commonpb.LedgerLogPayload_CreatedTransaction{
				CreatedTransaction: &commonpb.CreatedTransaction{Transaction: commonpb.NewTransaction()},
			}},
		}}}},
	} {
		event := &eventspb.Event{Date: &commonpb.Timestamp{Data: 1700000100}, Log: &commonpb.Log{Payload: payload}}
		shared, err := eventToSinkJSON(event)
		require.NoError(t, err)
		batch := &amountBatch{}
		sink := &ClickHouseSink{conn: &amountConn{batch: batch}, table: "ledger_events"}
		require.NoError(t, sink.Publish(t.Context(), []*eventspb.Event{event}))
		require.Len(t, batch.rows, 1)
		require.JSONEq(t, string(shared), batch.rows[0][4].(string))
	}
}
