package indexbuilder

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
)

func makeCreatedTxLog(seq uint64, ledger string, txID uint64, postings []*commonpb.Posting) *commonpb.Log {
	return &commonpb.Log{
		Sequence: seq,
		Payload: &commonpb.LogPayload{
			Type: &commonpb.LogPayload_Apply{
				Apply: &commonpb.ApplyLedgerLog{
					LedgerName: ledger,
					Log: &commonpb.LedgerLog{
						Id: 1,
						Data: &commonpb.LedgerLogPayload{
							Payload: &commonpb.LedgerLogPayload_CreatedTransaction{
								CreatedTransaction: &commonpb.CreatedTransaction{
									Transaction: &commonpb.Transaction{
										Id:       txID,
										Postings: postings,
									},
								},
							},
						},
					},
				},
			},
		},
	}
}

func makeRevertedTxLog(seq uint64, ledger string, revertedTxID, revertTxID uint64, postings []*commonpb.Posting) *commonpb.Log {
	return &commonpb.Log{
		Sequence: seq,
		Payload: &commonpb.LogPayload{
			Type: &commonpb.LogPayload_Apply{
				Apply: &commonpb.ApplyLedgerLog{
					LedgerName: ledger,
					Log: &commonpb.LedgerLog{
						Id: 2,
						Data: &commonpb.LedgerLogPayload{
							Payload: &commonpb.LedgerLogPayload_RevertedTransaction{
								RevertedTransaction: &commonpb.RevertedTransaction{
									RevertedTransactionId: revertedTxID,
									RevertTransaction: &commonpb.Transaction{
										Id:       revertTxID,
										Postings: postings,
									},
								},
							},
						},
					},
				},
			},
		},
	}
}

func TestParsePostingsFromLog_CreatedTransaction(t *testing.T) {
	t.Parallel()

	log := makeCreatedTxLog(42, "default", 100, []*commonpb.Posting{
		{Source: "users:alice", Destination: "orders:1234", Amount: &commonpb.Uint256{V0: 1000}, Asset: "USD"},
		{Source: "orders:1234", Destination: "merchants:bob", Amount: &commonpb.Uint256{V0: 900}, Asset: "USD"},
	})

	data, err := log.MarshalVT()
	require.NoError(t, err)

	var parsed parsedLog
	require.NoError(t, parsePostingsFromLog(data, &parsed))

	require.Equal(t, uint64(42), parsed.Sequence)
	require.Equal(t, "default", parsed.Ledger)
	require.Equal(t, uint64(100), parsed.TxID)
	require.Equal(t, int32(1), parsed.LogType)
	require.Len(t, parsed.Postings, 2)
	require.Equal(t, "users:alice", parsed.Postings[0].Source)
	require.Equal(t, "orders:1234", parsed.Postings[0].Destination)
	require.Equal(t, "orders:1234", parsed.Postings[1].Source)
	require.Equal(t, "merchants:bob", parsed.Postings[1].Destination)
}

func TestParsePostingsFromLog_RevertedTransaction(t *testing.T) {
	t.Parallel()

	log := makeRevertedTxLog(55, "prod", 10, 200, []*commonpb.Posting{
		{Source: "merchants:bob", Destination: "users:alice", Amount: &commonpb.Uint256{V0: 1000}, Asset: "USD"},
	})

	data, err := log.MarshalVT()
	require.NoError(t, err)

	var parsed parsedLog
	require.NoError(t, parsePostingsFromLog(data, &parsed))

	require.Equal(t, uint64(55), parsed.Sequence)
	require.Equal(t, "prod", parsed.Ledger)
	require.Equal(t, uint64(200), parsed.TxID)
	require.Equal(t, int32(2), parsed.LogType)
	require.Len(t, parsed.Postings, 1)
	require.Equal(t, "merchants:bob", parsed.Postings[0].Source)
	require.Equal(t, "users:alice", parsed.Postings[0].Destination)
}

func TestParsePostingsFromLog_NonDataLog(t *testing.T) {
	t.Parallel()

	// CreateIndex is a config mutation — should be skipped (LogType=0).
	log := &commonpb.Log{
		Sequence: 10,
		Payload: &commonpb.LogPayload{
			Type: &commonpb.LogPayload_Apply{
				Apply: &commonpb.ApplyLedgerLog{
					LedgerName: "default",
					Log: &commonpb.LedgerLog{
						Id: 1,
						Data: &commonpb.LedgerLogPayload{
							Payload: &commonpb.LedgerLogPayload_CreateIndex{
								CreateIndex: &commonpb.CreatedIndexLog{},
							},
						},
					},
				},
			},
		},
	}

	data, err := log.MarshalVT()
	require.NoError(t, err)

	var parsed parsedLog
	require.NoError(t, parsePostingsFromLog(data, &parsed))

	require.Equal(t, uint64(10), parsed.Sequence)
	require.Equal(t, int32(0), parsed.LogType)
	require.Empty(t, parsed.Postings)
}

func TestParsePostingsFromLog_NonApplyLog(t *testing.T) {
	t.Parallel()

	// CreateLedger is a non-apply log type.
	log := &commonpb.Log{
		Sequence: 5,
		Payload: &commonpb.LogPayload{
			Type: &commonpb.LogPayload_CreateLedger{
				CreateLedger: &commonpb.CreatedLedgerLog{},
			},
		},
	}

	data, err := log.MarshalVT()
	require.NoError(t, err)

	var parsed parsedLog
	require.NoError(t, parsePostingsFromLog(data, &parsed))

	require.Equal(t, uint64(5), parsed.Sequence)
	require.Equal(t, int32(0), parsed.LogType)
	require.Empty(t, parsed.Postings)
}

func TestParsePostingsFromLog_EmptyPostings(t *testing.T) {
	t.Parallel()

	log := makeCreatedTxLog(99, "empty", 42, nil)

	data, err := log.MarshalVT()
	require.NoError(t, err)

	var parsed parsedLog
	require.NoError(t, parsePostingsFromLog(data, &parsed))

	require.Equal(t, uint64(99), parsed.Sequence)
	require.Equal(t, "empty", parsed.Ledger)
	require.Equal(t, uint64(42), parsed.TxID)
	require.Equal(t, int32(1), parsed.LogType)
	require.Empty(t, parsed.Postings)
}

func TestParsePostingsFromLog_SliceReuse(t *testing.T) {
	t.Parallel()

	log1 := makeCreatedTxLog(1, "test", 1, []*commonpb.Posting{
		{Source: "a", Destination: "b", Amount: &commonpb.Uint256{V0: 100}, Asset: "USD"},
		{Source: "c", Destination: "d", Amount: &commonpb.Uint256{V0: 200}, Asset: "EUR"},
	})

	data1, err := log1.MarshalVT()
	require.NoError(t, err)

	var parsed parsedLog
	require.NoError(t, parsePostingsFromLog(data1, &parsed))
	require.Len(t, parsed.Postings, 2)

	// Second parse with fewer postings should still work and reuse the backing array.
	log2 := makeCreatedTxLog(2, "test", 2, []*commonpb.Posting{
		{Source: "x", Destination: "y", Amount: &commonpb.Uint256{V0: 300}, Asset: "GBP"},
	})

	data2, err := log2.MarshalVT()
	require.NoError(t, err)

	require.NoError(t, parsePostingsFromLog(data2, &parsed))
	require.Len(t, parsed.Postings, 1)
	require.Equal(t, "x", parsed.Postings[0].Source)
	require.Equal(t, "y", parsed.Postings[0].Destination)
}

func BenchmarkParsePostings(b *testing.B) {
	postings := []*commonpb.Posting{
		{Source: "users:alice", Destination: "orders:1234", Amount: &commonpb.Uint256{V0: 1000}, Asset: "USD/2"},
		{Source: "orders:1234", Destination: "merchants:bob", Amount: &commonpb.Uint256{V0: 900}, Asset: "USD/2"},
		{Source: "merchants:bob", Destination: "fees:platform", Amount: &commonpb.Uint256{V0: 100}, Asset: "USD/2"},
	}

	log := makeCreatedTxLog(1, "default", 42, postings)

	data, err := log.MarshalVT()
	require.NoError(b, err)

	b.Run("protowire", func(b *testing.B) {
		var parsed parsedLog

		b.ReportAllocs()
		b.ResetTimer()

		for range b.N {
			_ = parsePostingsFromLog(data, &parsed)
		}
	})

	b.Run("UnmarshalVT+reset", func(b *testing.B) {
		msg := &commonpb.Log{}

		b.ReportAllocs()
		b.ResetTimer()

		for range b.N {
			resetLogForReuse(msg)

			_ = msg.UnmarshalVT(data)
		}
	})
}
