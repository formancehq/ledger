package main

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/tests/oracle"
	"github.com/formancehq/ledger/v3/tests/oracle/oracletest"
)

// applyLog builds a response log carrying the global sequence seq and a
// ledger-local Apply log id.
func applyLog(seq, id uint64, ledger string) *commonpb.Log {
	return &commonpb.Log{
		Sequence: seq,
		Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_Apply{
			Apply: &commonpb.ApplyLedgerLog{
				LedgerName: ledger,
				Log:        &commonpb.LedgerLog{Id: id},
			},
		}},
	}
}

// topLevelLog builds a response log with no Apply arm — the shape ledger
// metadata takes, which carries a sequence but no ledger-local id.
func topLevelLog(seq uint64) *commonpb.Log {
	return &commonpb.Log{
		Sequence: seq,
		Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_SavedLedgerMetadata{
			SavedLedgerMetadata: &commonpb.SavedLedgerMetadataLog{},
		}},
	}
}

func newLearnChecker() *Checker {
	return &Checker{logBySeq: map[uint64]learnedLog{}}
}

func TestLearnLogSequences(t *testing.T) {
	t.Parallel()

	c := newLearnChecker()
	bulk := oracle.Bulk{Requests: []*servicepb.Request{
		oracletest.TxReq("world", "acc:a", "USD", 1),
		oracletest.TxReq("world", "acc:b", "USD", 1),
	}}

	c.learnLogSequences(bulk, []*commonpb.Log{
		applyLog(41, 7, "L"),
		applyLog(42, 8, "L"),
	})

	require.Equal(t, uint64(42), c.maxLogSeq)
	require.Len(t, c.logBySeq, 2)
	require.Equal(t, learnedLog{ledger: "L", id: 7}, c.logBySeq[41])
	require.Equal(t, learnedLog{ledger: "L", id: 8}, c.logBySeq[42])
}

// A top-level log takes a global sequence but has no ledger-local id, so it
// raises the high-water mark and is not learnable.
func TestLearnLogSequences_TopLevelCountsOnlyTowardMax(t *testing.T) {
	t.Parallel()

	c := newLearnChecker()
	bulk := oracle.Bulk{Requests: []*servicepb.Request{
		oracletest.TxReq("world", "acc:a", "USD", 1),
	}}

	c.learnLogSequences(bulk, []*commonpb.Log{topLevelLog(99)})

	require.Equal(t, uint64(99), c.maxLogSeq)
	require.Empty(t, c.logBySeq)
	require.Empty(t, c.logSeqRing)
}

func TestLearnLogSequences_EvictsOldestBeyondWindow(t *testing.T) {
	t.Parallel()

	c := newLearnChecker()

	for i := range uint64(learnedLogSeqWindow + 10) {
		seq := i + 1
		c.learnLogSequences(
			oracle.Bulk{Requests: []*servicepb.Request{oracletest.TxReq("world", "acc:a", "USD", 1)}},
			[]*commonpb.Log{applyLog(seq, seq, "L")},
		)
	}

	require.Len(t, c.logSeqRing, learnedLogSeqWindow)
	require.Len(t, c.logBySeq, learnedLogSeqWindow)

	// The ten oldest are gone, the newest is present, and the ring stays the map's
	// key set — a stale ring entry would hand out a sequence with no record.
	require.NotContains(t, c.logBySeq, uint64(1))
	require.NotContains(t, c.logBySeq, uint64(10))
	require.Contains(t, c.logBySeq, uint64(11))
	require.Contains(t, c.logBySeq, uint64(learnedLogSeqWindow+10))

	for _, seq := range c.logSeqRing {
		require.Contains(t, c.logBySeq, seq)
	}
}

// A sequence already learned is never relearned, so a replay cannot retarget it.
func TestLearnLogSequences_DuplicateSequenceKeepsFirst(t *testing.T) {
	t.Parallel()

	c := newLearnChecker()
	bulk := oracle.Bulk{Requests: []*servicepb.Request{oracletest.TxReq("world", "acc:a", "USD", 1)}}

	c.learnLogSequences(bulk, []*commonpb.Log{applyLog(5, 3, "L")})
	c.learnLogSequences(bulk, []*commonpb.Log{applyLog(5, 99, "L")})

	require.Equal(t, learnedLog{ledger: "L", id: 3}, c.logBySeq[5])
	require.Len(t, c.logSeqRing, 1)
}

func TestLogKindAt(t *testing.T) {
	t.Parallel()

	ls := buildLedger(t,
		oracletest.AddTypeReq("acc"),
		oracletest.TxReq("world", "acc:a", "USD", 1),
	)

	kind, ok := ls.LogKindAt(1)
	require.True(t, ok)
	require.Equal(t, "added_account_type", kind)

	kind, ok = ls.LogKindAt(2)
	require.True(t, ok)
	require.Equal(t, "created_transaction", kind)

	_, ok = ls.LogKindAt(0)
	require.False(t, ok)

	_, ok = ls.LogKindAt(99)
	require.False(t, ok)
}

// serverLogKind must speak the same vocabulary as the model's kinds, or every
// GetLog on a learned sequence would report a mismatch.
func TestServerLogKindMatchesModelVocabulary(t *testing.T) {
	t.Parallel()

	ls := buildLedger(t,
		oracletest.AddTypeReq("acc"),
		oracletest.TxReq("world", "acc:a", "USD", 1),
	)

	modelKind, ok := ls.LogKindAt(2)
	require.True(t, ok)

	served := &commonpb.Log{
		Sequence: 2,
		Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_Apply{
			Apply: &commonpb.ApplyLedgerLog{
				LedgerName: "L",
				Log: &commonpb.LedgerLog{Id: 2, Data: &commonpb.LedgerLogPayload{
					Payload: &commonpb.LedgerLogPayload_CreatedTransaction{
						CreatedTransaction: &commonpb.CreatedTransaction{},
					},
				}},
			},
		}},
	}

	require.Equal(t, modelKind, serverLogKind(served))
	require.Equal(t, "other", serverLogKind(topLevelLog(3)))
}
