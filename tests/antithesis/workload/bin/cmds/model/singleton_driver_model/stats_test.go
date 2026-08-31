package main

import (
	"math/big"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/tests/oracle/oracletest"
)

func TestModelUsage_CountsWhatTheUsagebuilderCounts(t *testing.T) {
	t.Parallel()

	// tx1 carries two postings, tx2 one plus a reference, and the revert of tx1
	// is itself a transaction whose reversed postings count again — the
	// CounterPosting contract sums postings per CreatedTransaction AND
	// RevertedTransaction log.
	ls := buildLedger(t,
		oracletest.TxReqMulti(true,
			commonpb.NewPosting("world", "acc:a", "USD", big.NewInt(10)),
			commonpb.NewPosting("acc:a", "acc:b", "USD", big.NewInt(5)),
		),
		oracletest.TxReqRefL("L", "ref-1", "world", "acc:c", "USD", 7),
		oracletest.RevertReqL("L", 1, true),
	)

	u := modelUsage(ls)
	require.Equal(t, uint64(5), u.postings, "2 on tx1 + 1 on tx2 + 2 on the revert")
	require.Equal(t, uint64(1), u.references)
	require.Equal(t, uint64(1), u.reverts)
}

func TestModelUsage_EmptyLedger(t *testing.T) {
	t.Parallel()

	ls := buildLedger(t, oracletest.AddTypeReq("acc"))

	u := modelUsage(ls)
	require.Zero(t, u.postings)
	require.Zero(t, u.references)
	require.Zero(t, u.reverts)
}

func TestModelUsage_ReferenceCountedOncePerTransaction(t *testing.T) {
	t.Parallel()

	// A reference is counted per create carrying one, not per posting.
	ls := buildLedger(t,
		oracletest.TxReqRefL("L", "ref-a", "world", "acc:a", "USD", 1),
		oracletest.TxReqRefL("L", "ref-b", "world", "acc:b", "USD", 1),
		oracletest.TxReq("world", "acc:c", "USD", 1),
	)

	u := modelUsage(ls)
	require.Equal(t, uint64(3), u.postings)
	require.Equal(t, uint64(2), u.references)
	require.Zero(t, u.reverts)
}
