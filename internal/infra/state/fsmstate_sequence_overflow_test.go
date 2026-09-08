package state

import (
	"encoding/binary"
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// TestLoadFSMStateFromStore_RefusesMaxUint64Heads pins the overflow guard on
// the recovery path.
//
// Both heads are advanced with a plain +1, which wraps at math.MaxUint64: the
// FSM would restart allocating at sequence 0 — a position the checker reports
// as impossible — on top of whatever row is already there, and the audit chain
// would be rewritten from its beginning. The FSM cannot reach either head on its
// own (both counters are seeded at 1 and only incremented), so a store holding
// one was corrupted or restored from a tampered export.
//
// This guard became reachable when the sequence-keyed scans moved to the prefix
// successor: before that, dal.ReadLastEntry's exclusive 0xFF-run upper bound was
// byte-identical to the MaxUint64 key and never returned the row, so the wrap
// was avoided by accident rather than refused on purpose.
func TestLoadFSMStateFromStore_RefusesMaxUint64Heads(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name  string
		zone  byte
		sub   byte
		value func() []byte
		want  string
	}{
		{
			name: "log head at the maximum uint64",
			zone: dal.ZoneHistory,
			sub:  dal.SubHistoryLog,
			value: func() []byte {
				out, err := (&commonpb.Log{Sequence: math.MaxUint64}).MarshalVT()
				require.NoError(t, err)

				return out
			},
			want: "stored log head is 18446744073709551615",
		},
		{
			name: "audit head at the maximum uint64",
			zone: dal.ZoneHistory,
			sub:  dal.SubHistoryAudit,
			value: func() []byte {
				out, err := (&auditpb.AuditEntry{Sequence: math.MaxUint64}).MarshalVT()
				require.NoError(t, err)

				return out
			},
			want: "stored audit head is 18446744073709551615",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, store, _ := newTestMachine(t)

			key := make([]byte, 10)
			key[0], key[1] = tc.zone, tc.sub
			binary.BigEndian.PutUint64(key[2:], math.MaxUint64)

			batch := store.OpenWriteSession()
			require.NoError(t, batch.SetBytes(key, tc.value()))
			require.NoError(t, batch.Commit())

			handle, err := store.NewReadHandle()
			require.NoError(t, err)

			defer func() { _ = handle.Close() }()

			state, err := LoadFSMStateFromStore(store, handle, "overflow-cluster")
			require.Error(t, err, "recovery must refuse to boot rather than wrap the counter")
			require.Nil(t, state)
			require.Contains(t, err.Error(), tc.want)
		})
	}
}

// TestAllocatorsRefuseToWrap pins the guard at the allocators themselves, which
// is the only place that holds for every head.
//
// The boot guard in LoadFSMStateFromStore rejects a head of math.MaxUint64, but
// a head one below it boots happily: the next allocation returns MaxUint64 and
// leaves the counter at 0, and every allocation after that hands out a sequence
// that already addresses a stored row. Rejecting the penultimate head instead
// would only move the question to the head below that one.
//
// Both counters are replicated state, so the refusal is deterministic — every
// node reaches it on the same entry and fails the same proposal (invariant #2).
func TestAllocatorsRefuseToWrap(t *testing.T) {
	t.Parallel()

	t.Run("log sequence", func(t *testing.T) {
		t.Parallel()

		ws := &WriteSet{NextSequenceID: math.MaxUint64 - 1}

		last, err := ws.IncrementNextSequenceID()
		require.NoError(t, err, "the last representable sequence is still allocatable")
		require.EqualValues(t, uint64(math.MaxUint64-1), last)
		require.EqualValues(t, uint64(math.MaxUint64), ws.NextSequenceID)

		_, err = ws.IncrementNextSequenceID()
		require.ErrorIs(t, err, domain.ErrSequenceSpaceExhausted,
			"the allocation that would wrap the counter must fail instead")
		require.EqualValues(t, uint64(math.MaxUint64), ws.NextSequenceID,
			"a refused allocation must not move the counter")
	})

	t.Run("audit sequence", func(t *testing.T) {
		t.Parallel()

		s := &FSMState{NextAuditSequenceID: math.MaxUint64 - 1}

		last, err := s.AppendAuditEntry([]byte("hash-penultimate"))
		require.NoError(t, err)
		require.EqualValues(t, uint64(math.MaxUint64-1), last)

		_, err = s.AppendAuditEntry([]byte("hash-wrap"))
		require.ErrorIs(t, err, domain.ErrSequenceSpaceExhausted,
			"wrapping would restart the audit chain over its own beginning")
		require.EqualValues(t, uint64(math.MaxUint64), s.NextAuditSequenceID)
		require.Equal(t, []byte("hash-penultimate"), s.LastAuditHash,
			"a refused append must not advance the chain head either")
	})
}
