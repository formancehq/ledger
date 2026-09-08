package state

import (
	"encoding/binary"
	"math"
	"testing"

	"github.com/stretchr/testify/require"

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
