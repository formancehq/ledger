package readstore

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// A rewrite stamps every event it writes with the single FSM sequence it read
// from, so the promoted keyspace resolves EMPTY at any pin below that
// sequence. The resolver must report such a version as not yet live, leaving
// the caller to reject rather than serve a fully-populated index as empty.
func TestPinnedVersionResolver_HidesAVersionAboveThePin(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)

	const (
		ledger    = "l"
		canonical = "metadata:account:k2"
	)

	batch := s.NewBatch()
	require.NoError(t, s.WriteIndexVersionState(batch, ledger, canonical, IndexVersionState{
		CurrentVersion:     2,
		ActivationSequence: 150,
	}))
	require.NoError(t, batch.Commit())

	snap := s.NewSnapshot()
	defer func() { _ = snap.Close() }()

	for _, tc := range []struct {
		name string
		pin  uint64
		want uint32
	}{
		{"pin below the activation resolves nothing", 100, 0},
		{"pin at the activation serves the version", 150, 2},
		{"pin past the activation serves the version", 900, 2},
		{"no pin skips the check", 0, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			v, primed, err := PinnedVersionResolver(snap, ledger, tc.pin)(canonical)
			require.NoError(t, err)
			require.True(t, primed, "the record exists; only the version is withheld")
			require.Equal(t, tc.want, v)
		})
	}
}

// A version built by an initial backfill carries no activation sequence: its
// events hold the sequences of the logs they were folded from, so every pin
// resolves them correctly.
func TestPinnedVersionResolver_BackfilledVersionServesAtAnyPin(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)

	const (
		ledger    = "l"
		canonical = "metadata:account:k0"
	)

	batch := s.NewBatch()
	require.NoError(t, s.WriteIndexVersionState(batch, ledger, canonical, IndexVersionState{
		CurrentVersion: 1,
	}))
	require.NoError(t, batch.Commit())

	snap := s.NewSnapshot()
	defer func() { _ = snap.Close() }()

	v, primed, err := PinnedVersionResolver(snap, ledger, 1)(canonical)
	require.NoError(t, err)
	require.True(t, primed)
	require.Equal(t, uint32(1), v)
}

// The activation sequence survives the encode/decode round trip alongside a
// rewrite cursor, so a resumed rewrite does not lose it.
func TestIndexVersionState_ActivationRoundTrip(t *testing.T) {
	t.Parallel()

	want := IndexVersionState{
		CurrentVersion:     3,
		PendingVersion:     4,
		ActivationSequence: 4242,
		RewriteProgress:    []byte("cursor"),
	}

	got, ok := decodeIndexVersionState(encodeIndexVersionState(want))
	require.True(t, ok)
	require.Equal(t, want, got)
}

// A removed index leaves no record, which is what tells a reader "removed"
// rather than "still building" — the builder always writes one when it folds
// the CreateIndex log.
func TestPinnedVersionResolver_AbsentRecordReportsNotPrimed(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)

	snap := s.NewSnapshot()
	defer func() { _ = snap.Close() }()

	v, primed, err := PinnedVersionResolver(snap, "l", 10)("metadata:account:gone")
	require.NoError(t, err)
	require.False(t, primed)
	require.Zero(t, v)
}
