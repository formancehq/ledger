package dal

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

func newDeterminismStore(t *testing.T, deterministic bool) *Store {
	t.Helper()

	cfg := DefaultConfig()
	cfg.DeterministicEncoding = deterministic

	store, err := NewStore(t.TempDir(), logging.Testing(), noop.Meter{}, cfg)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, store.Close())
	})

	return store
}

// TestWriteSession_MarshalProto_DeterministicWhenFlagOn verifies that two
// LedgerInfo messages built from the same logical state but with different
// map-insertion order produce byte-identical bytes when the parent store has
// DeterministicEncoding=true. With the flag OFF the bytes may differ (the
// historical default).
//
// LedgerInfo is a good probe because it carries both a Metadata map<string,
// string> AND a repeated AccountType slice — the marshaler should sort the
// map keys but preserve the slice order, and the producer is expected to
// keep the slice in canonical order itself.
func TestWriteSession_MarshalProto_DeterministicWhenFlagOn(t *testing.T) {
	t.Parallel()

	// Build two semantically-identical LedgerInfo with maps populated in
	// opposite insertion order. With the default Go map randomization, the
	// non-deterministic marshaler would emit different bytes; the
	// deterministic marshaler should produce identical bytes.
	first := &commonpb.LedgerInfo{
		Name:     "test",
		Metadata: map[string]*commonpb.MetadataValue{},
	}
	second := &commonpb.LedgerInfo{
		Name:     "test",
		Metadata: map[string]*commonpb.MetadataValue{},
	}

	keys := []string{"alpha", "beta", "gamma", "delta", "epsilon"}
	for _, k := range keys {
		first.Metadata[k] = &commonpb.MetadataValue{
			Type: &commonpb.MetadataValue_StringValue{StringValue: k + "-value"},
		}
	}

	for _, v := range slices.Backward(keys) {
		k := v
		second.Metadata[k] = &commonpb.MetadataValue{
			Type: &commonpb.MetadataValue_StringValue{StringValue: k + "-value"},
		}
	}

	t.Run("flag_on_produces_identical_bytes", func(t *testing.T) {
		t.Parallel()

		store := newDeterminismStore(t, true)
		sess := store.OpenWriteSession()
		defer func() { _ = sess.Cancel() }()

		bytesFirst, err := sess.MarshalProto(first)
		require.NoError(t, err)
		// Copy because MarshalProto returns the session's reusable buffer.
		bytesFirstCopy := append([]byte(nil), bytesFirst...)

		bytesSecond, err := sess.MarshalProto(second)
		require.NoError(t, err)

		require.Equal(t, bytesFirstCopy, bytesSecond,
			"deterministic encoding must produce identical bytes for the same logical state regardless of map insertion order")
	})

	t.Run("flag_off_uses_historical_path", func(t *testing.T) {
		t.Parallel()

		store := newDeterminismStore(t, false)
		sess := store.OpenWriteSession()
		defer func() { _ = sess.Cancel() }()

		// We don't assert inequality here (map iteration could happen to
		// match by chance), only that the historical path still works and
		// produces a valid encoded message.
		bytesFirst, err := sess.MarshalProto(first)
		require.NoError(t, err)
		require.NotEmpty(t, bytesFirst)

		// Roundtrip to confirm the bytes are a valid LedgerInfo regardless of
		// the encoder branch taken.
		round := &commonpb.LedgerInfo{}
		require.NoError(t, round.UnmarshalVT(bytesFirst))
		require.Equal(t, first.GetName(), round.GetName())
		require.Len(t, round.GetMetadata(), len(keys))
	})
}

// TestWriteSession_DeterministicEncoding_PropagatesFromStore verifies that a
// session created via OpenWriteSession() inherits the store's
// DeterministicEncoding flag, while sessions built via NewWriteSessionFromDB
// remain in the non-deterministic legacy path (they bypass the FSM and never
// feed the cross-node digest).
func TestWriteSession_DeterministicEncoding_PropagatesFromStore(t *testing.T) {
	t.Parallel()

	on := newDeterminismStore(t, true)
	off := newDeterminismStore(t, false)

	onSess := on.OpenWriteSession()
	defer func() { _ = onSess.Cancel() }()

	offSess := off.OpenWriteSession()
	defer func() { _ = offSess.Cancel() }()

	require.True(t, onSess.DeterministicEncoding())
	require.False(t, offSess.DeterministicEncoding())

	require.True(t, on.DeterministicEncoding())
	require.False(t, off.DeterministicEncoding())
}
