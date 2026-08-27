package indexbuilder

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/readstore"
)

// Membership assertions for the append-only metadata index event log (see
// readstore/event_keys.go). A (value, entity) group is live at a pin when the
// latest event at or below it is an ADD, so a forward entry is asserted by
// resolving its group rather than by probing a key. Every helper resolves at
// currentPin — the membership a caught-up query sees.
const currentPin = uint64(math.MaxUint64)

// resolveMetadataEvents returns the entities live under (metaKey, version,
// encoded) at the current pin, in key order.
func resolveMetadataEvents(t *testing.T, b *Builder, ledger, ns, metaKey string, version uint32, encoded []byte) []string {
	t.Helper()

	prefix := append([]byte(nil), readstore.MetadataIndexPrefixV(dal.NewKeyBuilder(), ledger, ns, metaKey, version)...)
	prefix = append(prefix, encoded...)

	snap := b.readStore.NewSnapshot()
	defer func() { _ = snap.Close() }()

	it, err := readstore.NewEventResolveIterator(snap, prefix, currentPin)
	require.NoError(t, err)

	defer it.Close()

	var live []string
	for it.Next() {
		live = append(live, string(it.Current()))
	}

	require.NoError(t, it.Err())

	return live
}

// requireMetadataLive asserts entity matches (metaKey, encoded) at version.
func requireMetadataLive(t *testing.T, b *Builder, ledger, ns, metaKey string, version uint32, encoded, entity []byte) {
	t.Helper()

	require.Contains(t, resolveMetadataEvents(t, b, ledger, ns, metaKey, version, encoded), string(entity),
		"entity %q must be live under %s v%d for value %x", entity, metaKey, version, encoded)
}

// requireMetadataDead asserts entity does not match (metaKey, encoded) at
// version — the group either holds no event for it or its latest event is a DEL.
func requireMetadataDead(t *testing.T, b *Builder, ledger, ns, metaKey string, version uint32, encoded, entity []byte) {
	t.Helper()

	require.NotContains(t, resolveMetadataEvents(t, b, ledger, ns, metaKey, version, encoded), string(entity),
		"entity %q must not be live under %s v%d for value %x", entity, metaKey, version, encoded)
}

// seedRewriteSequence puts both stores at log sequence seq: the FSM gets a log
// there, so the schema rewrite stamps its events with it, and the read-store
// cursor reaches it, so the rewrite's atomic switch is not held by the seq gate.
func seedRewriteSequence(t *testing.T, b *Builder, seq uint64) {
	t.Helper()

	writeLogToFSM(t, b, &commonpb.Log{Sequence: seq})

	progress := b.readStore.NewBatch()
	require.NoError(t, b.readStore.WriteProgress(progress, seq))
	require.NoError(t, progress.Commit())
}

// seedMetadataEvent commits one raw index event, standing in for the state a
// previous fold left in the index at seq.
func seedMetadataEvent(t *testing.T, b *Builder, ledger, ns, metaKey string, version uint32, encoded, entity []byte, seq uint64, op byte) {
	t.Helper()

	key := readstore.MetadataIndexEventKeyV(dal.NewKeyBuilder(), ledger, ns, metaKey, version, encoded, entity, seq, op)

	seed := b.readStore.NewBatch()
	require.NoError(t, seed.SetBytes(key, nil))
	require.NoError(t, seed.Commit())
}
