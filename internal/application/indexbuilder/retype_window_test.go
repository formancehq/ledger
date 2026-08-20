package indexbuilder

import (
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain/indexes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/readstore"
)

// eventGroupExists reports whether the versioned metadata index holds an
// event group for (encoded value, entity) — i.e. whether a query scanning
// that value at that version would consider the entity at all.
func eventGroupExists(t *testing.T, s *readstore.Store, ledger, ns, metaKey string, version uint32, encoded, entity []byte) bool {
	t.Helper()

	kb := dal.NewKeyBuilder()
	prefix := append([]byte(nil), readstore.MetadataIndexPrefixV(kb, ledger, ns, metaKey, version)...)
	prefix = append(prefix, encoded...)
	prefix = append(prefix, entity...)

	iter, err := s.DB().NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: readstore.IncrementBytes(prefix),
	})
	require.NoError(t, err)

	defer func() { _ = iter.Close() }()

	return iter.First()
}

// A live write during a retype window is coerced once per version: v_current
// keeps the OLD type's encoding, so the index it serves stays exactly the one
// the retype never touched, while v_pending carries the retype's target. One
// shared encoding — the pre-fix behavior — turns v_current into a mixture no
// declared type describes: -1 under INT64→UINT32 encodes as null in BOTH
// versions, and the old-typed query loses a row it must still see (EN-1724).
func TestRetypeWindow_LiveWriteIsCoercedPerVersion(t *testing.T) {
	t.Parallel()

	b := newTestBuilderWithStore(t)
	b.seedBatchSchema(t)
	b.accounts = make(map[string]struct{})

	const (
		ledger  = "test"
		metaKey = "k2"
		account = "acct:1"
	)

	id := indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_ACCOUNT, metaKey)
	canonical := indexes.Canonical(id)

	cfg := newLedgerIndexConfig()
	cfg.byCanonical[canonical] = &commonpb.Index{Id: id}

	// Mid-window state: v1 was built under INT64, the retype to UINT32 is
	// rewriting into v2.
	b.putVersionState(ledger, canonical, readstore.IndexVersionState{
		CurrentVersion:      1,
		PendingVersion:      2,
		CurrentType:         commonpb.MetadataType_METADATA_TYPE_INT64,
		CurrentTypeDeclared: true,
		PendingType:         commonpb.MetadataType_METADATA_TYPE_UINT32,
		PendingTypeDeclared: true,
	})

	batch := b.readStore.NewBatch()
	b.initBatch(batch)
	b.wb.SetEventSequence(1)

	// -1: valid INT64, out of range for UINT32.
	require.NoError(t, b.indexSavedMetadata(b.kb, cfg, ledger, savedAccountMetadata(account, metaKey, -1)))
	require.NoError(t, b.wb.Flush())

	oldEncoded := readstore.EncodeMetadataValue(nil, commonpb.NewIntValue(-1))
	newEncoded := readstore.EncodeMetadataValue(nil, commonpb.ConvertMetadataValue(commonpb.NewIntValue(-1), commonpb.MetadataType_METADATA_TYPE_UINT32))

	assert.True(t, eventGroupExists(t, b.readStore, ledger, readstore.NamespaceAccount, metaKey, 1, oldEncoded, []byte(account)),
		"v_current must hold the OLD encoding: the window serves the index as if no retype had happened")
	assert.False(t, eventGroupExists(t, b.readStore, ledger, readstore.NamespaceAccount, metaKey, 1, newEncoded, []byte(account)),
		"the new encoding must not leak into v_current — that mixture is the partial-results bug")
	assert.True(t, eventGroupExists(t, b.readStore, ledger, readstore.NamespaceAccount, metaKey, 2, newEncoded, []byte(account)),
		"v_pending must hold the NEW encoding so the switch promotes a complete new-typed index")
}

// The reverse direction: the FSM stored the value under the NEW schema (a
// retype flips the declared type at apply), so what reaches the builder for
// an out-of-range write is already null-with-original. v_current must still
// receive the OLD type's faithful encoding, recovered through the null's
// original representation.
func TestRetypeWindow_NullOriginalRoundTripsIntoTheOldEncoding(t *testing.T) {
	t.Parallel()

	b := newTestBuilderWithStore(t)
	b.seedBatchSchema(t)
	b.accounts = make(map[string]struct{})

	const (
		ledger  = "test"
		metaKey = "k2"
		account = "acct:2"
	)

	id := indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_ACCOUNT, metaKey)
	canonical := indexes.Canonical(id)

	cfg := newLedgerIndexConfig()
	cfg.byCanonical[canonical] = &commonpb.Index{Id: id}

	b.putVersionState(ledger, canonical, readstore.IndexVersionState{
		CurrentVersion:      1,
		PendingVersion:      2,
		CurrentType:         commonpb.MetadataType_METADATA_TYPE_INT64,
		CurrentTypeDeclared: true,
		PendingType:         commonpb.MetadataType_METADATA_TYPE_UINT32,
		PendingTypeDeclared: true,
	})

	batch := b.readStore.NewBatch()
	b.initBatch(batch)
	b.wb.SetEventSequence(1)

	// What the FSM stores for a post-retype write of -1 under UINT32.
	stored := commonpb.ConvertMetadataValue(commonpb.NewIntValue(-1), commonpb.MetadataType_METADATA_TYPE_UINT32)
	_, isNull := stored.GetType().(*commonpb.MetadataValue_NullValue)
	require.True(t, isNull, "premise: -1 under UINT32 is stored as null-with-original")

	sm := &commonpb.SavedMetadata{
		Target: &commonpb.Target{Target: &commonpb.Target_Account{
			Account: &commonpb.TargetAccount{Addr: account},
		}},
		Metadata: map[string]*commonpb.MetadataValue{metaKey: stored},
	}
	require.NoError(t, b.indexSavedMetadata(b.kb, cfg, ledger, sm))
	require.NoError(t, b.wb.Flush())

	oldEncoded := readstore.EncodeMetadataValue(nil, commonpb.NewIntValue(-1))

	assert.True(t, eventGroupExists(t, b.readStore, ledger, readstore.NamespaceAccount, metaKey, 1, oldEncoded, []byte(account)),
		"v_current recovers int(-1) through the null's original — the write lands exactly where the old world would have put it")
}
