package state

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/pkg/kv"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

func newTestLedgerKeyStore() *attributes.KeyStore[domain.LedgerKey, *commonpb.LedgerInfo] {
	return attributes.NewKeyStore[domain.LedgerKey, *commonpb.LedgerInfo](
		kv.NewShardedMap[attributes.U128, attributes.Entry[*commonpb.LedgerInfo]](func(k attributes.U128) uint64 { return k.Hi() }),
	)
}

// TestRawAccessor_NormalizesTypedNilToErrNotFound pins the contract that
// MirrorPreload's bloom-confirmed-absent sentinel — a present cache entry
// holding a typed-nil pointer (see protoSnapshotSlot.MirrorPreload in
// cache_snapshotter.go) — surfaces as (zero R, domain.ErrNotFound).
//
// A naive `any(v) == nil` check fails for typed-nil pointers because
// the interface wrap keeps the dynamic type descriptor; the test would
// observe a spurious (nil R, nil) hit instead of ErrNotFound, and
// downstream callers that dereference under `if err == nil` would
// panic. The fix uses `v == zeroV` (requires V comparable).
func TestRawAccessor_NormalizesTypedNilToErrNotFound(t *testing.T) {
	t.Parallel()

	store := attributes.NewDerivedKeyStore(newTestLedgerKeyStore())

	// Simulate MirrorPreload's bloom-confirmed-absent shape: Put a
	// typed-nil pointer for the key. DerivedKeyStore returns (nil V,
	// nil err) on the next Get for this key.
	key := domain.LedgerKey{Name: "absent"}
	store.Put(key, (*commonpb.LedgerInfo)(nil))

	accessor := newRawAccessor[domain.LedgerKey, *commonpb.LedgerInfo, commonpb.LedgerInfoReader](store)

	reader, err := accessor.Get(key)
	require.ErrorIs(t, err, domain.ErrNotFound,
		"typed-nil cache entry must surface as ErrNotFound, not (nil reader, nil)")
	require.Nil(t, reader)
}

// TestRawAccessor_HitReturnsReader confirms that a present non-nil
// value is wrapped via AsReader and returned with no error.
func TestRawAccessor_HitReturnsReader(t *testing.T) {
	t.Parallel()

	store := attributes.NewDerivedKeyStore(newTestLedgerKeyStore())

	key := domain.LedgerKey{Name: "live"}
	store.Put(key, &commonpb.LedgerInfo{Name: "live", Id: 42})

	accessor := newRawAccessor[domain.LedgerKey, *commonpb.LedgerInfo, commonpb.LedgerInfoReader](store)

	reader, err := accessor.Get(key)
	require.NoError(t, err)
	require.NotNil(t, reader)
	require.Equal(t, "live", reader.GetName())
	require.Equal(t, uint32(42), reader.GetId())
}

// TestRawAccessor_AbsentReturnsErrNotFound confirms that a key not in
// the overlay nor in the parent surfaces as ErrNotFound (the
// DerivedKeyStore's default miss shape, distinct from the typed-nil
// preload sentinel).
func TestRawAccessor_AbsentReturnsErrNotFound(t *testing.T) {
	t.Parallel()

	store := attributes.NewDerivedKeyStore(newTestLedgerKeyStore())
	accessor := newRawAccessor[domain.LedgerKey, *commonpb.LedgerInfo, commonpb.LedgerInfoReader](store)

	reader, err := accessor.Get(domain.LedgerKey{Name: "missing"})
	require.ErrorIs(t, err, domain.ErrNotFound)
	require.Nil(t, reader)
}
