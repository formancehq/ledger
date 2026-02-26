package state

import (
	"testing"

	"github.com/formancehq/ledger-v3-poc/internal/domain"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/infra/attributes"
	"github.com/formancehq/ledger-v3-poc/internal/infra/cache"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"
)

func newTestRegistry(t *testing.T) *StateRegistry {
	t.Helper()
	meter := noop.NewMeterProvider().Meter("test")
	c, err := cache.New(1000, meter)
	require.NoError(t, err)
	attrs := attributes.New()
	return NewStateRegistry(c, attrs)
}

func TestNewStateRegistryFieldsNotNil(t *testing.T) {
	t.Parallel()

	reg := newTestRegistry(t)

	require.NotNil(t, reg.Cache)
	require.NotNil(t, reg.Attrs)
	require.NotNil(t, reg.Volumes)
	require.NotNil(t, reg.AccountMetadata)
	require.NotNil(t, reg.Reversions)
	require.NotNil(t, reg.IdempotencyKeys)
	require.NotNil(t, reg.References)
	require.NotNil(t, reg.Ledgers)
	require.NotNil(t, reg.Boundaries)
	require.NotNil(t, reg.SinkConfigs)
}

func TestStateRegistryKeyStoresPutAndGet(t *testing.T) {
	t.Parallel()

	reg := newTestRegistry(t)

	// Ledgers: Put and Get via canonical bytes
	ledgerKey := domain.LedgerKey{Name: "my-ledger"}
	ledgerInfo := &commonpb.LedgerInfo{Name: "my-ledger"}
	_, _, err := reg.Ledgers.Put(ledgerKey.Bytes(), ledgerInfo)
	require.NoError(t, err)
	gotLedger, _, err := reg.Ledgers.Get(ledgerKey.Bytes())
	require.NoError(t, err)
	require.NotNil(t, gotLedger)
	require.Equal(t, "my-ledger", gotLedger.Name)
}

func TestNewDerivedRegistryFieldsNotNil(t *testing.T) {
	t.Parallel()

	reg := newTestRegistry(t)
	derived := NewDerivedRegistry(reg)

	require.NotNil(t, derived.Volumes)
	require.NotNil(t, derived.AccountMetadata)
	require.NotNil(t, derived.Reversions)
	require.NotNil(t, derived.IdempotencyKeys)
	require.NotNil(t, derived.References)
	require.NotNil(t, derived.Ledgers)
	require.NotNil(t, derived.Boundaries)
	require.NotNil(t, derived.SinkConfigs)
}

func TestDerivedRegistryReadsFromParent(t *testing.T) {
	t.Parallel()

	reg := newTestRegistry(t)
	ledgerKey := domain.LedgerKey{Name: "ledger-a"}
	_, _, err := reg.Ledgers.Put(ledgerKey.Bytes(), &commonpb.LedgerInfo{Name: "ledger-a"})
	require.NoError(t, err)

	derived := NewDerivedRegistry(reg)

	// Derived reads through to parent
	got, err := derived.Ledgers.Get(ledgerKey)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(t, "ledger-a", got.Name)
}

func TestDerivedRegistryBuffersWrites(t *testing.T) {
	t.Parallel()

	reg := newTestRegistry(t)
	derived := NewDerivedRegistry(reg)

	ledgerKey := domain.LedgerKey{Name: "new-ledger"}
	derived.Ledgers.Put(ledgerKey, &commonpb.LedgerInfo{Name: "new-ledger"})

	// Derived sees the write
	got, err := derived.Ledgers.Get(ledgerKey)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(t, "new-ledger", got.Name)

	// Parent does NOT see the write (not merged yet)
	parentGot, _, err := reg.Ledgers.Get(ledgerKey.Bytes())
	require.Error(t, err) // ErrNotFound
	require.Nil(t, parentGot)
}

func TestDerivedRegistryMerge(t *testing.T) {
	t.Parallel()

	reg := newTestRegistry(t)
	derived := NewDerivedRegistry(reg)

	// Write through derived
	ledgerKey := domain.LedgerKey{Name: "merge-test"}
	derived.Ledgers.Put(ledgerKey, &commonpb.LedgerInfo{Name: "merge-test"})

	// Merge propagates to parent
	updates, _, err := derived.Ledgers.Merge()
	require.NoError(t, err)
	require.Len(t, updates, 1)
	require.Equal(t, "merge-test", updates[0].New.Name)

	// After applying the merge update, parent should have the value
	// (Merge returns the updates; the caller applies them to the parent via KeyStore)
}
