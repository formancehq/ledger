package check

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

func TestLedgerKVVerifierDetectsProjectionDrift(t *testing.T) {
	t.Parallel()
	for _, scenario := range []string{"healthy", "changed", "missing", "injected", "truncated"} {
		t.Run(scenario, func(t *testing.T) {
			t.Parallel()
			store := createTestStore(t)
			v := newLedgerKVVerifier()
			v.applyOrder(&raftcmdpb.Order{Type: &raftcmdpb.Order_LedgerScoped{LedgerScoped: &raftcmdpb.LedgerScopedOrder{
				Ledger: "ledger", Payload: &raftcmdpb.LedgerScopedOrder_PutKv{PutKv: &raftcmdpb.PutKVOrder{Key: "key", Value: []byte{0, 255}}},
			}}})
			batch := store.OpenWriteSession()
			if scenario != "missing" {
				value := []byte{0, 255}
				if scenario == "changed" {
					value = []byte("tampered")
				}
				require.NoError(t, batch.SetBytes(dal.LedgerKVKey("ledger", "key"), value))
			}
			if scenario == "injected" {
				require.NoError(t, batch.SetBytes(dal.LedgerKVKey("ledger", "extra"), []byte("x")))
			}
			require.NoError(t, batch.Commit())
			handle, err := store.NewReadHandle()
			require.NoError(t, err)
			defer func() { require.NoError(t, handle.Close()) }()
			v.liveTruncated = scenario == "truncated"
			var errors []*servicepb.CheckStoreError
			require.NoError(t, v.compare(handle, func(e *servicepb.CheckStoreEvent) { errors = append(errors, e.GetError()) }))
			if scenario == "healthy" || scenario == "truncated" {
				require.Empty(t, errors)
			} else {
				require.Len(t, errors, 1)
				require.Equal(t, servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_KV_MISMATCH, errors[0].GetErrorType())
			}
		})
	}
}

func TestLedgerKVVerifierIgnoresRetiredMirrorCursorRows(t *testing.T) {
	t.Parallel()
	store := createTestStore(t)
	legacyKey := dal.NewKeyBuilder().PutZonePrefix(dal.ZonePerLedger, 0x05).
		PutLedgerNameFixed("ledger").Build()
	require.Equal(t, byte(0x06), dal.SubPLKV, "KV must not reuse the retired mirror cursor prefix")
	batch := store.OpenWriteSession()
	require.NoError(t, batch.SetBytes(legacyKey, []byte("old mirror cursor")))
	require.NoError(t, batch.Commit())
	handle, err := store.NewReadHandle()
	require.NoError(t, err)
	defer func() { require.NoError(t, handle.Close()) }()
	var findings []*servicepb.CheckStoreError
	require.NoError(t, newLedgerKVVerifier().compare(handle, func(e *servicepb.CheckStoreEvent) {
		findings = append(findings, e.GetError())
	}))
	require.Empty(t, findings, "retired rows from a released store are outside the KV projection")
}
