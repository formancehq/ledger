package check

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

func TestLedgerMetadataVerifier_LifecycleAndCorruption(t *testing.T) {
	t.Parallel()
	for _, scenario := range []string{"healthy", "missing", "injected", "changed", "truncated"} {
		t.Run(scenario, func(t *testing.T) {
			t.Parallel()
			store, attrs := createTestStore(t), attributes.New()
			v := newLedgerMetadataVerifier()
			apply := func(payload any) {
				ls := &raftcmdpb.LedgerScopedOrder{Ledger: "main"}
				switch p := payload.(type) {
				case *raftcmdpb.CreateLedgerOrder:
					ls.Payload = &raftcmdpb.LedgerScopedOrder_CreateLedger{CreateLedger: p}
				case *raftcmdpb.SaveLedgerMetadataOrder:
					ls.Payload = &raftcmdpb.LedgerScopedOrder_SaveLedgerMetadata{SaveLedgerMetadata: p}
				case *raftcmdpb.DeleteLedgerMetadataOrder:
					ls.Payload = &raftcmdpb.LedgerScopedOrder_DeleteLedgerMetadata{DeleteLedgerMetadata: p}
				case *raftcmdpb.DeleteLedgerOrder:
					ls.Payload = &raftcmdpb.LedgerScopedOrder_DeleteLedger{DeleteLedger: p}
				}
				v.applyOrder(&raftcmdpb.Order{Type: &raftcmdpb.Order_LedgerScoped{LedgerScoped: ls}})
			}
			apply(&raftcmdpb.CreateLedgerOrder{Metadata: map[string]*commonpb.MetadataValue{"owner": commonpb.NewStringValue("initial"), "removed": commonpb.NewBoolValue(true)}})
			apply(&raftcmdpb.SaveLedgerMetadataOrder{Metadata: map[string]*commonpb.MetadataValue{"owner": commonpb.NewStringValue("later"), "count": commonpb.NewUintValue(9007199254740993)}})
			apply(&raftcmdpb.DeleteLedgerMetadataOrder{Key: "removed"})
			batch := store.OpenWriteSession()
			put := func(key string, value *commonpb.MetadataValue) {
				_, err := attrs.LedgerMetadata.Set(batch, domain.LedgerMetadataKey{LedgerName: "main", Key: key}.Bytes(), value)
				require.NoError(t, err)
			}
			if scenario != "missing" {
				value := "later"
				if scenario == "changed" {
					value = "corrupt"
				}
				put("owner", commonpb.NewStringValue(value))
			}
			put("count", commonpb.NewUintValue(9007199254740993))
			if scenario == "injected" {
				put("extra", commonpb.NewBoolValue(true))
			}
			require.NoError(t, batch.Commit())
			handle, err := store.NewReadHandle()
			require.NoError(t, err)
			defer func() { require.NoError(t, handle.Close()) }()
			v.liveTruncated = scenario == "truncated"
			var events []*servicepb.CheckStoreError
			require.NoError(t, v.compare(handle, attrs, func(e *servicepb.CheckStoreEvent) { events = append(events, e.GetError()) }))
			if scenario == "healthy" || scenario == "truncated" {
				require.Empty(t, events)
			} else {
				require.Len(t, events, 1)
				require.Equal(t, servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_METADATA_MISMATCH, events[0].GetErrorType())
			}
			apply(&raftcmdpb.DeleteLedgerOrder{})
			require.Empty(t, v.values, "ledger deletion drops all folded metadata")
		})
	}
}
