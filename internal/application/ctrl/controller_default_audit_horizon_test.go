package ctrl

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/pkg/cursor"
	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/readstore"
)

func TestListAuditEntriesTrimsProjectionAheadOfMainSnapshot(t *testing.T) {
	t.Parallel()

	logger := logging.NopZap()
	meter := noop.NewMeterProvider().Meter("test")
	store, err := dal.NewStore(t.TempDir(), logger, meter, dal.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })

	mainBatch := store.OpenWriteSession()
	mainKey := dal.NewKeyBuilder().PutZonePrefix(dal.ZoneHistory, dal.SubHistoryAudit).PutUint64(1).Build()
	require.NoError(t, mainBatch.SetProto(mainKey, &auditpb.AuditEntry{
		Sequence: 1,
		Outcome:  &auditpb.AuditEntry_Success{Success: &auditpb.AuditSuccess{}},
		Ledgers:  []string{"main"},
	}))
	require.NoError(t, mainBatch.Commit())

	rs, err := readstore.New(t.TempDir(), logger, readstore.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = rs.Close() })
	indexBatch := rs.NewBatch()
	kb := dal.NewKeyBuilder()
	require.NoError(t, indexBatch.SetBytes(readstore.AuditIndexStringKey(kb, readstore.AuditFieldLedger, "main", 1), nil))
	require.NoError(t, indexBatch.SetBytes(readstore.AuditIndexStringKey(kb, readstore.AuditFieldLedger, "main", 2), nil))
	require.NoError(t, indexBatch.Commit())

	filter := &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Audit{Audit: &commonpb.AuditCondition{
		Field: commonpb.AuditField_AUDIT_FIELD_LEDGER,
		Condition: &commonpb.AuditCondition_StringCond{StringCond: &commonpb.StringCondition{
			Value: &commonpb.StringCondition_Hardcoded{Hardcoded: "main"},
		}},
	}}}
	ctrl := NewDefaultController(nil, store, logger, attributes.New(), rs, nil, nil, meter)
	c, err := ctrl.ListAuditEntriesFrom(context.Background(), store, rs, 10, 0, filter, false)
	require.NoError(t, err)
	entries, err := cursor.Collect(c)
	require.NoError(t, err)
	require.Len(t, entries, 1, "audit candidates beyond the main snapshot must be trimmed before materialization")
	require.Equal(t, uint64(1), entries[0].GetSequence())

	_, err = ctrl.ListAuditEntriesFrom(context.Background(), store, rs, 10, 0, &commonpb.QueryFilter{}, false)
	require.Error(t, err, "a malformed filter must fail without leaking its main-store read handle")
}
