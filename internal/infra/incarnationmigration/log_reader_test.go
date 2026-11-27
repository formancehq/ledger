package incarnationmigration

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

func TestAuditLogReaderPreservesOutOfOrderReferences(t *testing.T) {
	t.Parallel()
	store := migrationStore(t)
	appendFixture(t, store, 1, 100, &commonpb.LogPayload{Type: &commonpb.LogPayload_CreateLedger{CreateLedger: &commonpb.CreatedLedgerLog{Name: "ledger"}}})
	appendFixture(t, store, 2, 200, &commonpb.LogPayload{Type: &commonpb.LogPayload_DeleteLedger{DeleteLedger: &commonpb.DeletedLedgerLog{Name: "ledger"}}})
	reader, err := store.NewReadHandle()
	require.NoError(t, err)
	defer func() { require.NoError(t, reader.Close()) }()
	logs, err := newAuditLogReader(t.Context(), reader)
	require.NoError(t, err)
	defer func() { require.NoError(t, logs.close()) }()
	for _, sequence := range []uint64{2, 1, 2} {
		log, err := logs.get(t.Context(), sequence)
		require.NoError(t, err)
		require.Equal(t, sequence, log.GetSequence())
	}
	_, err = logs.get(t.Context(), 3)
	require.ErrorContains(t, err, "missing audit-linked log 3")
}
