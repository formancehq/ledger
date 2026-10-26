package check

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

func TestCheckerFSMRejectsMalformedMetadataKey(t *testing.T) {
	t.Parallel()

	machine, store, attrs := newFSMPurgeMachine(t)
	applyFSMPurgeProposal(t, machine, store, attrs, 1, createLedgerOrder("main"))
	applyFSMPurgeProposal(t, machine, store, attrs, 2,
		saveAccountMetadataOrder("main", "users:001", map[string]string{"status": "approved"}))
	require.Empty(t, runFSMPurgeCheck(t, store, attrs), "healthy control must have no corruption events")

	// Retain every legitimate row and its audit/log witnesses. Only add a
	// surplus row with a valid protobuf value and no account/key separator.
	key := make([]byte, dal.LedgerNameFixedSize)
	copy(key, "main")
	key = append(key, []byte("users:001/status")...)
	batch := store.OpenWriteSession()
	defer func() { _ = batch.Cancel() }() // Best-effort cleanup after commit or assertion failure.
	_, err := attrs.Metadata.Set(batch, key, commonpb.NewStringValue("injected"))
	require.NoError(t, err)
	require.NoError(t, batch.Commit())

	findings := runFSMPurgeCheck(t, store, attrs)
	require.Len(t, findings, 1)
	require.Equal(t, servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_METADATA_MISMATCH, findings[0].GetErrorType())
	require.Contains(t, findings[0].GetMessage(), fmt.Sprintf("unparseable metadata projection key %x:", key))
	require.Contains(t, findings[0].GetMessage(), "missing account/key separator")
	require.Empty(t, findings[0].GetLedger())
	require.Empty(t, findings[0].GetAccount())
}

func TestCheckerFSMRejectsMalformedTransactionKey(t *testing.T) {
	t.Parallel()

	machine, store, attrs := newFSMPurgeMachine(t)
	applyFSMPurgeProposal(t, machine, store, attrs, 1, createLedgerOrder("main"))
	applyFSMPurgeProposal(t, machine, store, attrs, 2,
		createTransactionOrder("main", true, newPosting("world", "users:001", "USD", 100)))
	require.Empty(t, runFSMPurgeCheck(t, store, attrs), "healthy control must have no corruption events")

	// Keep the valid ledger prefix and transaction ID width, but corrupt the
	// separator. The valid transaction and its audit/log witnesses stay intact.
	key := domain.TransactionKey{LedgerName: "main", ID: 9999}.Bytes()
	key[dal.LedgerNameFixedSize] = dal.CanonicalKeySepMetadata
	batch := store.OpenWriteSession()
	defer func() { _ = batch.Cancel() }() // Best-effort cleanup after commit or assertion failure.
	_, err := attrs.Transaction.Set(batch, key, &commonpb.TransactionState{CreatedByLog: 9999})
	require.NoError(t, err)
	require.NoError(t, batch.Commit())

	findings := runFSMPurgeCheck(t, store, attrs)
	require.Len(t, findings, 1)
	require.Equal(t, servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_TRANSACTION_UPDATE_MISMATCH, findings[0].GetErrorType())
	require.Contains(t, findings[0].GetMessage(), fmt.Sprintf("unparseable transaction projection key %x:", key))
	require.Contains(t, findings[0].GetMessage(), "expected separator")
	require.Empty(t, findings[0].GetLedger())
	require.Zero(t, findings[0].GetTransactionId())
}
