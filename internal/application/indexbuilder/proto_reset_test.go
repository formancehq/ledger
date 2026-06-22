package indexbuilder

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

func TestResetLogForReuse_PreservesNestedAllocations(t *testing.T) {
	t.Parallel()

	log := buildTestLog()

	// Capture pointers to nested objects that should be preserved.
	payload := log.GetPayload()
	apply := log.GetPayload().GetType().(*commonpb.LogPayload_Apply)
	applyLog := apply.Apply
	ledgerLog := applyLog.GetLog()
	ledgerLogPayload := ledgerLog.GetData()
	ct := ledgerLogPayload.GetPayload().(*commonpb.LedgerLogPayload_CreatedTransaction)
	createdTx := ct.CreatedTransaction
	txn := createdTx.GetTransaction()
	timestamp := txn.GetTimestamp()
	insertedAt := txn.GetInsertedAt()
	date := ledgerLog.GetDate()

	resetLogForReuse(log)

	// Verify the preserved chain is the SAME pointer (not reallocated).
	assert.Same(t, payload, log.GetPayload(), "Payload pointer should be preserved")
	assert.Same(t, apply, log.GetPayload().GetType().(*commonpb.LogPayload_Apply), "Apply wrapper preserved")
	assert.Same(t, applyLog, apply.Apply, "ApplyLedgerLog preserved")
	assert.Same(t, ledgerLog, applyLog.GetLog(), "LedgerLog preserved")
	assert.Same(t, ledgerLogPayload, ledgerLog.GetData(), "LedgerLogPayload preserved")
	assert.Same(t, ct, ledgerLogPayload.GetPayload().(*commonpb.LedgerLogPayload_CreatedTransaction), "CT wrapper preserved")
	assert.Same(t, createdTx, ct.CreatedTransaction, "CreatedTransaction preserved")
	assert.Same(t, txn, createdTx.GetTransaction(), "Transaction preserved")
	assert.Same(t, timestamp, txn.GetTimestamp(), "Timestamp preserved")
	assert.Same(t, insertedAt, txn.GetInsertedAt(), "InsertedAt preserved")
	assert.Same(t, date, ledgerLog.GetDate(), "Date preserved")
}

func TestResetLogForReuse_ClearsStaleData(t *testing.T) {
	t.Parallel()

	log := buildTestLog()

	resetLogForReuse(log)

	// Scalar fields must be zeroed (proto3 omits defaults from wire).
	assert.Equal(t, uint64(0), log.GetSequence())
	assert.Empty(t, log.GetReceipt())

	// Optional fields must be nil'd.
	assert.Nil(t, log.GetIdempotency())
	assert.Nil(t, log.GetSignature())
	assert.Nil(t, log.GetResponseSignature())

	apply := log.GetPayload().GetType().(*commonpb.LogPayload_Apply)
	assert.Empty(t, apply.Apply.GetLedgerName())
	assert.Equal(t, uint64(0), apply.Apply.GetLog().GetId())

	ct := apply.Apply.GetLog().GetData().GetPayload().(*commonpb.LedgerLogPayload_CreatedTransaction)
	createdTx := ct.CreatedTransaction

	assert.Equal(t, uint64(0), createdTx.GetChapterId())
	assert.Nil(t, createdTx.GetPostCommitVolumes())
	assert.Empty(t, createdTx.GetAccountMetadata())
	assert.Empty(t, createdTx.GetPreviousAccountMetadata())

	txn := createdTx.GetTransaction()
	assert.Equal(t, uint64(0), txn.GetId())
	assert.Empty(t, txn.GetReference())
	assert.False(t, txn.GetReverted())
	assert.Empty(t, txn.GetPostings())
	assert.Nil(t, txn.GetMetadata())
	assert.Nil(t, txn.GetUpdatedAt())
	assert.Nil(t, txn.GetRevertedAt())
}

func TestResetLogForReuse_PreservesSliceCapacity(t *testing.T) {
	t.Parallel()

	log := buildTestLog()

	ct := log.GetPayload().GetType().(*commonpb.LogPayload_Apply).Apply.GetLog().GetData().GetPayload().(*commonpb.LedgerLogPayload_CreatedTransaction).CreatedTransaction
	postingsCap := cap(ct.GetTransaction().GetPostings())
	require.Greater(t, postingsCap, 0)

	resetLogForReuse(log)

	ct2 := log.GetPayload().GetType().(*commonpb.LogPayload_Apply).Apply.GetLog().GetData().GetPayload().(*commonpb.LedgerLogPayload_CreatedTransaction).CreatedTransaction
	assert.Equal(t, 0, len(ct2.GetTransaction().GetPostings()))
	assert.Equal(t, postingsCap, cap(ct2.GetTransaction().GetPostings()), "Postings capacity preserved")
}

func TestResetLogForReuse_HandlesOneofTypeChange(t *testing.T) {
	t.Parallel()

	// Start with a CreatedTransaction log.
	log := buildTestLog()
	resetLogForReuse(log)

	// The oneof wrapper should still be CreatedTransaction.
	_, ok := log.GetPayload().GetType().(*commonpb.LogPayload_Apply).Apply.GetLog().GetData().GetPayload().(*commonpb.LedgerLogPayload_CreatedTransaction)
	assert.True(t, ok, "CreatedTransaction wrapper preserved after reset")

	// Simulate UnmarshalVT changing the type to SavedMetadata:
	// vtprotobuf checks the oneof type and allocates a new wrapper if different.
	// After reset, the old wrapper is still CreatedTransaction, so vtprotobuf
	// would reuse it if the next log is also CreatedTransaction, or allocate
	// a new wrapper if the type changes.
}

func TestResetLogForReuse_NonApplyLog(t *testing.T) {
	t.Parallel()

	log := &commonpb.Log{
		Sequence: 42,
		Payload: &commonpb.LogPayload{
			Type: &commonpb.LogPayload_CreateLedger{},
		},
	}

	resetLogForReuse(log)

	// Non-Apply payload type should be nil'd.
	assert.NotNil(t, log.GetPayload(), "Payload pointer preserved")
	assert.Nil(t, log.GetPayload().GetType(), "Non-Apply type nil'd")
}

func TestResetLogForReuse_NilPayload(t *testing.T) {
	t.Parallel()

	log := &commonpb.Log{Sequence: 42}
	resetLogForReuse(log)

	assert.Equal(t, uint64(0), log.GetSequence())
	assert.Nil(t, log.GetPayload())
}

func TestResetLogForReuse_RoundTrip(t *testing.T) {
	t.Parallel()

	// Serialize two different logs.
	log1 := buildTestLog()
	data1, err := log1.MarshalVT()
	require.NoError(t, err)

	log2 := &commonpb.Log{
		Sequence: 99999,
		Payload: &commonpb.LogPayload{
			Type: &commonpb.LogPayload_Apply{
				Apply: &commonpb.ApplyLedgerLog{
					LedgerName: "other-ledger",
					Log: &commonpb.LedgerLog{
						Id:   77,
						Date: &commonpb.Timestamp{},
						Data: &commonpb.LedgerLogPayload{
							Payload: &commonpb.LedgerLogPayload_CreatedTransaction{
								CreatedTransaction: &commonpb.CreatedTransaction{
									Transaction: &commonpb.Transaction{
										Id: 500,
										Postings: []*commonpb.Posting{
											{Source: "bank", Destination: "treasury", Amount: &commonpb.Uint256{V0: 9999}, Asset: "GBP"},
										},
										Timestamp:  &commonpb.Timestamp{},
										InsertedAt: &commonpb.Timestamp{},
									},
								},
							},
						},
					},
				},
			},
		},
	}
	data2, err := log2.MarshalVT()
	require.NoError(t, err)

	// Unmarshal log1 into reusable message.
	m := &commonpb.Log{}
	require.NoError(t, m.UnmarshalVT(data1))
	assert.Equal(t, uint64(12345), m.GetSequence())
	assert.Equal(t, "default", m.GetPayload().GetApply().GetLedgerName())

	// Reset for reuse and unmarshal log2.
	resetLogForReuse(m)
	require.NoError(t, m.UnmarshalVT(data2))

	// Verify log2 values — no stale data from log1.
	assert.Equal(t, uint64(99999), m.GetSequence())
	assert.Empty(t, m.GetReceipt(), "Receipt should be empty (not stale)")
	assert.Nil(t, m.GetIdempotency(), "Idempotency should be nil (not stale)")

	apply := m.GetPayload().GetApply()
	assert.Equal(t, "other-ledger", apply.GetLedgerName())
	assert.Equal(t, uint64(77), apply.GetLog().GetId())

	ct := apply.GetLog().GetData().GetCreatedTransaction()
	require.NotNil(t, ct)
	assert.Equal(t, uint64(500), ct.GetTransaction().GetId())
	assert.Empty(t, ct.GetTransaction().GetReference(), "Reference should be empty (not stale)")
	assert.False(t, ct.GetTransaction().GetReverted(), "Reverted should be false (not stale)")
	assert.Nil(t, ct.GetTransaction().GetMetadata(), "Metadata should be nil (not stale)")
	assert.Nil(t, ct.GetTransaction().GetUpdatedAt(), "UpdatedAt should be nil (not stale)")
	assert.Nil(t, ct.GetTransaction().GetRevertedAt(), "RevertedAt should be nil (not stale)")
	assert.Empty(t, ct.GetAccountMetadata(), "AccountMetadata should be empty (not stale)")
	assert.Len(t, ct.GetTransaction().GetPostings(), 1)
	assert.Equal(t, "bank", ct.GetTransaction().GetPostings()[0].GetSource())
}

func BenchmarkResetLog(b *testing.B) {
	// Serialize a test log once.
	log := buildTestLog()
	data, err := log.MarshalVT()
	require.NoError(b, err)

	b.Run("proto.Reset", func(b *testing.B) {
		m := &commonpb.Log{}
		b.ReportAllocs()
		for b.Loop() {
			m.Reset()
			if err := m.UnmarshalVT(data); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("resetLogForReuse", func(b *testing.B) {
		m := &commonpb.Log{}
		b.ReportAllocs()
		for b.Loop() {
			resetLogForReuse(m)
			if err := m.UnmarshalVT(data); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func buildTestLog() *commonpb.Log {
	return &commonpb.Log{
		Sequence: 12345,
		Receipt:  "test-receipt",
		Idempotency: &commonpb.Idempotency{
			Key: "test-key",
		},
		Payload: &commonpb.LogPayload{
			Type: &commonpb.LogPayload_Apply{
				Apply: &commonpb.ApplyLedgerLog{
					LedgerName: "default",
					Log: &commonpb.LedgerLog{
						Id:   42,
						Date: &commonpb.Timestamp{},
						Data: &commonpb.LedgerLogPayload{
							Payload: &commonpb.LedgerLogPayload_CreatedTransaction{
								CreatedTransaction: &commonpb.CreatedTransaction{
									ChapterId: 7,
									Transaction: &commonpb.Transaction{
										Id:        100,
										Reference: "ref-001",
										Reverted:  true,
										Postings: []*commonpb.Posting{
											{Source: "world", Destination: "users:001", Amount: &commonpb.Uint256{V0: 100}, Asset: "USD"},
											{Source: "world", Destination: "users:002", Amount: &commonpb.Uint256{V0: 200}, Asset: "EUR"},
										},
										Metadata: map[string]*commonpb.MetadataValue{
											"type": {},
										},
										Timestamp:  &commonpb.Timestamp{},
										InsertedAt: &commonpb.Timestamp{},
										UpdatedAt:  &commonpb.Timestamp{},
										RevertedAt: &commonpb.Timestamp{},
									},
									AccountMetadata: map[string]*commonpb.MetadataMap{
										"users:001": {Values: map[string]*commonpb.MetadataValue{"type": {}}},
									},
									PreviousAccountMetadata: map[string]*commonpb.MetadataMap{
										"users:001": {Values: map[string]*commonpb.MetadataValue{"type": {}}},
									},
								},
							},
						},
					},
				},
			},
		},
	}
}
