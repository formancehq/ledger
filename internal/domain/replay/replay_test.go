package replay_test

import (
	"errors"
	"math/big"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain/accounttype"
	"github.com/formancehq/ledger/v3/internal/domain/replay"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// writerStub satisfies replay.Writer with no-ops; the index hooks are
// overridable so the dispatch tests can observe calls and inject failures.
type writerStub struct {
	createIndex func(ledger string, id *commonpb.IndexID, createdAt *commonpb.Timestamp) error
	dropIndex   func(ledger string, id *commonpb.IndexID) error

	removedFieldTypes int
}

func (w *writerStub) AddVolumeDelta([]byte, *big.Int, *big.Int) error { return nil }
func (w *writerStub) GetVolume([]byte) (*raftcmdpb.VolumePair, error) { return nil, nil }
func (w *writerStub) DeleteVolume([]byte) error                       { return nil }
func (w *writerStub) MoveVolume([]byte, []byte) error                 { return nil }
func (w *writerStub) SetMetadata([]byte, *commonpb.MetadataValue) error {
	return nil
}
func (w *writerStub) DeleteMetadata([]byte) error       { return nil }
func (w *writerStub) MoveMetadata([]byte, []byte) error { return nil }
func (w *writerStub) CreateTransaction([]byte, uint64, *commonpb.Timestamp, map[string]*commonpb.MetadataValue, []*commonpb.Posting, uint64) error {
	return nil
}
func (w *writerStub) SetTransactionReference(string, string, uint64) error { return nil }
func (w *writerStub) SetRevertedBy([]byte, uint64, *commonpb.Timestamp) error {
	return nil
}
func (w *writerStub) SaveTxMetadata([]byte, map[string]*commonpb.MetadataValue) error {
	return nil
}
func (w *writerStub) DeleteTxMetadata([]byte, string) error { return nil }
func (w *writerStub) SetMetadataFieldType(string, commonpb.TargetType, string, commonpb.MetadataType) error {
	return nil
}

func (w *writerStub) RemoveMetadataFieldType(string, commonpb.TargetType, string) error {
	w.removedFieldTypes++

	return nil
}

func (w *writerStub) CreateIndex(ledger string, id *commonpb.IndexID, createdAt *commonpb.Timestamp) error {
	if w.createIndex != nil {
		return w.createIndex(ledger, id, createdAt)
	}

	return nil
}

func (w *writerStub) DropIndex(ledger string, id *commonpb.IndexID) error {
	if w.dropIndex != nil {
		return w.dropIndex(ledger, id)
	}

	return nil
}

func (w *writerStub) AddAccountType(string, *commonpb.AccountType) error { return nil }
func (w *writerStub) RemoveAccountType(string, string) error             { return nil }
func (w *writerStub) SetDefaultEnforcementMode(string, commonpb.ChartEnforcementMode) error {
	return nil
}

func metaIndexID(key string) *commonpb.IndexID {
	return &commonpb.IndexID{
		Kind: &commonpb.IndexID_Metadata{
			Metadata: &commonpb.MetadataIndexID{
				Target: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
				Key:    key,
			},
		},
	}
}

func replayOne(t *testing.T, w replay.Writer, date *commonpb.Timestamp, payload *commonpb.LedgerLogPayload) error {
	t.Helper()

	return replay.ReplayLedgerLog("ledger", 1, payload, date, w,
		map[string]map[string]*commonpb.AccountType{},
		map[string][]accounttype.CompiledType{}, nil)
}

func TestReplayLedgerLog_CreateIndexDispatch(t *testing.T) {
	t.Parallel()

	id := metaIndexID("k0")
	date := &commonpb.Timestamp{Data: 42}

	var gotLedger string
	var gotID *commonpb.IndexID
	var gotDate *commonpb.Timestamp

	w := &writerStub{createIndex: func(ledger string, id *commonpb.IndexID, createdAt *commonpb.Timestamp) error {
		gotLedger, gotID, gotDate = ledger, id, createdAt

		return nil
	}}

	require.NoError(t, replayOne(t, w, date, &commonpb.LedgerLogPayload{
		Payload: &commonpb.LedgerLogPayload_CreateIndex{
			CreateIndex: &commonpb.CreatedIndexLog{Id: id},
		},
	}))
	require.Equal(t, "ledger", gotLedger)
	require.Same(t, id, gotID)
	require.Same(t, date, gotDate)

	// A malformed log with no id is skipped, not dispatched.
	called := false
	w = &writerStub{createIndex: func(string, *commonpb.IndexID, *commonpb.Timestamp) error {
		called = true

		return nil
	}}
	require.NoError(t, replayOne(t, w, date, &commonpb.LedgerLogPayload{
		Payload: &commonpb.LedgerLogPayload_CreateIndex{CreateIndex: &commonpb.CreatedIndexLog{}},
	}))
	require.False(t, called)

	// A writer failure surfaces.
	boom := errors.New("boom")
	w = &writerStub{createIndex: func(string, *commonpb.IndexID, *commonpb.Timestamp) error { return boom }}
	require.ErrorIs(t, replayOne(t, w, date, &commonpb.LedgerLogPayload{
		Payload: &commonpb.LedgerLogPayload_CreateIndex{
			CreateIndex: &commonpb.CreatedIndexLog{Id: id},
		},
	}), boom)
}

func TestReplayLedgerLog_DropIndexDispatch(t *testing.T) {
	t.Parallel()

	id := metaIndexID("k0")

	var gotID *commonpb.IndexID
	w := &writerStub{dropIndex: func(_ string, id *commonpb.IndexID) error {
		gotID = id

		return nil
	}}

	require.NoError(t, replayOne(t, w, nil, &commonpb.LedgerLogPayload{
		Payload: &commonpb.LedgerLogPayload_DropIndex{
			DropIndex: &commonpb.DroppedIndexLog{Id: id},
		},
	}))
	require.Same(t, id, gotID)

	called := false
	w = &writerStub{dropIndex: func(string, *commonpb.IndexID) error {
		called = true

		return nil
	}}
	require.NoError(t, replayOne(t, w, nil, &commonpb.LedgerLogPayload{
		Payload: &commonpb.LedgerLogPayload_DropIndex{DropIndex: &commonpb.DroppedIndexLog{}},
	}))
	require.False(t, called)

	boom := errors.New("boom")
	w = &writerStub{dropIndex: func(string, *commonpb.IndexID) error { return boom }}
	require.ErrorIs(t, replayOne(t, w, nil, &commonpb.LedgerLogPayload{
		Payload: &commonpb.LedgerLogPayload_DropIndex{
			DropIndex: &commonpb.DroppedIndexLog{Id: id},
		},
	}), boom)
}

func TestReplayLedgerLog_RemovedFieldTypeCascade(t *testing.T) {
	t.Parallel()

	id := metaIndexID("k0")

	// The cascade drops exactly the index the log names.
	var gotID *commonpb.IndexID
	w := &writerStub{dropIndex: func(_ string, id *commonpb.IndexID) error {
		gotID = id

		return nil
	}}

	require.NoError(t, replayOne(t, w, nil, &commonpb.LedgerLogPayload{
		Payload: &commonpb.LedgerLogPayload_RemovedMetadataFieldType{
			RemovedMetadataFieldType: &commonpb.RemovedMetadataFieldTypeLog{
				TargetType:   commonpb.TargetType_TARGET_TYPE_ACCOUNT,
				Key:          "k0",
				DroppedIndex: id,
			},
		},
	}))
	require.Equal(t, 1, w.removedFieldTypes)
	require.Same(t, id, gotID)

	// A removal that dropped nothing leaves the registry untouched.
	called := false
	w = &writerStub{dropIndex: func(string, *commonpb.IndexID) error {
		called = true

		return nil
	}}
	require.NoError(t, replayOne(t, w, nil, &commonpb.LedgerLogPayload{
		Payload: &commonpb.LedgerLogPayload_RemovedMetadataFieldType{
			RemovedMetadataFieldType: &commonpb.RemovedMetadataFieldTypeLog{
				TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
				Key:        "k0",
			},
		},
	}))
	require.Equal(t, 1, w.removedFieldTypes)
	require.False(t, called)

	// A cascade failure surfaces.
	boom := errors.New("boom")
	w = &writerStub{dropIndex: func(string, *commonpb.IndexID) error { return boom }}
	require.ErrorIs(t, replayOne(t, w, nil, &commonpb.LedgerLogPayload{
		Payload: &commonpb.LedgerLogPayload_RemovedMetadataFieldType{
			RemovedMetadataFieldType: &commonpb.RemovedMetadataFieldTypeLog{
				TargetType:   commonpb.TargetType_TARGET_TYPE_ACCOUNT,
				Key:          "k0",
				DroppedIndex: id,
			},
		},
	}), boom)
}
