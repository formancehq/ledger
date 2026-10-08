package replay_test

import (
	"errors"
	"math/big"
	"testing"

	"github.com/stretchr/testify/require"

	ledgerpb "github.com/formancehq/ledger/pkg/client/v3/grpc"

	"github.com/formancehq/ledger/v3/internal/domain/accounttype"
	"github.com/formancehq/ledger/v3/internal/domain/replay"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// writerStub satisfies replay.Writer with no-ops; the index hooks are
// overridable so the dispatch tests can observe calls and inject failures.
type writerStub struct {
	createIndex func(ledger string, id *ledgerpb.IndexID, createdAt *ledgerpb.Timestamp) error
	dropIndex   func(ledger string, id *ledgerpb.IndexID) error

	removedFieldTypes int
	purgedAccounts    []string
}

type livenessWriterStub struct{ writerStub }

func (w *livenessWriterStub) AccountHasNonZeroVolume(string, string) (bool, error) { return false, nil }

func (w *writerStub) AddVolumeDelta([]byte, *big.Int, *big.Int) error { return nil }
func (w *writerStub) GetVolume([]byte) (*raftcmdpb.VolumePair, error) { return nil, nil }
func (w *writerStub) DeleteVolume([]byte) error                       { return nil }
func (w *writerStub) MoveVolume([]byte, []byte) error                 { return nil }
func (w *writerStub) SetMetadata([]byte, *ledgerpb.MetadataValue) error {
	return nil
}
func (w *writerStub) DeleteMetadata([]byte) error { return nil }
func (w *writerStub) PurgeAccount(_ string, account string, _ replay.ExclusionCollector) error {
	w.purgedAccounts = append(w.purgedAccounts, account)

	return nil
}
func (w *writerStub) MoveMetadata([]byte, []byte) error { return nil }
func (w *writerStub) CreateTransaction([]byte, uint64, *ledgerpb.Timestamp, map[string]*ledgerpb.MetadataValue, []*ledgerpb.Posting, uint64) error {
	return nil
}
func (w *writerStub) SetTransactionReference(string, string, uint64) error { return nil }
func (w *writerStub) SetRevertedBy([]byte, uint64, *ledgerpb.Timestamp) error {
	return nil
}
func (w *writerStub) SaveTxMetadata([]byte, map[string]*ledgerpb.MetadataValue) error {
	return nil
}
func (w *writerStub) DeleteTxMetadata([]byte, string) error { return nil }
func (w *writerStub) SetMetadataFieldType(string, ledgerpb.TargetType, string, ledgerpb.MetadataType) error {
	return nil
}

func (w *writerStub) RemoveMetadataFieldType(string, ledgerpb.TargetType, string) error {
	w.removedFieldTypes++

	return nil
}

func (w *writerStub) CreateIndex(ledger string, id *ledgerpb.IndexID, createdAt *ledgerpb.Timestamp) error {
	if w.createIndex != nil {
		return w.createIndex(ledger, id, createdAt)
	}

	return nil
}

func (w *writerStub) DropIndex(ledger string, id *ledgerpb.IndexID) error {
	if w.dropIndex != nil {
		return w.dropIndex(ledger, id)
	}

	return nil
}

func (w *writerStub) AddAccountType(string, *ledgerpb.AccountType) error { return nil }
func (w *writerStub) RemoveAccountType(string, string) error             { return nil }
func (w *writerStub) SetDefaultEnforcementMode(string, ledgerpb.ChartEnforcementMode) error {
	return nil
}

func metaIndexID(key string) *ledgerpb.IndexID {
	return &ledgerpb.IndexID{
		Kind: &ledgerpb.IndexID_Metadata{
			Metadata: &ledgerpb.MetadataIndexID{
				Target: ledgerpb.TargetType_TARGET_TYPE_ACCOUNT,
				Key:    key,
			},
		},
	}
}

func TestReplayLedgerLog_DefersExplicitAccountPurgeToProposalBoundary(t *testing.T) {
	t.Parallel()

	w := &writerStub{}
	buffer := replay.NewEphemeralPurgeBuffer()
	err := replay.ReplayLedgerLog(
		"ledger",
		1,
		&ledgerpb.LedgerLogPayload{},
		[]string{"ephemeral:1"},
		nil,
		w,
		nil,
		nil,
		buffer,
	)
	require.NoError(t, err)
	require.Empty(t, w.purgedAccounts, "transaction post-commit volumes are checked before the proposal boundary")

	require.NoError(t, buffer.Flush(w, nil, nil))
	require.Equal(t, []string{"ephemeral:1"}, w.purgedAccounts)
}

func TestReplayLedgerLogEmptyMetadataDoesNotCreatePurgeCandidate(t *testing.T) {
	t.Parallel()

	w := &livenessWriterStub{}
	buffer := replay.NewEphemeralPurgeBuffer()
	types := map[string][]accounttype.CompiledType{
		"ledger": accounttype.CompileTypes(map[string]*ledgerpb.AccountType{
			"ephemeral": {Name: "ephemeral", Pattern: "ephemeral:{id}", Persistence: ledgerpb.AccountTypePersistence_ACCOUNT_TYPE_EPHEMERAL},
		}),
	}
	require.NoError(t, replay.ReplayLedgerLog(
		"ledger", 1,
		&ledgerpb.LedgerLogPayload{Payload: &ledgerpb.LedgerLogPayload_SavedMetadata{SavedMetadata: &ledgerpb.SavedMetadata{
			Target: &ledgerpb.Target{Target: &ledgerpb.Target_Account{Account: &ledgerpb.TargetAccount{Addr: "ephemeral:1"}}},
		}}},
		nil, nil, w, map[string]map[string]*ledgerpb.AccountType{}, types, buffer,
	))
	require.NoError(t, buffer.Flush(w, types, nil))
	require.Empty(t, w.purgedAccounts)
}

func TestReplayLedgerLogWorldMetadataCreatesPurgeCandidate(t *testing.T) {
	t.Parallel()

	w := &livenessWriterStub{}
	buffer := replay.NewEphemeralPurgeBuffer()
	types := map[string][]accounttype.CompiledType{
		"ledger": accounttype.CompileTypes(map[string]*ledgerpb.AccountType{
			"world": {Name: "world", Pattern: "world", Persistence: ledgerpb.AccountTypePersistence_ACCOUNT_TYPE_EPHEMERAL},
		}),
	}
	require.NoError(t, replay.ReplayLedgerLog(
		"ledger", 1,
		&ledgerpb.LedgerLogPayload{Payload: &ledgerpb.LedgerLogPayload_SavedMetadata{SavedMetadata: &ledgerpb.SavedMetadata{
			Target:   &ledgerpb.Target{Target: &ledgerpb.Target_Account{Account: &ledgerpb.TargetAccount{Addr: "world"}}},
			Metadata: map[string]*ledgerpb.MetadataValue{"status": ledgerpb.NewStringValue("active")},
		}}},
		nil, nil, w, map[string]map[string]*ledgerpb.AccountType{}, types, buffer,
	))
	require.NoError(t, buffer.Flush(w, types, nil))
	require.Equal(t, []string{"world"}, w.purgedAccounts)
}

func TestPostingOnlyWorldCreatesPurgeCandidate(t *testing.T) {
	t.Parallel()

	w := &livenessWriterStub{}
	buffer := replay.NewEphemeralPurgeBuffer()
	types := map[string][]accounttype.CompiledType{
		"ledger": accounttype.CompileTypes(map[string]*ledgerpb.AccountType{
			"world": {Name: "world", Pattern: "world", Persistence: ledgerpb.AccountTypePersistence_ACCOUNT_TYPE_EPHEMERAL},
		}),
	}
	postings := []*ledgerpb.Posting{{Source: "world", Destination: "alice", Asset: "USD", Amount: ledgerpb.NewUint256FromUint64(1)}}
	buffer.Add("ledger", postings)
	require.NoError(t, buffer.Flush(w, types, nil))
	require.Equal(t, []string{"world"}, w.purgedAccounts)
}

func replayOne(t *testing.T, w replay.Writer, date *ledgerpb.Timestamp, payload *ledgerpb.LedgerLogPayload) error {
	t.Helper()

	return replay.ReplayLedgerLog("ledger", 1, payload, nil, date, w,
		map[string]map[string]*ledgerpb.AccountType{},
		map[string][]accounttype.CompiledType{}, nil)
}

func TestReplayLedgerLog_CreateIndexDispatch(t *testing.T) {
	t.Parallel()

	id := metaIndexID("k0")
	date := &ledgerpb.Timestamp{Data: 42}

	var gotLedger string
	var gotID *ledgerpb.IndexID
	var gotDate *ledgerpb.Timestamp

	w := &writerStub{createIndex: func(ledger string, id *ledgerpb.IndexID, createdAt *ledgerpb.Timestamp) error {
		gotLedger, gotID, gotDate = ledger, id, createdAt

		return nil
	}}

	require.NoError(t, replayOne(t, w, date, &ledgerpb.LedgerLogPayload{
		Payload: &ledgerpb.LedgerLogPayload_CreateIndex{
			CreateIndex: &ledgerpb.CreatedIndexLog{Id: id},
		},
	}))
	require.Equal(t, "ledger", gotLedger)
	require.Same(t, id, gotID)
	require.Same(t, date, gotDate)

	// A malformed log with no id is skipped, not dispatched.
	called := false
	w = &writerStub{createIndex: func(string, *ledgerpb.IndexID, *ledgerpb.Timestamp) error {
		called = true

		return nil
	}}
	require.NoError(t, replayOne(t, w, date, &ledgerpb.LedgerLogPayload{
		Payload: &ledgerpb.LedgerLogPayload_CreateIndex{CreateIndex: &ledgerpb.CreatedIndexLog{}},
	}))
	require.False(t, called)

	// A writer failure surfaces.
	boom := errors.New("boom")
	w = &writerStub{createIndex: func(string, *ledgerpb.IndexID, *ledgerpb.Timestamp) error { return boom }}
	require.ErrorIs(t, replayOne(t, w, date, &ledgerpb.LedgerLogPayload{
		Payload: &ledgerpb.LedgerLogPayload_CreateIndex{
			CreateIndex: &ledgerpb.CreatedIndexLog{Id: id},
		},
	}), boom)
}

func TestReplayLedgerLog_DropIndexDispatch(t *testing.T) {
	t.Parallel()

	id := metaIndexID("k0")

	var gotID *ledgerpb.IndexID
	w := &writerStub{dropIndex: func(_ string, id *ledgerpb.IndexID) error {
		gotID = id

		return nil
	}}

	require.NoError(t, replayOne(t, w, nil, &ledgerpb.LedgerLogPayload{
		Payload: &ledgerpb.LedgerLogPayload_DropIndex{
			DropIndex: &ledgerpb.DroppedIndexLog{Id: id},
		},
	}))
	require.Same(t, id, gotID)

	called := false
	w = &writerStub{dropIndex: func(string, *ledgerpb.IndexID) error {
		called = true

		return nil
	}}
	require.NoError(t, replayOne(t, w, nil, &ledgerpb.LedgerLogPayload{
		Payload: &ledgerpb.LedgerLogPayload_DropIndex{DropIndex: &ledgerpb.DroppedIndexLog{}},
	}))
	require.False(t, called)

	boom := errors.New("boom")
	w = &writerStub{dropIndex: func(string, *ledgerpb.IndexID) error { return boom }}
	require.ErrorIs(t, replayOne(t, w, nil, &ledgerpb.LedgerLogPayload{
		Payload: &ledgerpb.LedgerLogPayload_DropIndex{
			DropIndex: &ledgerpb.DroppedIndexLog{Id: id},
		},
	}), boom)
}

func TestReplayLedgerLog_RemovedFieldTypeCascade(t *testing.T) {
	t.Parallel()

	id := metaIndexID("k0")

	// The cascade drops exactly the index the log names.
	var gotID *ledgerpb.IndexID
	w := &writerStub{dropIndex: func(_ string, id *ledgerpb.IndexID) error {
		gotID = id

		return nil
	}}

	require.NoError(t, replayOne(t, w, nil, &ledgerpb.LedgerLogPayload{
		Payload: &ledgerpb.LedgerLogPayload_RemovedMetadataFieldType{
			RemovedMetadataFieldType: &ledgerpb.RemovedMetadataFieldTypeLog{
				TargetType:   ledgerpb.TargetType_TARGET_TYPE_ACCOUNT,
				Key:          "k0",
				DroppedIndex: id,
			},
		},
	}))
	require.Equal(t, 1, w.removedFieldTypes)
	require.Same(t, id, gotID)

	// A removal that dropped nothing leaves the registry untouched.
	called := false
	w = &writerStub{dropIndex: func(string, *ledgerpb.IndexID) error {
		called = true

		return nil
	}}
	require.NoError(t, replayOne(t, w, nil, &ledgerpb.LedgerLogPayload{
		Payload: &ledgerpb.LedgerLogPayload_RemovedMetadataFieldType{
			RemovedMetadataFieldType: &ledgerpb.RemovedMetadataFieldTypeLog{
				TargetType: ledgerpb.TargetType_TARGET_TYPE_ACCOUNT,
				Key:        "k0",
			},
		},
	}))
	require.Equal(t, 1, w.removedFieldTypes)
	require.False(t, called)

	// A cascade failure surfaces.
	boom := errors.New("boom")
	w = &writerStub{dropIndex: func(string, *ledgerpb.IndexID) error { return boom }}
	require.ErrorIs(t, replayOne(t, w, nil, &ledgerpb.LedgerLogPayload{
		Payload: &ledgerpb.LedgerLogPayload_RemovedMetadataFieldType{
			RemovedMetadataFieldType: &ledgerpb.RemovedMetadataFieldTypeLog{
				TargetType:   ledgerpb.TargetType_TARGET_TYPE_ACCOUNT,
				Key:          "k0",
				DroppedIndex: id,
			},
		},
	}), boom)
}
