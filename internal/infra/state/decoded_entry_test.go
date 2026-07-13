package state

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"

	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// entWith builds a *raftpb.Entry for decoded_entry_test literals.
func entWith(index, term uint64, t raftpb.EntryType, data []byte) *raftpb.Entry {
	return &raftpb.Entry{
		Index: new(index),
		Term:  new(term),
		Type:  new(t),
		Data:  data,
	}
}

// marshalProposal builds a Proposal with one ledger-apply order and returns
// its wire bytes — the smallest valid payload DecodeEntries will accept.
func marshalProposal(t *testing.T, id uint64) []byte {
	t.Helper()
	cmd := &raftcmdpb.Proposal{
		Id: id,
		Orders: []*raftcmdpb.Order{{Type: &raftcmdpb.Order_LedgerScoped{
			LedgerScoped: &raftcmdpb.LedgerScopedOrder{
				Ledger:  "l",
				Payload: &raftcmdpb.LedgerScopedOrder_Apply{Apply: &raftcmdpb.LedgerApplyOrder{}},
			},
		}}},
	}
	data, err := cmd.MarshalVT()
	require.NoError(t, err)

	return data
}

// marshalCheckpointProposal builds a Proposal whose only order triggers a
// checkpoint (CreateQueryCheckpoint).
func marshalCheckpointProposal(t *testing.T, id uint64) []byte {
	t.Helper()
	cmd := &raftcmdpb.Proposal{
		Id: id,
		Orders: []*raftcmdpb.Order{{Type: &raftcmdpb.Order_SystemScoped{
			SystemScoped: &raftcmdpb.SystemScopedOrder{
				Payload: &raftcmdpb.SystemScopedOrder_CreateQueryCheckpoint{
					CreateQueryCheckpoint: &raftcmdpb.CreateQueryCheckpointOrder{},
				},
			},
		}}},
	}
	data, err := cmd.MarshalVT()
	require.NoError(t, err)

	return data
}

func TestDecodeEntries(t *testing.T) {
	t.Parallel()

	t.Run("nil input", func(t *testing.T) {
		t.Parallel()
		decoded, err := DecodeEntries(nil)
		require.NoError(t, err)
		require.Nil(t, decoded)
	})

	t.Run("empty slice", func(t *testing.T) {
		t.Parallel()
		decoded, err := DecodeEntries([]*raftpb.Entry{})
		require.NoError(t, err)
		require.Nil(t, decoded)
	})

	t.Run("mixed: normal, conf-change, empty-data", func(t *testing.T) {
		t.Parallel()
		entries := []*raftpb.Entry{
			entWith(1, 1, raftpb.EntryNormal, marshalProposal(t, 1)),
			entWith(2, 1, raftpb.EntryConfChange, []byte("ignored payload")),
			entWith(3, 1, raftpb.EntryNormal, nil),
			entWith(4, 1, raftpb.EntryNormal, marshalProposal(t, 4)),
		}

		decoded, err := DecodeEntries(entries)
		require.NoError(t, err)
		require.Len(t, decoded, 4, "DecodeEntries preserves positional alignment")

		defer ReleaseDecodedEntries(decoded)

		require.NotNil(t, decoded[0].Proposal, "normal entry decodes")
		require.Equal(t, uint64(1), decoded[0].Proposal.GetId())

		require.Nil(t, decoded[1].Proposal, "EntryConfChange carries no proposal payload")
		require.Equal(t, raftpb.EntryConfChange, decoded[1].Entry.GetType())

		require.Nil(t, decoded[2].Proposal, "EntryNormal with empty Data carries no proposal payload")

		require.NotNil(t, decoded[3].Proposal)
		require.Equal(t, uint64(4), decoded[3].Proposal.GetId())
	})

	t.Run("malformed payload returns error and nil slice", func(t *testing.T) {
		t.Parallel()
		entries := []*raftpb.Entry{
			entWith(10, 1, raftpb.EntryNormal, marshalProposal(t, 10)),
			entWith(11, 1, raftpb.EntryNormal, []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff}), // not a valid Proposal wire form
		}

		decoded, err := DecodeEntries(entries)
		require.Error(t, err, "malformed Data must surface as error")
		require.Contains(t, err.Error(), "raft index 11")
		require.Nil(t, decoded, "on error, no slice is returned (already-decoded proposals are released internally)")
	})

	t.Run("malformed payload at first position", func(t *testing.T) {
		t.Parallel()
		entries := []*raftpb.Entry{
			entWith(20, 1, raftpb.EntryNormal, []byte{0xff, 0xff, 0xff}),
			entWith(21, 1, raftpb.EntryNormal, marshalProposal(t, 21)),
		}

		decoded, err := DecodeEntries(entries)
		require.Error(t, err)
		require.Nil(t, decoded)
	})
}

func TestReleaseDecodedEntries(t *testing.T) {
	t.Parallel()

	t.Run("nil slice is a no-op", func(t *testing.T) {
		t.Parallel()
		require.NotPanics(t, func() { ReleaseDecodedEntries(nil) })
	})

	t.Run("empty slice is a no-op", func(t *testing.T) {
		t.Parallel()
		require.NotPanics(t, func() { ReleaseDecodedEntries([]DecodedEntry{}) })
	})

	t.Run("nils out Proposal pointers so double-release is safe", func(t *testing.T) {
		t.Parallel()
		entries := []*raftpb.Entry{
			entWith(1, 1, raftpb.EntryNormal, marshalProposal(t, 1)),
			entWith(2, 1, raftpb.EntryConfChange, nil),
			entWith(3, 1, raftpb.EntryNormal, marshalProposal(t, 3)),
		}

		decoded, err := DecodeEntries(entries)
		require.NoError(t, err)
		require.NotNil(t, decoded[0].Proposal)
		require.Nil(t, decoded[1].Proposal)
		require.NotNil(t, decoded[2].Proposal)

		ReleaseDecodedEntries(decoded)

		// After release, every Proposal pointer is nil — a second release
		// is a no-op rather than a double pool return.
		for i := range decoded {
			require.Nil(t, decoded[i].Proposal, "Proposal at %d must be nil after release", i)
		}

		require.NotPanics(t, func() { ReleaseDecodedEntries(decoded) },
			"second release must be safe (idempotent)")
	})
}

func TestDecodedEntryRequiresCheckpoint(t *testing.T) {
	t.Parallel()

	t.Run("nil proposal", func(t *testing.T) {
		t.Parallel()
		require.False(t, DecodedEntryRequiresCheckpoint(DecodedEntry{}))
	})

	t.Run("apply-only proposal", func(t *testing.T) {
		t.Parallel()
		entries := []*raftpb.Entry{
			entWith(1, 1, raftpb.EntryNormal, marshalProposal(t, 1)),
		}
		decoded, err := DecodeEntries(entries)
		require.NoError(t, err)

		defer ReleaseDecodedEntries(decoded)

		require.False(t, DecodedEntryRequiresCheckpoint(decoded[0]))
	})

	t.Run("checkpoint-trigger proposal", func(t *testing.T) {
		t.Parallel()
		entries := []*raftpb.Entry{
			entWith(1, 1, raftpb.EntryNormal, marshalCheckpointProposal(t, 1)),
		}
		decoded, err := DecodeEntries(entries)
		require.NoError(t, err)

		defer ReleaseDecodedEntries(decoded)

		require.True(t, DecodedEntryRequiresCheckpoint(decoded[0]))
	})
}

func TestValidateCheckpointEntryPositionsDecoded(t *testing.T) {
	t.Parallel()

	apply := func(t *testing.T, idx uint64) *raftpb.Entry {
		t.Helper()

		return entWith(idx, 1, raftpb.EntryNormal, marshalProposal(t, idx))
	}
	chkpt := func(t *testing.T, idx uint64) *raftpb.Entry {
		t.Helper()

		return entWith(idx, 1, raftpb.EntryNormal, marshalCheckpointProposal(t, idx))
	}

	confChange := entWith(99, 1, raftpb.EntryConfChange, nil)
	emptyData := entWith(99, 1, raftpb.EntryNormal, nil)

	decode := func(t *testing.T, entries []*raftpb.Entry) []DecodedEntry {
		t.Helper()

		d, err := DecodeEntries(entries)
		require.NoError(t, err)
		t.Cleanup(func() { ReleaseDecodedEntries(d) })

		return d
	}

	t.Run("empty slice", func(t *testing.T) {
		t.Parallel()
		require.NoError(t, ValidateCheckpointEntryPositionsDecoded(nil))
	})

	t.Run("no trigger", func(t *testing.T) {
		t.Parallel()
		require.NoError(t, ValidateCheckpointEntryPositionsDecoded(
			decode(t, []*raftpb.Entry{apply(t, 1), apply(t, 2)}),
		))
	})

	t.Run("trigger last", func(t *testing.T) {
		t.Parallel()
		require.NoError(t, ValidateCheckpointEntryPositionsDecoded(
			decode(t, []*raftpb.Entry{apply(t, 1), chkpt(t, 2)}),
		))
	})

	t.Run("single trigger", func(t *testing.T) {
		t.Parallel()
		require.NoError(t, ValidateCheckpointEntryPositionsDecoded(
			decode(t, []*raftpb.Entry{chkpt(t, 1)}),
		))
	})

	t.Run("trigger first", func(t *testing.T) {
		t.Parallel()
		err := ValidateCheckpointEntryPositionsDecoded(
			decode(t, []*raftpb.Entry{chkpt(t, 1), apply(t, 2)}),
		)
		require.Error(t, err)
		require.Contains(t, err.Error(), "applier must pre-split")
	})

	t.Run("trigger middle", func(t *testing.T) {
		t.Parallel()
		err := ValidateCheckpointEntryPositionsDecoded(
			decode(t, []*raftpb.Entry{apply(t, 1), chkpt(t, 2), apply(t, 3)}),
		)
		require.Error(t, err)
	})

	t.Run("conf-change and empty entries are skipped", func(t *testing.T) {
		t.Parallel()
		require.NoError(t, ValidateCheckpointEntryPositionsDecoded(
			decode(t, []*raftpb.Entry{confChange, emptyData, chkpt(t, 100)}),
		))
	})

	t.Run("nil-proposal entry then trigger last", func(t *testing.T) {
		t.Parallel()
		// emptyData has no Proposal; the trailing chkpt is last, so this is valid.
		require.NoError(t, ValidateCheckpointEntryPositionsDecoded(
			decode(t, []*raftpb.Entry{apply(t, 1), emptyData, chkpt(t, 100)}),
		))
	})
}
