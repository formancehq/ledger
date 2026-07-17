package check

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
	domainreplay "github.com/formancehq/ledger/v3/internal/domain/replay"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// pcvRow is a single (account, asset, color) post-commit volume row for the
// test builder below.
type pcvRow struct {
	account, asset, color, input, output string
}

func buildPCV(rows ...pcvRow) *commonpb.PostCommitVolumes {
	byAccount := map[string]*commonpb.VolumesByAssets{}
	for _, r := range rows {
		byAccount[r.account] = &commonpb.VolumesByAssets{
			Volumes: append(byAccount[r.account].GetVolumes(), &commonpb.VolumeEntry{
				Asset:   r.asset,
				Color:   r.color,
				Volumes: &commonpb.Volumes{Input: r.input, Output: r.output},
			}),
		}
	}

	return &commonpb.PostCommitVolumes{VolumesByAccount: byAccount}
}

func coloredPosting(source, destination, asset, color string, amount int64) *commonpb.Posting {
	p := newPosting(source, destination, asset, amount)
	p.Color = color

	return p
}

// runPCVCheck applies postings to a fresh replay store (mirroring the checker's
// pre-purge replay state) and runs compareTransactionPostCommitVolumes against a
// created transaction carrying pcv. It returns the VOLUME_MISMATCH messages.
func runPCVCheck(t *testing.T, postings []*commonpb.Posting, pcv *commonpb.PostCommitVolumes) []string {
	t.Helper()

	rs := newTestReplayStore(t)
	require.NoError(t, domainreplay.ApplyPostings("ledger", postings, rs))

	data := &commonpb.LedgerLogPayload{
		Payload: &commonpb.LedgerLogPayload_CreatedTransaction{
			CreatedTransaction: &commonpb.CreatedTransaction{
				Transaction: &commonpb.Transaction{Id: 1, Postings: postings, PostCommitVolumes: pcv},
			},
		},
	}

	var msgs []string

	err := compareTransactionPostCommitVolumes("ledger", 7, data, rs, nil, func(e *servicepb.CheckStoreEvent) {
		if ev := e.GetError(); ev != nil &&
			ev.GetErrorType() == servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_VOLUME_MISMATCH {
			msgs = append(msgs, ev.GetMessage())
		}
	})
	require.NoError(t, err)

	return msgs
}

func TestCompareTransactionPostCommitVolumes_Valid(t *testing.T) {
	t.Parallel()

	postings := []*commonpb.Posting{newPosting("world", "alice", "USD", 100)}
	pcv := buildPCV(
		pcvRow{account: "world", asset: "USD", input: "0", output: "100"},
		pcvRow{account: "alice", asset: "USD", input: "100", output: "0"},
	)

	require.Empty(t, runPCVCheck(t, postings, pcv), "a correct snapshot must not emit any mismatch")
}

func TestCompareTransactionPostCommitVolumes_DetectsMissingRow(t *testing.T) {
	t.Parallel()

	postings := []*commonpb.Posting{newPosting("world", "alice", "USD", 100)}
	// Drop the alice row.
	pcv := buildPCV(pcvRow{account: "world", asset: "USD", input: "0", output: "100"})

	msgs := runPCVCheck(t, postings, pcv)
	require.Len(t, msgs, 1)
	require.Contains(t, msgs[0], "is missing")
	require.Contains(t, msgs[0], "alice")
}

func TestCompareTransactionPostCommitVolumes_DetectsExtraRow(t *testing.T) {
	t.Parallel()

	postings := []*commonpb.Posting{newPosting("world", "alice", "USD", 100)}
	pcv := buildPCV(
		pcvRow{account: "world", asset: "USD", input: "0", output: "100"},
		pcvRow{account: "alice", asset: "USD", input: "100", output: "0"},
		// bob was never touched by the postings.
		pcvRow{account: "bob", asset: "USD", input: "5", output: "0"},
	)

	msgs := runPCVCheck(t, postings, pcv)
	require.Len(t, msgs, 1)
	require.Contains(t, msgs[0], "unexpected")
	require.Contains(t, msgs[0], "bob")
}

func TestCompareTransactionPostCommitVolumes_DetectsModifiedRow(t *testing.T) {
	t.Parallel()

	postings := []*commonpb.Posting{newPosting("world", "alice", "USD", 100)}
	pcv := buildPCV(
		pcvRow{account: "world", asset: "USD", input: "0", output: "100"},
		// alice output tampered from 0 to 100.
		pcvRow{account: "alice", asset: "USD", input: "100", output: "100"},
	)

	msgs := runPCVCheck(t, postings, pcv)
	require.Len(t, msgs, 1)
	require.Contains(t, msgs[0], "mismatch")
	require.Contains(t, msgs[0], "alice")
}

func TestCompareTransactionPostCommitVolumes_DetectsDuplicateRow(t *testing.T) {
	t.Parallel()

	postings := []*commonpb.Posting{newPosting("world", "alice", "USD", 100)}
	pcv := buildPCV(
		pcvRow{account: "world", asset: "USD", input: "0", output: "100"},
		pcvRow{account: "alice", asset: "USD", input: "100", output: "0"},
		// duplicate alice/USD row.
		pcvRow{account: "alice", asset: "USD", input: "100", output: "0"},
	)

	msgs := runPCVCheck(t, postings, pcv)
	require.Len(t, msgs, 1)
	require.Contains(t, msgs[0], "duplicate")
}

func TestCompareTransactionPostCommitVolumes_ColorsAreDistinctTuples(t *testing.T) {
	t.Parallel()

	postings := []*commonpb.Posting{
		coloredPosting("world", "alice", "USD", "RED", 100),
		coloredPosting("world", "alice", "USD", "", 50),
	}

	t.Run("correct per-color rows pass", func(t *testing.T) {
		t.Parallel()

		pcv := buildPCV(
			pcvRow{account: "world", asset: "USD", color: "RED", input: "0", output: "100"},
			pcvRow{account: "world", asset: "USD", color: "", input: "0", output: "50"},
			pcvRow{account: "alice", asset: "USD", color: "RED", input: "100", output: "0"},
			pcvRow{account: "alice", asset: "USD", color: "", input: "50", output: "0"},
		)

		require.Empty(t, runPCVCheck(t, postings, pcv))
	})

	t.Run("a missing color bucket is a missing row", func(t *testing.T) {
		t.Parallel()

		// Omit the uncolored alice bucket.
		pcv := buildPCV(
			pcvRow{account: "world", asset: "USD", color: "RED", input: "0", output: "100"},
			pcvRow{account: "world", asset: "USD", color: "", input: "0", output: "50"},
			pcvRow{account: "alice", asset: "USD", color: "RED", input: "100", output: "0"},
		)

		msgs := runPCVCheck(t, postings, pcv)
		require.Len(t, msgs, 1)
		require.Contains(t, msgs[0], "is missing")
		require.Contains(t, msgs[0], "alice")
	})
}

func TestCompareTransactionPostCommitVolumes_RevertBranch(t *testing.T) {
	t.Parallel()

	// The compensating transaction carries its own post-revert snapshot on the
	// RevertTransaction; a tampered value must surface.
	postings := []*commonpb.Posting{newPosting("alice", "world", "USD", 100)}

	rs := newTestReplayStore(t)
	require.NoError(t, domainreplay.ApplyPostings("ledger", postings, rs))

	data := &commonpb.LedgerLogPayload{
		Payload: &commonpb.LedgerLogPayload_RevertedTransaction{
			RevertedTransaction: &commonpb.RevertedTransaction{
				RevertedTransactionId: 1,
				RevertTransaction: &commonpb.Transaction{
					Id:       2,
					Postings: postings,
					PostCommitVolumes: buildPCV(
						pcvRow{account: "alice", asset: "USD", input: "0", output: "999"}, // tampered
						pcvRow{account: "world", asset: "USD", input: "100", output: "0"},
					),
				},
			},
		},
	}

	var msgs []string

	err := compareTransactionPostCommitVolumes("ledger", 7, data, rs, nil, func(e *servicepb.CheckStoreEvent) {
		if ev := e.GetError(); ev != nil &&
			ev.GetErrorType() == servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_VOLUME_MISMATCH {
			msgs = append(msgs, ev.GetMessage())
		}
	})
	require.NoError(t, err)
	require.Len(t, msgs, 1)
	require.Contains(t, msgs[0], "mismatch")
	require.Contains(t, msgs[0], "alice")
}

// TestCompareTransactionPostCommitVolumes_ArchivedBaseline reproduces the
// archived-store false positive NumaryBot flagged: after archiving, the replay
// store holds only post-archive deltas, so the expected snapshot must add the
// pre-archive baseline volume. A post-archive transaction that touches an
// account funded before the boundary must validate against baseline + delta.
func TestCompareTransactionPostCommitVolumes_ArchivedBaseline(t *testing.T) {
	t.Parallel()

	// treasury was funded to {in:1_000_000, out:0} before the archive boundary;
	// that lives in the baseline checkpoint, not the post-archive replay.
	treasuryKey := domain.NewVolumeKey("ledger", "treasury", "USD", "")
	baseline := map[string]*raftcmdpb.VolumePair{
		string(treasuryKey.Bytes()): {
			Input:  commonpb.NewUint256FromUint64(1_000_000),
			Output: commonpb.NewUint256FromUint64(0),
		},
	}

	// Post-archive tx: treasury -> alice 100. Replay only sees this delta.
	postings := []*commonpb.Posting{newPosting("treasury", "alice", "USD", 100)}

	rs := newTestReplayStore(t)
	require.NoError(t, domainreplay.ApplyPostings("ledger", postings, rs))

	// The FSM snapshot is the live cumulative: treasury {1_000_000, 100},
	// alice {100, 0}.
	data := &commonpb.LedgerLogPayload{
		Payload: &commonpb.LedgerLogPayload_CreatedTransaction{
			CreatedTransaction: &commonpb.CreatedTransaction{
				Transaction: &commonpb.Transaction{Id: 1, Postings: postings, PostCommitVolumes: buildPCV(
					pcvRow{account: "treasury", asset: "USD", input: "1000000", output: "100"},
					pcvRow{account: "alice", asset: "USD", input: "100", output: "0"},
				)},
			},
		},
	}

	collect := func() []string {
		var msgs []string

		err := compareTransactionPostCommitVolumes("ledger", 7, data, rs, baseline, func(e *servicepb.CheckStoreEvent) {
			if ev := e.GetError(); ev != nil &&
				ev.GetErrorType() == servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_VOLUME_MISMATCH {
				msgs = append(msgs, ev.GetMessage())
			}
		})
		require.NoError(t, err)

		return msgs
	}

	// With baseline accounted for, the snapshot validates cleanly.
	require.Empty(t, collect(), "baseline + delta must match the stored snapshot")

	// Sanity: without the baseline (the bug) the treasury input would look
	// tampered — proving the baseline is what makes this correct.
	var withoutBaseline []string
	require.NoError(t, compareTransactionPostCommitVolumes("ledger", 7, data, rs, nil, func(e *servicepb.CheckStoreEvent) {
		if ev := e.GetError(); ev != nil &&
			ev.GetErrorType() == servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_VOLUME_MISMATCH {
			withoutBaseline = append(withoutBaseline, ev.GetMessage())
		}
	}))
	require.Len(t, withoutBaseline, 1)
	require.Contains(t, withoutBaseline[0], "treasury")
}
