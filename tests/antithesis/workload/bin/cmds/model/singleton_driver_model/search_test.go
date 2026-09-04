package main

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/pkg/actions"
	"github.com/formancehq/ledger/v3/tests/oracle"
	"github.com/formancehq/ledger/v3/tests/oracle/oracletest"
)

func bulkOf(reqs ...*servicepb.Request) oracle.Bulk { return oracle.Bulk{Requests: reqs} }

func collectBases(c *Checker) []oracle.GlobalState {
	var out []oracle.GlobalState
	// ^uint64(0): no high-water bound — fold every in-flight/pending bulk.
	c.candidateBases(^uint64(0), nil, func(b oracle.GlobalState) bool {
		out = append(out, b)

		return false
	})

	return out
}

// candidateBases must not fold an operation dispatched after the observation's
// high-water — it could not have preceded the observation, and folding it would
// invent a state the server was never in.
func TestCandidateBases_BoundsInflightByMaxTicket(t *testing.T) {
	t.Parallel()

	c := NewChecker([]string{"L"}, nil)
	c.inflight[5] = bulkOf(oracletest.AddTypeReq("T"))

	collect := func(maxTicket uint64) []oracle.GlobalState {
		var out []oracle.GlobalState
		c.candidateBases(maxTicket, nil, func(b oracle.GlobalState) bool {
			out = append(out, b)

			return false
		})

		return out
	}

	// Below the in-flight ticket: only modelState — the future bulk is excluded.
	require.Len(t, collect(4), 1)
	// At/above it: the bulk is folded (modelState + the add).
	require.Len(t, collect(5), 2)
}

// pending is minSeq-ordered, so once an entry is dispatched after the high-water
// the rest are too: candidateBases truncates pending there.
func TestCandidateBases_TruncatesPendingByMaxTicket(t *testing.T) {
	t.Parallel()

	c := NewChecker([]string{"L"}, nil)
	c.pending = []*pendingObservation{
		{minSeq: 1, obs: observation{bulk: bulkOf(oracletest.AddTypeReq("A")), ticket: 2}},
		{minSeq: 2, obs: observation{bulk: bulkOf(oracletest.AddTypeReq("B")), ticket: 6}},
	}

	var bases []oracle.GlobalState
	// maxTicket 4: P1 (ticket 2) folds, P2 (ticket 6) is excluded — states are
	// modelState and modelState+P1, never +P1+P2.
	c.candidateBases(4, nil, func(b oracle.GlobalState) bool {
		bases = append(bases, b)

		return false
	})
	require.Len(t, bases, 2)
}

func TestCandidateBases_FoldsInflight(t *testing.T) {
	t.Parallel()

	c := NewChecker([]string{"L"}, nil)
	c.inflight[1] = bulkOf(oracletest.AddTypeReq("T"))

	// modelState (empty) and the state with the in-flight add folded in.
	require.Len(t, collectBases(c), 2)
}

// Pending bulks may only appear as an ordered prefix, never reordered. P1
// declares type a as EPHEMERAL; P2 funds then drains a:1 (net zero). In commit
// order P1→P2 the cell is EPHEMERAL with a zero balance, so it's purged and a:1
// is absent in every legal state. The reversed P2→P1 would commit a:1 while
// untyped (no purge) and leave it present — a state the server is never in.
// The pinned prefix must never produce it.
func TestCandidateBases_PinsPendingOrder(t *testing.T) {
	t.Parallel()

	c := NewChecker([]string{"L"}, nil)
	p1 := bulkOf(oracletest.AddTypeReqP("a", commonpb.AccountTypePersistence_ACCOUNT_TYPE_EPHEMERAL))
	p2 := bulkOf(oracletest.TxReq("world", "a:1", "USD", 5), oracletest.TxReq("a:1", "world", "USD", 5))
	c.pending = []*pendingObservation{
		{minSeq: 1, obs: observation{bulk: p1}},
		{minSeq: 3, obs: observation{bulk: p2}},
	}

	for _, b := range collectBases(c) {
		_, present := b.Ledger("L").Volumes().Get(oracle.VolumeKey{Address: "a:1", Asset: "USD"})
		require.False(t, present, "a:1 must be absent in every candidate base (reordered prefix is illegal)")
	}
}

// A committed archive buffered in the re-order queue must not stall the pending
// prefix. The prefix advances only through Apply succeeding, and the model has not
// been told the Sealer sealed chapter 1 — so without folding that transition, the
// archive is refused, every later pending bulk becomes unreachable in the search,
// and a correct server reads as a business-state divergence.
func TestCandidateBases_BufferedArchiveDoesNotStallPending(t *testing.T) {
	t.Parallel()

	c := NewChecker([]string{"L"}, nil)
	c.modelState = c.modelState.WithChapters(registry(t, oracle.ChapterClosing, oracle.ChapterOpen))
	c.pending = []*pendingObservation{
		{minSeq: 1, obs: observation{bulk: bulkOf(actions.ArchiveChapterAction(1))}},
		{minSeq: 2, obs: observation{bulk: bulkOf(oracletest.AddTypeReq("T"))}},
	}

	reached := false
	for _, b := range collectBases(c) {
		if b.Ledger("L").Types().Has("T") {
			reached = true
		}
	}
	require.True(t, reached, "the bulk behind the buffered archive must be reachable")
}

// The observation being validated is out of the in-flight set by the time a
// failure validates, so its own chapter has to be named by the caller — otherwise
// the search never offers the state where that chapter is sealed, and a rejection
// whose reason turns on the seal cannot be explained.
func TestCandidateBases_AddressedChapterIsSealable(t *testing.T) {
	t.Parallel()

	c := NewChecker([]string{"L"}, nil)
	c.modelState = c.modelState.WithChapters(registry(t, oracle.ChapterClosing, oracle.ChapterOpen))

	sealedSomewhere := func(addressed []uint64) bool {
		found := false
		c.candidateBases(^uint64(0), addressed, func(b oracle.GlobalState) bool {
			if status, ok := b.Chapters().StatusOf(1); ok && status == oracle.ChapterClosed {
				found = true
			}

			return false
		})

		return found
	}

	require.False(t, sealedSomewhere(nil), "a chapter no order names is not worth branching on")
	require.True(t, sealedSomewhere([]uint64{1}))
}

// The general search makes self-explanation structurally impossible: a
// lone AddAccountType(T) returning AlreadyExists is "explained" only when a
// concurrent in-flight bulk actually adds T. With no such bulk, the model
// predicts success on the only candidate base, so the failure is a finding.
func TestModelFailure_NoSelfExplanation(t *testing.T) {
	t.Parallel()

	alreadyExistsExplained := func(bases []oracle.GlobalState) bool {
		for _, b := range bases {
			res := b.Apply(bulkOf(oracletest.AddTypeReq("T")))
			if !res.OK && res.Reason == domain.ErrReasonAccountTypeAlreadyExists {
				return true
			}
		}

		return false
	}

	// No concurrent add -> AlreadyExists is not explainable (would be a finding).
	require.False(t, alreadyExistsExplained(collectBases(NewChecker([]string{"L"}, nil))))

	// Concurrent in-flight add of T -> AlreadyExists is explainable.
	withAdd := NewChecker([]string{"L"}, nil)
	withAdd.inflight[1] = bulkOf(oracletest.AddTypeReq("T"))
	require.True(t, alreadyExistsExplained(collectBases(withAdd)))
}
