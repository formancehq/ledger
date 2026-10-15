package main

import (
	"testing"

	"google.golang.org/grpc/codes"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/tests/oracle"
	"github.com/formancehq/ledger/v3/tests/oracle/oracletest"
)

func bulkOf(reqs ...*servicepb.Request) oracle.Bulk { return oracle.Bulk{Requests: reqs} }

func collectBases(c *Checker) []oracle.GlobalState {
	var out []oracle.GlobalState
	// ^uint64(0): no high-water bound — fold every in-flight/pending bulk.
	c.candidateBases(^uint64(0), func(b oracle.GlobalState) bool {
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
		c.candidateBases(maxTicket, func(b oracle.GlobalState) bool {
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
	c.candidateBases(4, func(b oracle.GlobalState) bool {
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

func TestCandidateBasesRejectsIntractableInflightSet(t *testing.T) {
	t.Parallel()

	c := NewChecker([]string{"L"}, nil)
	for ticket := uint64(1); ticket <= maxCandidateInflight+1; ticket++ {
		// Every bulk is independently committable. Without the explicit bound,
		// exhausting this search requires exploring every subset.
		c.inflight[ticket] = bulkOf(oracletest.AddTypeReq(string(rune('A' + ticket))))
	}

	require.PanicsWithValue(t, "candidate search exceeded its bounded in-flight set", func() {
		c.candidateBases(maxCandidateInflight+1, func(oracle.GlobalState) bool { return false })
	})
}

func TestCandidateBasesCompletesAtInflightBound(t *testing.T) {
	t.Parallel()

	c := NewChecker([]string{"L"}, nil)
	for ticket := uint64(1); ticket <= maxCandidateInflight; ticket++ {
		c.inflight[ticket] = bulkOf(oracletest.AddTypeReq(string(rune('A' + ticket))))
	}

	visited := 0
	c.candidateBases(maxCandidateInflight, func(oracle.GlobalState) bool {
		visited++

		return false
	})
	// Distinct insertion orders produce distinct fingerprints, so this is the
	// full sum of partial permutations: sum(8!/(8-k)!, k=0..8).
	require.Equal(t, 109601, visited)
}

// A delete committed between a scripted create's admission and its apply.
// Admission saw the live ledger — the reference existed, so the skip applied
// and nothing was resolved — and the FSM refused the order on the deleted one.
// No single base gives that pair: on the deleted base the purged ledger makes
// admission forward the script instead, so the verdict must come from the
// base before the delete.
func TestValidateFailure_AdmissionOlderThanApply(t *testing.T) {
	t.Parallel()

	c := NewChecker([]string{"L"}, nil)
	committed := c.modelState.Apply(bulkOf(oracletest.TxReqRefL("L", "R", "world", "a", "USD", 10)))
	require.True(t, committed.OK, committed.Reason)
	c.modelState = committed.State

	c.inflight[1] = bulkOf(&servicepb.Request{Type: &servicepb.Request_DeleteLedger{
		DeleteLedger: &servicepb.DeleteLedgerRequest{Name: "L"},
	}})

	scripted := &servicepb.Request{Type: &servicepb.Request_Apply{Apply: &servicepb.LedgerApplyRequest{
		Ledger:           "L",
		SkippableReasons: []commonpb.ErrorReason{commonpb.ErrorReason_ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT},
		Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_CreateTransaction{CreateTransaction: &servicepb.CreateTransactionPayload{
			Reference: "R",
			Script: &commonpb.Script{Plain: `vars {
	account $dst = meta(@cfg:1, "dst")
}
send [USD 1] (
	source = @world
	destination = $dst
)`},
		}}},
	}}}
	failed := bulkOf(scripted)

	deletedBase := c.modelState.Apply(c.inflight[1]).State
	require.Equal(t, domain.ErrReasonPreloadUnavailable, deletedBase.Apply(failed).Reason)

	c.validateFailure(1, failed, reasonErr(t, codes.FailedPrecondition, domain.ErrReasonLedgerDeleted))
	require.Contains(t, c.rejections, rejectedBulk{ledgers: "L", orders: 1, reason: domain.ErrReasonLedgerDeleted})
}
