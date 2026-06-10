package state

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// TestCheckStaleProposal_CacheEpochMismatchAfterFirstReset reproduces the
// scenario from #302: a proposal admitted before applyClusterConfig wiped
// the cache is applied AFTER the wipe. Before the fix, cache.epoch
// started at 0, so the staleness check (`preloadEpoch != 0 && …`) was
// short-circuited and the proposal slipped through against an empty
// cache. Initializing the epoch to 1 in cache.New means every preload
// carries epoch >= 1, the `!= 0` guard never opts out, and the actual
// `preloadEpoch != currentEpoch` comparison catches the drift.
func TestCheckStaleProposal_CacheEpochMismatchAfterFirstReset(t *testing.T) {
	t.Parallel()

	fsm, _, _ := newTestMachine(t)

	// Fresh cache: epoch is 1 (not 0) — the regression contract.
	require.Equal(t, uint64(1), fsm.Registry.Cache.Epoch(),
		"fresh cache must expose epoch=1 so the FSM staleness check is never inert")

	// A proposal admitted under the fresh epoch.
	proposal := &raftcmdpb.Proposal{
		Id: 1,
		Preload: &raftcmdpb.PreloadSet{
			CacheEpoch: fsm.Registry.Cache.Epoch(),
		},
	}

	// While the cache is still at the admission-time epoch, the proposal
	// passes the staleness check.
	require.NoError(t, fsm.checkStaleProposal(0, proposal),
		"proposal carrying the live cache epoch must pass")

	// A cluster config change wipes the cache and bumps the epoch.
	fsm.Registry.Cache.ResetWithThreshold(2000)
	require.Equal(t, uint64(2), fsm.Registry.Cache.Epoch())

	// The in-flight proposal (still carrying epoch=1) must now be rejected
	// as stale — this is exactly the protection #302 reported as missing.
	err := fsm.checkStaleProposal(0, proposal)
	require.ErrorIs(t, err, domain.ErrStaleProposal,
		"proposal built before the cache reset must be rejected after the reset (#302)")
}

// TestCheckStaleProposal_PreloadWithoutEpochIsAccepted documents the
// transitional contract: a proposal carrying CacheEpoch=0 (e.g. one
// committed but not yet applied at the moment a node is upgraded
// to this fix) is still accepted. The sentinel `!= 0 &&` short-circuit
// is kept on purpose so the upgrade does not invalidate in-flight Raft
// entries. New admissions never produce epoch=0 since cache.New now
// initializes to 1.
func TestCheckStaleProposal_PreloadWithoutEpochIsAccepted(t *testing.T) {
	t.Parallel()

	fsm, _, _ := newTestMachine(t)
	fsm.Registry.Cache.ResetWithThreshold(2000) // live epoch = 2

	proposal := &raftcmdpb.Proposal{
		Id: 1,
		Preload: &raftcmdpb.PreloadSet{
			CacheEpoch: 0, // pre-upgrade entry
		},
	}

	require.NoError(t, fsm.checkStaleProposal(0, proposal),
		"proposal lacking CacheEpoch (pre-upgrade) must not be rejected by epoch check")
}
