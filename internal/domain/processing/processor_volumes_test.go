package processing

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// TestBuildPostCommitVolumes_PropagatesReadError pins EN-1440: a
// non-ErrNotFound volume-read error (on the gated FSM scope, that is
// *state.ErrCoverageMiss — an admission-contract violation (invariants #6/#9)
// that is impossible by design) must reject the order, not be swallowed into a
// truncated volume map. The processing package cannot import
// state.ErrCoverageMiss (import cycle: state imports processing), so a generic
// sentinel stands in for any non-ErrNotFound error; the fix propagates all of
// them identically.
func TestBuildPostCommitVolumes_PropagatesReadError(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	volumes := setupVolumesStub(mockStore)

	sentinel := errors.New("simulated coverage miss")
	// First pair read is the posting source ("world"); program it to fail.
	volumes.expectGet(domain.NewVolumeKey("test", "world", "USD"), nil, sentinel)

	postings := []*commonpb.Posting{{
		Source:      "world",
		Destination: "users:001",
		Amount:      commonpb.NewUint256FromUint64(500),
		Asset:       "USD",
	}}

	result, err := buildPostCommitVolumes(mockStore, "test", postings)
	require.Nil(t, result, "no volume map must be returned when a read fails")
	require.Error(t, err)

	var storageErr *domain.ErrStorageOperation
	require.ErrorAs(t, err, &storageErr, "read error must surface as ErrStorageOperation")
	require.ErrorIs(t, err, sentinel, "the underlying cause must be preserved for errors.As/Is")
}

// TestBuildPostCommitVolumes_FoundAndAbsent covers the happy path: a present
// volume is reported with its real Input/Output, and a declared-but-absent key
// (ErrNotFound) is reported as "0"/"0" — byte-identical to the pre-fix output.
func TestBuildPostCommitVolumes_FoundAndAbsent(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	volumes := setupVolumesStub(mockStore)

	// Source present with real volumes; destination left unregistered so the
	// stub returns ErrNotFound -> synthesised zero balance.
	sourceVol := &raftcmdpb.VolumePair{
		Input:  commonpb.NewUint256FromUint64(100),
		Output: commonpb.NewUint256FromUint64(40),
	}
	volumes.expectGet(domain.NewVolumeKey("test", "bank", "USD"), sourceVol.AsReader(), nil)

	postings := []*commonpb.Posting{{
		Source:      "bank",
		Destination: "users:001",
		Amount:      commonpb.NewUint256FromUint64(60),
		Asset:       "USD",
	}}

	result, err := buildPostCommitVolumes(mockStore, "test", postings)
	require.Nil(t, err)
	require.NotNil(t, result)

	bank := result.GetVolumesByAccount()["bank"].GetVolumes()["USD"]
	require.Equal(t, "100", bank.GetInput())
	require.Equal(t, "40", bank.GetOutput())

	users := result.GetVolumesByAccount()["users:001"].GetVolumes()["USD"]
	require.Equal(t, "0", users.GetInput())
	require.Equal(t, "0", users.GetOutput())
}
