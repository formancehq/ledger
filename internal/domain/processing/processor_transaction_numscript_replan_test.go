package processing

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/domain/processing/numscript"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// replanScript reads @wallet's balance while binding its variable, then
// reaches a scaling source, which planning rejects whatever the balance.
const replanScript = `
	vars { monetary $amt = balance(@wallet, USD/2) }
	send $amt (
		source = @wallet with scaling through @pool
		destination = @out
	)
`

var replanWalletKey = domain.NewVolumeKey("test", "wallet", "USD/2", "")

func replanWalletVolume(input uint64) raftcmdpb.VolumePairReader {
	return (&raftcmdpb.VolumePair{
		Input:  commonpb.NewUint256FromUint64(input),
		Output: commonpb.NewUint256FromUint64(0),
	}).AsReader()
}

// replanAdmissionHash is the inputs hash admission binds for replanScript when
// it reads the given @wallet balance.
func replanAdmissionHash(t *testing.T, input uint64) []byte {
	t.Helper()

	mockStore := NewMockScope(gomock.NewController(t))
	mockStore.EXPECT().IsCovered(dal.SubAttrVolume, replanWalletKey).Return(true).AnyTimes()
	setupVolumesStub(mockStore).expectGet(replanWalletKey, replanWalletVolume(input), nil)

	observed, err := numscript.DiscoverNumscriptDependencies(
		numscript.NewNumscriptCache(16), replanScript, nil, "test",
		&declaredValueSource{scopeValueSource{store: mockStore, ledgerName: "test"}}, false,
	)
	require.ErrorIs(t, err, domain.ErrNumscriptScalingUnsupported)
	require.NotEmpty(t, observed.InputsHash)

	return observed.InputsHash
}

func replan(t *testing.T, mockStore *MockScope, script string, vars map[string]string, expectedHash []byte) domain.SerializableError {
	t.Helper()

	return replanFailedScript(mockStore, numscript.NewNumscriptCache(16), "test",
		&raftcmdpb.CreateTransactionOrder{}, &commonpb.Script{Plain: script, Vars: vars}, expectedHash)
}

// TestReplan_SameInputsReproduceAdmissionFailure pins the definitive outcome:
// over the values admission read, planning fails the same way.
func TestReplan_SameInputsReproduceAdmissionFailure(t *testing.T) {
	t.Parallel()

	expected := replanAdmissionHash(t, 1000)

	mockStore := NewMockScope(gomock.NewController(t))
	mockStore.EXPECT().IsCovered(dal.SubAttrVolume, replanWalletKey).Return(true)
	setupVolumesStub(mockStore).expectGet(replanWalletKey, replanWalletVolume(1000), nil)

	err := replan(t, mockStore, replanScript, nil, expected)
	require.ErrorIs(t, err, domain.ErrNumscriptScalingUnsupported)
}

// TestReplan_FailureBeforeAnyReadNeedsNoInput covers a failure of the script
// and vars alone: nothing is read, so there is no hash to compare.
func TestReplan_FailureBeforeAnyReadNeedsNoInput(t *testing.T) {
	t.Parallel()

	mockStore := NewMockScope(gomock.NewController(t))

	err := replan(t, mockStore, "vars { monetary $m }\nsend $m (source = @world destination = @out)", map[string]string{"m": "@world"}, nil)
	require.NotNil(t, err)
	require.NotErrorIs(t, err, domain.ErrStaleInputsResolution)
	require.Equal(t, domain.ErrReasonNumscriptExecutionError, err.Reason())
}

// TestReplan_ChangedInputIsStale covers a value that moved since admission:
// the same failure is reached, but not over the inputs admission bound.
func TestReplan_ChangedInputIsStale(t *testing.T) {
	t.Parallel()

	expected := replanAdmissionHash(t, 1000)

	mockStore := NewMockScope(gomock.NewController(t))
	mockStore.EXPECT().IsCovered(dal.SubAttrVolume, replanWalletKey).Return(true)
	setupVolumesStub(mockStore).expectGet(replanWalletKey, replanWalletVolume(2000), nil)

	require.ErrorIs(t, replan(t, mockStore, replanScript, nil, expected), domain.ErrStaleInputsResolution)
}

// TestReplan_UndeclaredReadIsStale covers a read outside the order's coverage.
// It is probed, never fetched, so it records no coverage miss.
func TestReplan_UndeclaredReadIsStale(t *testing.T) {
	t.Parallel()

	mockStore := NewMockScope(gomock.NewController(t))
	mockStore.EXPECT().IsCovered(dal.SubAttrVolume, replanWalletKey).Return(false)

	require.ErrorIs(t, replan(t, mockStore, replanScript, nil, []byte("bound-at-admission")), domain.ErrStaleInputsResolution)
}

// TestReplan_ScriptThatNowPlansIsStale covers a script whose planning succeeds
// at apply: admission's failure no longer holds and nothing was planned to run.
func TestReplan_ScriptThatNowPlansIsStale(t *testing.T) {
	t.Parallel()

	mockStore := NewMockScope(gomock.NewController(t))

	err := replan(t, mockStore, `send [USD/2 100] (source = @world destination = @out)`, nil, nil)
	require.ErrorIs(t, err, domain.ErrStaleInputsResolution)
}

// TestReplan_MarkedOrderWithoutScriptIsInvalid pins the impossible state: only
// scripted orders are marked.
func TestReplan_MarkedOrderWithoutScriptIsInvalid(t *testing.T) {
	t.Parallel()

	err := replanFailedScript(NewMockScope(gomock.NewController(t)), numscript.NewNumscriptCache(16), "test",
		&raftcmdpb.CreateTransactionOrder{}, nil, nil)

	var invalid *domain.ErrInvalidExecutionPlan
	require.ErrorAs(t, err, &invalid)
}
