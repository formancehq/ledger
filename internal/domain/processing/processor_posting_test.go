package processing

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

func TestApplyPosting_WorldAccount_SkipsBalanceCheck(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)

	sourceKey := domain.NewVolumeKey("test", "world", "USD", "")
	destKey := domain.NewVolumeKey("test", "users:001", "USD", "")

	zeroVol := &raftcmdpb.VolumePair{
		Input:  commonpb.NewUint256FromUint64(0),
		Output: commonpb.NewUint256FromUint64(0),
	}

	expectGetVolume(mockStore, sourceKey, zeroVol.AsReader(), nil)
	expectPutVolume(t, mockStore, sourceKey, nil)

	expectGetVolume(mockStore, destKey, zeroVol.AsReader(), nil)
	expectPutVolume(t, mockStore, destKey, nil)

	posting := &commonpb.Posting{
		Source:      "world",
		Destination: "users:001",
		Amount:      commonpb.NewUint256FromUint64(500),
		Asset:       "USD",
	}

	err := applyPosting(mockStore, "test", posting, false, nil)
	require.NoError(t, err)
}

func TestApplyPosting_InsufficientFunds(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)

	sourceKey := domain.NewVolumeKey("test", "bank", "USD", "")

	// Source has input=100, output=50, balance=50, but posting is 200
	sourceVol := &raftcmdpb.VolumePair{
		Input:  commonpb.NewUint256FromUint64(100),
		Output: commonpb.NewUint256FromUint64(50),
	}

	expectGetVolume(mockStore, sourceKey, sourceVol.AsReader(), nil)

	posting := &commonpb.Posting{
		Source:      "bank",
		Destination: "users:001",
		Amount:      commonpb.NewUint256FromUint64(200),
		Asset:       "USD",
	}

	err := applyPosting(mockStore, "test", posting, false, nil)
	require.Error(t, err)

	var insufficientFunds *domain.ErrInsufficientFunds
	require.ErrorAs(t, err, &insufficientFunds)
	require.Equal(t, "bank", insufficientFunds.Account)
	require.Equal(t, "USD", insufficientFunds.Asset)
}

func TestApplyPosting_ZeroInputBalance(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)

	sourceKey := domain.NewVolumeKey("test", "bank", "USD", "")

	// Source has zero input balance, Output=0
	sourceVol := &raftcmdpb.VolumePair{
		Input:  commonpb.NewUint256FromUint64(0),
		Output: commonpb.NewUint256FromUint64(0),
	}

	expectGetVolume(mockStore, sourceKey, sourceVol.AsReader(), nil)

	posting := &commonpb.Posting{
		Source:      "bank",
		Destination: "users:001",
		Amount:      commonpb.NewUint256FromUint64(100),
		Asset:       "USD",
	}

	// Zero input means posting amount > 0 triggers ErrInsufficientFunds
	err := applyPosting(mockStore, "test", posting, false, nil)
	require.Error(t, err)

	var insufficientFunds *domain.ErrInsufficientFunds
	require.ErrorAs(t, err, &insufficientFunds)
	require.Equal(t, "bank", insufficientFunds.Account)
	require.Equal(t, "USD", insufficientFunds.Asset)
}

func TestApplyPosting_ForceSkipsBalanceCheck(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)

	sourceKey := domain.NewVolumeKey("test", "bank", "USD", "")
	destKey := domain.NewVolumeKey("test", "users:001", "USD", "")

	// Source has insufficient balance, but force=true skips the check
	sourceVol := &raftcmdpb.VolumePair{
		Input:  commonpb.NewUint256FromUint64(10),
		Output: commonpb.NewUint256FromUint64(0),
	}
	destVol := &raftcmdpb.VolumePair{
		Input:  commonpb.NewUint256FromUint64(0),
		Output: commonpb.NewUint256FromUint64(0),
	}

	expectGetVolume(mockStore, sourceKey, sourceVol.AsReader(), nil)
	expectPutVolume(t, mockStore, sourceKey, nil)
	expectGetVolume(mockStore, destKey, destVol.AsReader(), nil)
	expectPutVolume(t, mockStore, destKey, nil)

	posting := &commonpb.Posting{
		Source:      "bank",
		Destination: "users:001",
		Amount:      commonpb.NewUint256FromUint64(500),
		Asset:       "USD",
	}

	err := applyPosting(mockStore, "test", posting, true, nil)
	require.NoError(t, err)
}

func TestApplyPosting_NotPreloaded(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)

	sourceKey := domain.NewVolumeKey("test", "bank", "USD", "")

	expectGetVolume(mockStore, sourceKey, nil, nil) //nolint:nilnil // test: nil volume

	posting := &commonpb.Posting{
		Source:      "bank",
		Destination: "users:001",
		Amount:      commonpb.NewUint256FromUint64(100),
		Asset:       "USD",
	}

	err := applyPosting(mockStore, "test", posting, false, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "balance not preloaded")
}

// TestApplyPosting_AbsentVolumes_TreatedAsZero pins the EN-1378 contract:
// a declared-but-absent volume key (Scope.GetVolume → domain.ErrNotFound)
// is treated as a zero balance, not as an admission failure. The
// coverage gate is what catches "admission forgot to declare"; ErrNotFound
// is the legitimate "fresh (account, asset)" signal once admission has
// stopped injecting zero-VolumePair AttributeValue plans.
func TestApplyPosting_AbsentVolumes_TreatedAsZero(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	sourceKey := domain.NewVolumeKey("test", "world", "USD")
	destKey := domain.NewVolumeKey("test", "users:001", "USD")

	// Both source (world) and destination are absent in the cache. Apply
	// must still succeed: world skips the balance check, dest receives the
	// amount onto a synthesised zero. expectPutVolume lazily wires the
	// volumes stub; an unregistered Get falls through to the stub's
	// default ErrNotFound, which is exactly the "absent" state
	// readVolumeOrZero must synthesise a zero balance for.
	expectPutVolume(t, mockStore, sourceKey, nil)
	expectPutVolume(t, mockStore, destKey, nil)

	posting := &commonpb.Posting{
		Source:      "world",
		Destination: "users:001",
		Amount:      commonpb.NewUint256FromUint64(500),
		Asset:       "USD",
	}

	require.NoError(t, applyPosting(mockStore, "test", posting, false, nil))
}

// TestApplyPosting_AbsentNonWorldSource_InsufficientFunds confirms the
// synthesised zero balance still feeds the regular balance check: a
// non-world source with no preloaded volume cannot cover a positive
// posting and must surface ErrInsufficientFunds (not pass through with a
// silently-zero balance).
func TestApplyPosting_AbsentNonWorldSource_InsufficientFunds(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	sourceKey := domain.NewVolumeKey("test", "bank", "USD")

	// kindStub's default for an unregistered Get is ErrNotFound — exactly
	// the "absent in cache" state readVolumeOrZero must treat as a zero
	// balance. We still need to register an explicit Get on this key so
	// the volumes stub is wired (no Put expectation either: the apply
	// path must fail before reaching the destination side).
	expectGetVolume(mockStore, sourceKey, nil, domain.ErrNotFound)

	posting := &commonpb.Posting{
		Source:      "bank",
		Destination: "users:001",
		Amount:      commonpb.NewUint256FromUint64(100),
		Asset:       "USD",
	}

	err := applyPosting(mockStore, "test", posting, false, nil)
	require.Error(t, err)

	var insufficientFunds *domain.ErrInsufficientFunds
	require.ErrorAs(t, err, &insufficientFunds)
	require.Equal(t, "bank", insufficientFunds.Account)
	require.Equal(t, "USD", insufficientFunds.Asset)
}

// uint256Max returns the maximum uint256 value (2^256 - 1).
func uint256Max() *uint256.Int {
	var m uint256.Int
	m.SubUint64(&m, 1) // 0 - 1 wraps to 2^256-1

	return &m
}

// TestApplyPosting_DestinationInputOverflow_Rejects pins the fix for
// #321. Before this PR the destination Input was incremented with plain
// uint256.Add, which wraps silently on overflow: two API calls
// `world → A` of (2^256-1) then 1 would wrap A.Input back to 0 while
// Output stayed unchanged — money silently created. The fix uses
// AddOverflow and rejects the order; the FSM apply path discards the
// WriteSet atomically on error.
func TestApplyPosting_DestinationInputOverflow_Rejects(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)

	sourceKey := domain.NewVolumeKey("test", "world", "USD", "")
	destKey := domain.NewVolumeKey("test", "users:001", "USD", "")

	// world output is 0 — safe to add anything on the source side.
	worldVol := &raftcmdpb.VolumePair{
		Input:  commonpb.NewUint256FromUint64(0),
		Output: commonpb.NewUint256FromUint64(0),
	}
	// Destination Input already at 2^256-1, so any positive amount
	// overflows on Input.
	destVol := &raftcmdpb.VolumePair{
		Input:  commonpb.NewUint256(uint256Max()),
		Output: commonpb.NewUint256FromUint64(0),
	}

	volumes := setupVolumesStub(mockStore)
	volumes.expectGet(sourceKey, worldVol.AsReader(), nil)
	volumes.expectGet(destKey, destVol.AsReader(), nil)
	// destination PutVolume must NOT be called once the overflow is
	// detected — left unchecked here; the assertion below is on the
	// returned error type.

	posting := &commonpb.Posting{
		Source:      "world",
		Destination: "users:001",
		Amount:      commonpb.NewUint256FromUint64(1),
		Asset:       "USD",
	}

	err := applyPosting(mockStore, "test", posting, false, nil)
	require.Error(t, err)

	var overflowErr *domain.ErrVolumeOverflow
	require.ErrorAs(t, err, &overflowErr,
		"posting that overflows destination Input must surface ErrVolumeOverflow (#321)")
	require.Equal(t, "users:001", overflowErr.Account)
	require.Equal(t, "USD", overflowErr.Asset)
	require.Equal(t, "input", overflowErr.Side)
}

// TestApplyPosting_SourceOutputOverflow_Rejects covers the
// world-source path: the balance check is skipped for `world` so the
// source Output mutation is the one that can wrap. We seed world's
// Output at 2^256-1 and try to add 1.
func TestApplyPosting_SourceOutputOverflow_Rejects(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)

	sourceKey := domain.NewVolumeKey("test", "world", "USD", "")

	worldVol := &raftcmdpb.VolumePair{
		Input:  commonpb.NewUint256FromUint64(0),
		Output: commonpb.NewUint256(uint256Max()),
	}

	expectGetVolume(mockStore, sourceKey, worldVol.AsReader(), nil)
	// source PutVolume must NOT be called once the overflow is detected.

	posting := &commonpb.Posting{
		Source:      "world",
		Destination: "users:001",
		Amount:      commonpb.NewUint256FromUint64(1),
		Asset:       "USD",
	}

	err := applyPosting(mockStore, "test", posting, false, nil)
	require.Error(t, err)

	var overflowErr *domain.ErrVolumeOverflow
	require.ErrorAs(t, err, &overflowErr)
	require.Equal(t, "world", overflowErr.Account)
	require.Equal(t, "output", overflowErr.Side)
}
