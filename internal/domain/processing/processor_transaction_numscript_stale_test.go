package processing

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/domain/processing/numscript"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// coverageMissDescribable is a domain.Describable carrying the COVERAGE_MISS
// reason — the same reason a real *state.ErrCoverageMiss carries. The processing
// package cannot import *state.ErrCoverageMiss directly (import cycle: state
// imports processing), so this stand-in reproduces the reason the apply path
// keys on. TestCoverageMissSurvivesNumscriptLibrary in internal/infra/state
// proves the concrete *state.ErrCoverageMiss round-trips with this same reason.
type coverageMissDescribable struct{}

func (coverageMissDescribable) Error() string               { return "preload coverage miss (test)" }
func (coverageMissDescribable) Reason() string              { return domain.ErrReasonCoverageMiss }
func (coverageMissDescribable) Metadata() map[string]string { return nil }

// staleScript reads a balance DURING dependency resolution via balance() in a
// var origin, so the apply-path re-resolution consults the Scope for @wallet's
// USD/2 balance. (A plain bounded source only records the dependency; its
// balance is not read at resolution time, so it would not exercise the read.)
const staleScript = `
	vars { monetary $amt = balance(@wallet, USD/2) }
	send $amt (source = @wallet destination = @out)
`

func newNumscriptProducer() *numscriptPostingProducer {
	return &numscriptPostingProducer{
		cache:      numscript.NewNumscriptCache(16),
		ledgerName: "test",
		assetCache: map[string]cachedAssetPrecision{},
		// A non-empty stored hash arms the stale-inputs re-resolution block.
		// It now lives on the producer (staged from OrderTechnical by the
		// dispatcher), not on the CreateTransactionOrder.
		inputsResolutionHash: []byte("stored-hash-from-admission"),
	}
}

func staleOrder() *raftcmdpb.CreateTransactionOrder {
	return &raftcmdpb.CreateTransactionOrder{}
}

// TestProduce_ChangedValueWithCompletedResolutionIsStale is regression #1: a
// COMPLETED apply-time re-resolution whose recorded balance differs from the one
// admission bound (hash mismatch) must reject with the retryable
// ErrStaleInputsResolution — the client re-admits against fresh state.
func TestProduce_ChangedValueWithCompletedResolutionIsStale(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	volumes := setupVolumesStub(mockStore)

	// @wallet has a real balance, so resolution completes; the recomputed hash
	// will not equal the arbitrary stored hash, so the block rejects as stale.
	walletVol := &raftcmdpb.VolumePair{
		Input:  commonpb.NewUint256FromUint64(1000),
		Output: commonpb.NewUint256FromUint64(0),
	}
	volumes.expectGet(domain.NewVolumeKey("test", "wallet", "USD/2"), walletVol.AsReader(), nil)

	producer := newNumscriptProducer()
	order := staleOrder()

	_, err := producer.produce(mockStore, "test", order, &commonpb.Script{Plain: staleScript})

	require.NotNil(t, err)
	require.ErrorIs(t, err, domain.ErrStaleInputsResolution,
		"a completed resolution with a changed recorded value must reject as stale")
}

// TestProduce_CoverageMissDuringResolutionIsLoudNotStale is regression #2: when
// apply-time re-resolution derives a key admission never declared, the gated
// Scope returns a coverage-contract error. It MUST surface loudly (the coverage
// reason), NOT be masked as retryable stale — masking it would spin the client
// in an infinite re-admit loop against the same missing declaration.
func TestProduce_CoverageMissDuringResolutionIsLoudNotStale(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	volumes := setupVolumesStub(mockStore)

	// The @wallet balance read fails with a coverage-contract violation.
	volumes.onGet(func(k domain.VolumeKey) (raftcmdpb.VolumePairReader, error) {
		return nil, coverageMissDescribable{}
	})

	producer := newNumscriptProducer()
	order := staleOrder()

	_, err := producer.produce(mockStore, "test", order, &commonpb.Script{Plain: staleScript})

	require.NotNil(t, err)
	require.NotErrorIs(t, err, domain.ErrStaleInputsResolution,
		"a coverage-contract violation must NOT be masked as stale (infinite re-admit loop)")

	var describable domain.Describable
	require.ErrorAs(t, err, &describable)
	require.Equal(t, domain.ErrReasonCoverageMiss, describable.Reason(),
		"the coverage-contract violation must surface loudly with its own reason")
}

// TestProduce_InvalidExecutionPlanDuringResolutionIsLoudNotStale mirrors
// regression #2 for the other coverage-contract error, *domain.ErrInvalidExecutionPlan.
func TestProduce_InvalidExecutionPlanDuringResolutionIsLoudNotStale(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	volumes := setupVolumesStub(mockStore)

	sentinel := &domain.ErrInvalidExecutionPlan{Reason_: "undeclared key"}
	volumes.onGet(func(k domain.VolumeKey) (raftcmdpb.VolumePairReader, error) {
		return nil, sentinel
	})

	producer := newNumscriptProducer()
	order := staleOrder()

	_, err := producer.produce(mockStore, "test", order, &commonpb.Script{Plain: staleScript})

	require.NotNil(t, err)
	require.NotErrorIs(t, err, domain.ErrStaleInputsResolution)

	var invalid *domain.ErrInvalidExecutionPlan
	require.ErrorAs(t, err, &invalid, "the invalid-execution-plan violation must surface loudly")
}

// TestIsCoverageContractViolation is a focused unit test for the discriminator
// the apply path uses, including the wrapped-in-chain case.
func TestIsCoverageContractViolation(t *testing.T) {
	t.Parallel()

	require.True(t, isCoverageContractViolation(coverageMissDescribable{}))
	require.True(t, isCoverageContractViolation(&domain.ErrInvalidExecutionPlan{Reason_: "x"}))
	// A coverage violation nested behind an fmt-wrapped error is still found via
	// the Unwrap chain.
	require.True(t, isCoverageContractViolation(
		fmt.Errorf("wrapped: %w", &domain.ErrInvalidExecutionPlan{Reason_: "x"})))

	require.False(t, isCoverageContractViolation(nil))
	require.False(t, isCoverageContractViolation(errors.New("some resolution error")))
	require.False(t, isCoverageContractViolation(domain.ErrStaleInputsResolution),
		"a genuine stale-inputs error is not a coverage violation")
}
