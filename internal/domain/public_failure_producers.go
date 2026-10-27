package domain

import (
	"errors"
	"strconv"
	"strings"
)

// PublicFailureFacts belongs to the API presentation only. These failures
// cannot enter an audited order outcome, so they do not implement
// SerializableError and have no FailureDetails variant.
func (errMaintenanceMode) PublicFailureFacts() FailureFacts {
	return NewFailureFacts(ErrReasonMaintenanceMode, ErrReasonMaintenanceMode, nil)
}

func (*ErrIdempotencyCheckFailed) PublicFailureFacts() FailureFacts {
	return NewFailureFacts(ErrReasonIdempotencyCheckFailed, ErrReasonIdempotencyCheckFailed, nil)
}

func (*ErrClusterPolicyInvalid) PublicFailureFacts() FailureFacts {
	return NewFailureFacts(ErrReasonClusterPolicyInvalid, ErrReasonClusterPolicyInvalid, nil)
}

func (e *ErrClusterPolicyRevisionConflict) PublicFailureFacts() FailureFacts {
	return NewFailureFacts(ErrReasonClusterPolicyRevisionConflict, ErrReasonClusterPolicyRevisionConflict, map[string]string{
		"revision": strconv.FormatUint(e.Revision, 10),
	})
}

func (e *ErrExecutionPlanTooLarge) PublicFailureFacts() FailureFacts {
	return NewFailureFacts(ErrReasonExecutionPlanTooLarge, ErrReasonExecutionPlanTooLarge, map[string]string{
		"size":  strconv.FormatUint(uint64(e.Size), 10),
		"limit": strconv.FormatUint(uint64(e.Limit), 10),
	})
}

// Technical errors can retain useful public context without acquiring a
// FailureDetails variant or becoming an audited business rejection.
func (e *ErrBalanceNotPreloaded) PublicFailureFacts() FailureFacts {
	return NewFailureFacts(e.Reason(), e.Reason(), map[string]string{
		"account": e.Account, "asset": e.Asset, "color": e.Color,
	})
}

func (e *ErrCheckpointNotReady) PublicFailureFacts() FailureFacts {
	return NewFailureFacts(e.Reason(), e.Reason(), map[string]string{
		"checkpointId": strconv.FormatUint(e.CheckpointID, 10),
	})
}

func (e *ErrIndexBuilding) PublicFailureFacts() FailureFacts {
	return NewFailureFacts(e.Reason(), e.Reason(), map[string]string{"index": e.Index})
}

func (e *ErrSequenceExhausted) PublicFailureFacts() FailureFacts {
	return NewFailureFacts(e.Reason(), e.Reason(), map[string]string{
		"counter": string(e.Counter),
	})
}

func (e *ErrMirrorV2LogIDGap) PublicFailureFacts() FailureFacts {
	return NewFailureFacts(e.Reason(), e.Reason(), map[string]string{
		"name": e.Name, "got": strconv.FormatUint(e.Got, 10), "expected": strconv.FormatUint(e.Expected, 10),
	})
}

func (e *ErrMirrorV2LogIDInvalid) PublicFailureFacts() FailureFacts {
	return NewFailureFacts(e.Reason(), e.Reason(), map[string]string{"name": e.Name})
}

func (e *ErrStaleClusterPolicy) PublicFailureFacts() FailureFacts {
	return NewFailureFacts(e.Reason(), e.Reason(), map[string]string{
		"proposedRevision": strconv.FormatUint(e.ProposedRevision, 10),
		"appliedRevision":  strconv.FormatUint(e.AppliedRevision, 10),
	})
}

func (e *ErrTransactionStateInconsistent) PublicFailureFacts() FailureFacts {
	return NewFailureFacts(e.Reason(), e.Reason(), map[string]string{
		"transactionId": strconv.FormatUint(e.TransactionID, 10),
	})
}

func (e *ErrVolumeNotMaterialized) PublicFailureFacts() FailureFacts {
	return NewFailureFacts(e.Reason(), e.Reason(), map[string]string{
		"account": e.Account, "asset": e.Asset, "color": e.Color, "side": e.Side,
	})
}

func (e *ErrDependencyDiscoveryFailed) PublicDetails() (string, map[string]string, bool) {
	if cause, ok := errors.AsType[SerializableError](e.Cause); ok {
		return PublicErrorDetails(cause)
	}
	if cause, ok := errors.AsType[Describable](e.Cause); ok {
		return strings.ToLower(strings.ReplaceAll(cause.Reason(), "_", " ")), nil, true
	}
	message, facts := RenderFailureFacts(NewFailureFacts(ErrReasonValidation, "DEPENDENCY_DISCOVERY_FAILED", nil))

	return message, facts, true
}
