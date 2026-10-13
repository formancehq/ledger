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

func (*ErrInvalidCallerAttribution) PublicFailureFacts() FailureFacts {
	return NewFailureFacts(ErrReasonInvalidCallerAttribution, ErrReasonInvalidCallerAttribution, nil)
}

func (e *ErrExecutionPlanTooLarge) PublicFailureFacts() FailureFacts {
	return NewFailureFacts(ErrReasonExecutionPlanTooLarge, ErrReasonExecutionPlanTooLarge, map[string]string{
		"size":  strconv.FormatUint(uint64(e.Size), 10),
		"limit": strconv.FormatUint(uint64(e.Limit), 10),
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
