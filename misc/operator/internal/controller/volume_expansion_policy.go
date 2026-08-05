package controller

import (
	"errors"
	"fmt"
	"math"
	"time"

	"k8s.io/apimachinery/pkg/api/resource"

	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
)

const (
	defaultVolumeExpansionThresholdPercent int32 = 70
	defaultVolumeExpansionTargetPercent    int32 = 55
	defaultVolumeExpansionCooldown               = 8 * time.Hour
	minimumVolumeExpansionCooldown               = 6 * time.Hour
	bytesPerGiB                                  = int64(1024 * 1024 * 1024)
)

var defaultVolumeExpansionMinimumIncrement = resource.MustParse("10Gi")

type volumeExpansionPolicy struct {
	ThresholdPercent int32
	TargetPercent    int32
	MinimumIncrement resource.Quantity
	MaximumSize      resource.Quantity
	Cooldown         time.Duration
}

func resolveVolumeExpansionPolicy(spec *ledgerv1alpha1.VolumeAutoExpansionSpec) (volumeExpansionPolicy, error) {
	if spec == nil || !spec.Enabled {
		return volumeExpansionPolicy{}, errors.New("policy is not enabled")
	}

	policy := volumeExpansionPolicy{
		ThresholdPercent: defaultVolumeExpansionThresholdPercent,
		TargetPercent:    defaultVolumeExpansionTargetPercent,
		MinimumIncrement: defaultVolumeExpansionMinimumIncrement.DeepCopy(),
		Cooldown:         defaultVolumeExpansionCooldown,
	}
	if spec.ThresholdPercent != nil {
		policy.ThresholdPercent = *spec.ThresholdPercent
	}
	if spec.TargetPercent != nil {
		policy.TargetPercent = *spec.TargetPercent
	}
	if spec.MinimumIncrement != nil {
		policy.MinimumIncrement = spec.MinimumIncrement.DeepCopy()
	}
	if spec.MaximumSize == nil {
		return volumeExpansionPolicy{}, errors.New("maximumSize is required when enabled")
	}
	policy.MaximumSize = spec.MaximumSize.DeepCopy()
	if spec.Cooldown != nil {
		policy.Cooldown = spec.Cooldown.Duration
	}

	switch {
	case policy.ThresholdPercent < 1 || policy.ThresholdPercent > 99:
		return volumeExpansionPolicy{}, fmt.Errorf("thresholdPercent must be between 1 and 99, got %d", policy.ThresholdPercent)
	case policy.TargetPercent < 1 || policy.TargetPercent > 98:
		return volumeExpansionPolicy{}, fmt.Errorf("targetPercent must be between 1 and 98, got %d", policy.TargetPercent)
	case policy.TargetPercent >= policy.ThresholdPercent:
		return volumeExpansionPolicy{}, fmt.Errorf("targetPercent (%d) must be lower than thresholdPercent (%d)", policy.TargetPercent, policy.ThresholdPercent)
	case policy.MinimumIncrement.Sign() <= 0:
		return volumeExpansionPolicy{}, errors.New("minimumIncrement must be greater than zero")
	case policy.MaximumSize.Sign() <= 0:
		return volumeExpansionPolicy{}, errors.New("maximumSize must be greater than zero")
	case policy.Cooldown < minimumVolumeExpansionCooldown:
		return volumeExpansionPolicy{}, fmt.Errorf("cooldown must be at least %s, got %s", minimumVolumeExpansionCooldown, policy.Cooldown)
	}

	return policy, nil
}

type volumePVCState struct {
	Name            string
	RequestedBytes  int64
	CapacityBytes   int64
	Bound           bool
	ResizePending   bool
	LastExpansionAt time.Time
}

type podVolumeMeasurement struct {
	Pod        string
	UsedBytes  uint64
	TotalBytes uint64
	Err        error
}

type volumeExpansionDecisionKind string

const (
	volumeExpansionDecisionNone       volumeExpansionDecisionKind = "none"
	volumeExpansionDecisionConverge   volumeExpansionDecisionKind = "converge"
	volumeExpansionDecisionPending    volumeExpansionDecisionKind = "pending"
	volumeExpansionDecisionCooldown   volumeExpansionDecisionKind = "cooldown"
	volumeExpansionDecisionIncomplete volumeExpansionDecisionKind = "measurement-incomplete"
	volumeExpansionDecisionExpand     volumeExpansionDecisionKind = "expand"
	volumeExpansionDecisionLimit      volumeExpansionDecisionKind = "limit-reached"
)

type volumeExpansionDecision struct {
	Kind                volumeExpansionDecisionKind
	TargetBytes         int64
	LargestRequestBytes int64
	MaxUsedBytes        uint64
	MaxUsageRatio       float64
	FailedMeasurements  int
	LastExpansionAt     time.Time
}

// decideVolumeExpansion is the pure policy module. It deliberately knows
// nothing about Kubernetes clients, pod exec, StorageClasses, events, or
// metrics: callers provide one complete PVC group plus the observations and
// receive the single next action that preserves convergence and cooldown.
func decideVolumeExpansion(
	policy volumeExpansionPolicy,
	pvcs []volumePVCState,
	measurements []podVolumeMeasurement,
	now time.Time,
) volumeExpansionDecision {
	decision := volumeExpansionDecision{Kind: volumeExpansionDecisionNone}
	if len(pvcs) == 0 {
		decision.Kind = volumeExpansionDecisionPending

		return decision
	}

	smallestRequest := int64(math.MaxInt64)
	for _, pvc := range pvcs {
		if pvc.RequestedBytes > decision.LargestRequestBytes {
			decision.LargestRequestBytes = pvc.RequestedBytes
		}
		if pvc.RequestedBytes < smallestRequest {
			smallestRequest = pvc.RequestedBytes
		}
		if pvc.LastExpansionAt.After(decision.LastExpansionAt) {
			decision.LastExpansionAt = pvc.LastExpansionAt
		}
		if !pvc.Bound {
			decision.Kind = volumeExpansionDecisionPending

			return decision
		}
	}

	// A previous multi-PVC patch may have succeeded only partially. Complete
	// that target before consulting usage, resize state, or cooldown so every
	// Raft replica converges to one capacity.
	if smallestRequest < decision.LargestRequestBytes {
		decision.Kind = volumeExpansionDecisionConverge
		decision.TargetBytes = decision.LargestRequestBytes

		return decision
	}

	for _, pvc := range pvcs {
		if pvc.ResizePending || pvc.CapacityBytes < pvc.RequestedBytes {
			decision.Kind = volumeExpansionDecisionPending

			return decision
		}
	}

	if !decision.LastExpansionAt.IsZero() && now.Before(decision.LastExpansionAt.Add(policy.Cooldown)) {
		decision.Kind = volumeExpansionDecisionCooldown

		return decision
	}

	validMeasurements := 0
	thresholdExceeded := false
	for _, measurement := range measurements {
		if measurement.Err != nil || measurement.TotalBytes == 0 {
			decision.FailedMeasurements++

			continue
		}
		validMeasurements++
		ratio := float64(measurement.UsedBytes) / float64(measurement.TotalBytes)
		if ratio > decision.MaxUsageRatio {
			decision.MaxUsageRatio = ratio
		}
		if measurement.UsedBytes > decision.MaxUsedBytes {
			decision.MaxUsedBytes = measurement.UsedBytes
		}
		if measurement.UsedBytes*100 >= measurement.TotalBytes*uint64(policy.ThresholdPercent) {
			thresholdExceeded = true
		}
	}

	if validMeasurements == 0 || (!thresholdExceeded && decision.FailedMeasurements > 0) {
		decision.Kind = volumeExpansionDecisionIncomplete

		return decision
	}
	if !thresholdExceeded {
		return decision
	}

	maximumBytes := policy.MaximumSize.Value()
	if decision.LargestRequestBytes >= maximumBytes {
		decision.Kind = volumeExpansionDecisionLimit
		decision.TargetBytes = maximumBytes

		return decision
	}

	minimumTarget := decision.LargestRequestBytes + policy.MinimumIncrement.Value()
	usageTarget := ceilDivideUint64(decision.MaxUsedBytes*100, uint64(policy.TargetPercent))
	targetBytes := min(roundUpToGiB(max(minimumTarget, int64(usageTarget))), maximumBytes)
	if targetBytes <= decision.LargestRequestBytes {
		decision.Kind = volumeExpansionDecisionLimit
		decision.TargetBytes = maximumBytes

		return decision
	}

	decision.Kind = volumeExpansionDecisionExpand
	decision.TargetBytes = targetBytes

	return decision
}

func ceilDivideUint64(value, divisor uint64) uint64 {
	return (value + divisor - 1) / divisor
}

func roundUpToGiB(value int64) int64 {
	return ((value + bytesPerGiB - 1) / bytesPerGiB) * bytesPerGiB
}
