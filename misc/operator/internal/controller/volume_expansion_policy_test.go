package controller

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
)

func TestResolveVolumeExpansionPolicyDefaults(t *testing.T) {
	t.Parallel()

	maximum := resource.MustParse("2Ti")
	policy, err := resolveVolumeExpansionPolicy(&ledgerv1alpha1.VolumeAutoExpansionSpec{
		Enabled:     true,
		MaximumSize: &maximum,
	})
	require.NoError(t, err)
	assert.Equal(t, int32(70), policy.ThresholdPercent)
	assert.Equal(t, int32(55), policy.TargetPercent)
	assert.Equal(t, "10Gi", policy.MinimumIncrement.String())
	assert.Equal(t, "2Ti", policy.MaximumSize.String())
	assert.Equal(t, 8*time.Hour, policy.Cooldown)
}

func TestResolveVolumeExpansionPolicyRejectsInvalidValues(t *testing.T) {
	t.Parallel()

	quantity := func(value string) *resource.Quantity {
		q := resource.MustParse(value)

		return &q
	}
	percentage := func(value int32) *int32 { return &value }
	duration := func(value time.Duration) *metav1.Duration {
		return &metav1.Duration{Duration: value}
	}

	tests := []struct {
		name    string
		spec    *ledgerv1alpha1.VolumeAutoExpansionSpec
		message string
	}{
		{name: "disabled", spec: &ledgerv1alpha1.VolumeAutoExpansionSpec{}, message: "not enabled"},
		{name: "missing maximum", spec: &ledgerv1alpha1.VolumeAutoExpansionSpec{Enabled: true}, message: "maximumSize is required"},
		{name: "threshold range", spec: &ledgerv1alpha1.VolumeAutoExpansionSpec{Enabled: true, MaximumSize: quantity("2Ti"), ThresholdPercent: percentage(100)}, message: "thresholdPercent"},
		{name: "target range", spec: &ledgerv1alpha1.VolumeAutoExpansionSpec{Enabled: true, MaximumSize: quantity("2Ti"), TargetPercent: percentage(0)}, message: "targetPercent"},
		{name: "target ordering", spec: &ledgerv1alpha1.VolumeAutoExpansionSpec{Enabled: true, MaximumSize: quantity("2Ti"), ThresholdPercent: percentage(70), TargetPercent: percentage(70)}, message: "must be lower"},
		{name: "increment", spec: &ledgerv1alpha1.VolumeAutoExpansionSpec{Enabled: true, MaximumSize: quantity("2Ti"), MinimumIncrement: quantity("0")}, message: "minimumIncrement"},
		{name: "maximum", spec: &ledgerv1alpha1.VolumeAutoExpansionSpec{Enabled: true, MaximumSize: quantity("0")}, message: "maximumSize"},
		{name: "cooldown", spec: &ledgerv1alpha1.VolumeAutoExpansionSpec{Enabled: true, MaximumSize: quantity("2Ti"), Cooldown: duration(5 * time.Hour)}, message: "at least 6h"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := resolveVolumeExpansionPolicy(tt.spec)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.message)
		})
	}
}

func TestValidateVolumeSpecAutoExpansion(t *testing.T) {
	t.Parallel()

	quantity := func(value string) *resource.Quantity {
		q := resource.MustParse(value)

		return &q
	}

	tests := []struct {
		name    string
		field   string
		spec    ledgerv1alpha1.VolumeSpec
		allowed bool
		wantErr string
	}{
		{
			name:    "valid data policy",
			field:   "persistence.data",
			spec:    ledgerv1alpha1.VolumeSpec{AutoExpansion: &ledgerv1alpha1.VolumeAutoExpansionSpec{Enabled: true, MaximumSize: quantity("20Gi")}},
			allowed: true,
		},
		{
			name:    "maximum not above initial size",
			field:   "persistence.data",
			spec:    ledgerv1alpha1.VolumeSpec{Size: resource.MustParse("20Gi"), AutoExpansion: &ledgerv1alpha1.VolumeAutoExpansionSpec{Enabled: true, MaximumSize: quantity("20Gi")}},
			allowed: true,
			wantErr: "greater than initial size",
		},
		{
			name:    "host path",
			field:   "persistence.data",
			spec:    ledgerv1alpha1.VolumeSpec{HostPath: &ledgerv1alpha1.HostPathVolumeSpec{Path: "/data"}, AutoExpansion: &ledgerv1alpha1.VolumeAutoExpansionSpec{Enabled: true, MaximumSize: quantity("20Gi")}},
			allowed: true,
			wantErr: "mutually exclusive",
		},
		{
			name:    "cold cache",
			field:   "persistence.coldCache",
			spec:    ledgerv1alpha1.VolumeSpec{AutoExpansion: &ledgerv1alpha1.VolumeAutoExpansionSpec{Enabled: true, MaximumSize: quantity("20Gi")}},
			allowed: false,
			wantErr: "supported only for wal and data",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := validateVolumeSpec(tt.field, &tt.spec, "10Gi", tt.allowed)
			if tt.wantErr == "" {
				require.NoError(t, err)

				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

func TestDecideVolumeExpansion(t *testing.T) {
	t.Parallel()

	policy := volumeExpansionPolicy{
		ThresholdPercent: 70,
		TargetPercent:    55,
		MinimumIncrement: resource.MustParse("10Gi"),
		MaximumSize:      resource.MustParse("200Gi"),
		Cooldown:         8 * time.Hour,
	}
	now := time.Date(2026, time.August, 5, 12, 0, 0, 0, time.UTC)
	boundPVC := func(name, requested, capacity string) volumePVCState {
		return volumePVCState{
			Name:           name,
			RequestedBytes: testQuantityValue(requested),
			CapacityBytes:  testQuantityValue(capacity),
			Bound:          true,
		}
	}
	measurement := func(used, total string) podVolumeMeasurement {
		return podVolumeMeasurement{
			Pod:        "ledger-test-0",
			UsedBytes:  uint64(testQuantityValue(used)),
			TotalBytes: uint64(testQuantityValue(total)),
		}
	}

	tests := []struct {
		name         string
		pvcs         []volumePVCState
		measurements []podVolumeMeasurement
		wantKind     volumeExpansionDecisionKind
		wantTarget   string
	}{
		{name: "missing group", wantKind: volumeExpansionDecisionPending},
		{name: "unbound", pvcs: []volumePVCState{{Name: "data-0", RequestedBytes: testQuantityValue("100Gi")}}, wantKind: volumeExpansionDecisionPending},
		{name: "converges partial patch", pvcs: []volumePVCState{boundPVC("data-0", "120Gi", "100Gi"), boundPVC("data-1", "100Gi", "100Gi")}, wantKind: volumeExpansionDecisionConverge, wantTarget: "120Gi"},
		{name: "resize pending", pvcs: []volumePVCState{boundPVC("data-0", "120Gi", "100Gi")}, wantKind: volumeExpansionDecisionPending},
		{name: "below threshold", pvcs: []volumePVCState{boundPVC("data-0", "100Gi", "100Gi")}, measurements: []podVolumeMeasurement{measurement("69Gi", "100Gi")}, wantKind: volumeExpansionDecisionNone},
		{name: "incomplete below threshold", pvcs: []volumePVCState{boundPVC("data-0", "100Gi", "100Gi")}, measurements: []podVolumeMeasurement{measurement("60Gi", "100Gi"), {Pod: "ledger-test-1", Err: errors.New("unavailable")}}, wantKind: volumeExpansionDecisionIncomplete},
		{name: "expands despite partial measurements", pvcs: []volumePVCState{boundPVC("data-0", "100Gi", "100Gi")}, measurements: []podVolumeMeasurement{measurement("70Gi", "100Gi"), {Pod: "ledger-test-1", Err: errors.New("unavailable")}}, wantKind: volumeExpansionDecisionExpand, wantTarget: "128Gi"},
		{name: "caps at maximum", pvcs: []volumePVCState{boundPVC("data-0", "190Gi", "190Gi")}, measurements: []podVolumeMeasurement{measurement("150Gi", "190Gi")}, wantKind: volumeExpansionDecisionExpand, wantTarget: "200Gi"},
		{name: "reports maximum", pvcs: []volumePVCState{boundPVC("data-0", "200Gi", "200Gi")}, measurements: []podVolumeMeasurement{measurement("150Gi", "200Gi")}, wantKind: volumeExpansionDecisionLimit, wantTarget: "200Gi"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			decision := decideVolumeExpansion(policy, tt.pvcs, tt.measurements, now)
			assert.Equal(t, tt.wantKind, decision.Kind)
			if tt.wantTarget != "" {
				assert.Equal(t, testQuantityValue(tt.wantTarget), decision.TargetBytes)
			}
		})
	}
}

func TestDecideVolumeExpansionHonorsCooldown(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.August, 5, 12, 0, 0, 0, time.UTC)
	policy := volumeExpansionPolicy{
		ThresholdPercent: 70,
		TargetPercent:    55,
		MinimumIncrement: resource.MustParse("10Gi"),
		MaximumSize:      resource.MustParse("200Gi"),
		Cooldown:         8 * time.Hour,
	}
	pvc := volumePVCState{
		Name:            "data-0",
		RequestedBytes:  testQuantityValue("100Gi"),
		CapacityBytes:   testQuantityValue("100Gi"),
		Bound:           true,
		LastExpansionAt: now.Add(-7 * time.Hour),
	}

	decision := decideVolumeExpansion(policy, []volumePVCState{pvc}, []podVolumeMeasurement{{
		Pod:        "ledger-test-0",
		UsedBytes:  uint64(testQuantityValue("80Gi")),
		TotalBytes: uint64(testQuantityValue("100Gi")),
	}}, now)
	assert.Equal(t, volumeExpansionDecisionCooldown, decision.Kind)
}

func testQuantityValue(value string) int64 {
	quantity := resource.MustParse(value)

	return quantity.Value()
}
