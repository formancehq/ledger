package controller

import (
	"testing"

	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	ledgerv1alpha1 "github.com/formance/ledger/operator/api/v1alpha1"
)

func TestVolumeClaimTemplatesChanged(t *testing.T) {
	tests := []struct {
		name     string
		existing []corev1.PersistentVolumeClaim
		desired  []corev1.PersistentVolumeClaim
		changed  bool
	}{
		{
			name:     "identical",
			existing: []corev1.PersistentVolumeClaim{{ObjectMeta: metav1.ObjectMeta{Name: "wal"}}, {ObjectMeta: metav1.ObjectMeta{Name: "data"}}},
			desired:  []corev1.PersistentVolumeClaim{{ObjectMeta: metav1.ObjectMeta{Name: "wal"}}, {ObjectMeta: metav1.ObjectMeta{Name: "data"}}},
			changed:  false,
		},
		{
			name:     "volume removed (switched to hostPath)",
			existing: []corev1.PersistentVolumeClaim{{ObjectMeta: metav1.ObjectMeta{Name: "wal"}}, {ObjectMeta: metav1.ObjectMeta{Name: "data"}}, {ObjectMeta: metav1.ObjectMeta{Name: "cold-cache"}}},
			desired:  []corev1.PersistentVolumeClaim{{ObjectMeta: metav1.ObjectMeta{Name: "wal"}}, {ObjectMeta: metav1.ObjectMeta{Name: "cold-cache"}}},
			changed:  true,
		},
		{
			name:     "volume added (switched from hostPath to PVC)",
			existing: []corev1.PersistentVolumeClaim{{ObjectMeta: metav1.ObjectMeta{Name: "wal"}}},
			desired:  []corev1.PersistentVolumeClaim{{ObjectMeta: metav1.ObjectMeta{Name: "wal"}}, {ObjectMeta: metav1.ObjectMeta{Name: "data"}}},
			changed:  true,
		},
		{
			name:     "all removed (all hostPath)",
			existing: []corev1.PersistentVolumeClaim{{ObjectMeta: metav1.ObjectMeta{Name: "wal"}}, {ObjectMeta: metav1.ObjectMeta{Name: "data"}}},
			desired:  nil,
			changed:  true,
		},
		{
			name:     "both empty",
			existing: nil,
			desired:  nil,
			changed:  false,
		},
		{
			name:     "different order same names",
			existing: []corev1.PersistentVolumeClaim{{ObjectMeta: metav1.ObjectMeta{Name: "data"}}, {ObjectMeta: metav1.ObjectMeta{Name: "wal"}}},
			desired:  []corev1.PersistentVolumeClaim{{ObjectMeta: metav1.ObjectMeta{Name: "wal"}}, {ObjectMeta: metav1.ObjectMeta{Name: "data"}}},
			changed:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.changed, volumeClaimTemplatesChanged(tt.existing, tt.desired))
		})
	}
}

func TestPvcVolumeNames(t *testing.T) {
	tests := []struct {
		name        string
		persistence ledgerv1alpha1.PersistenceSpec
		expected    []string
	}{
		{
			name:        "all PVC (default)",
			persistence: ledgerv1alpha1.PersistenceSpec{},
			expected:    []string{"wal", "data", "cold-cache"},
		},
		{
			name: "data hostPath",
			persistence: ledgerv1alpha1.PersistenceSpec{
				Data: ledgerv1alpha1.VolumeSpec{
					HostPath: &ledgerv1alpha1.HostPathVolumeSpec{Path: "/mnt/nvme0/data"},
				},
			},
			expected: []string{"wal", "cold-cache"},
		},
		{
			name: "all hostPath",
			persistence: ledgerv1alpha1.PersistenceSpec{
				WAL: ledgerv1alpha1.VolumeSpec{
					HostPath: &ledgerv1alpha1.HostPathVolumeSpec{Path: "/mnt/nvme0/wal"},
				},
				Data: ledgerv1alpha1.VolumeSpec{
					HostPath: &ledgerv1alpha1.HostPathVolumeSpec{Path: "/mnt/nvme0/data"},
				},
				ColdCache: ledgerv1alpha1.VolumeSpec{
					HostPath: &ledgerv1alpha1.HostPathVolumeSpec{Path: "/mnt/nvme0/cold-cache"},
				},
			},
			expected: nil,
		},
		{
			name: "only wal hostPath",
			persistence: ledgerv1alpha1.PersistenceSpec{
				WAL: ledgerv1alpha1.VolumeSpec{
					HostPath: &ledgerv1alpha1.HostPathVolumeSpec{Path: "/mnt/nvme0/wal"},
				},
			},
			expected: []string{"data", "cold-cache"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, pvcVolumeNames(&tt.persistence))
		})
	}
}
