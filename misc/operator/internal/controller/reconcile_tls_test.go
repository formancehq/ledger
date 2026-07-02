package controller

import (
	"testing"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	ledgerv1alpha1 "github.com/formance/ledger/operator/api/v1alpha1"
)

func TestComputeTargetTLSMode(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		desired   string
		actual    string
		converged bool
		want      string
	}{
		{
			name:    "bootstrap to disabled",
			desired: tlsModeDisabled,
			actual:  "",
			want:    tlsModeDisabled,
		},
		{
			name:    "bootstrap to required (no rolling restart needed)",
			desired: tlsModeRequired,
			actual:  "",
			want:    tlsModeRequired,
		},
		{
			name:    "stable disabled",
			desired: tlsModeDisabled,
			actual:  tlsModeDisabled,
			want:    tlsModeDisabled,
		},
		{
			name:    "stable required",
			desired: tlsModeRequired,
			actual:  tlsModeRequired,
			want:    tlsModeRequired,
		},
		{
			name:    "toggle enable: disabled -> optional first",
			desired: tlsModeRequired,
			actual:  tlsModeDisabled,
			want:    tlsModeOptional,
		},
		{
			name:    "toggle disable: required -> optional first",
			desired: tlsModeDisabled,
			actual:  tlsModeRequired,
			want:    tlsModeOptional,
		},
		{
			name:      "mid-migration not converged: stay optional (enable path)",
			desired:   tlsModeRequired,
			actual:    tlsModeOptional,
			converged: false,
			want:      tlsModeOptional,
		},
		{
			name:      "mid-migration converged: advance to desired (enable path)",
			desired:   tlsModeRequired,
			actual:    tlsModeOptional,
			converged: true,
			want:      tlsModeRequired,
		},
		{
			name:      "mid-migration not converged: stay optional (disable path)",
			desired:   tlsModeDisabled,
			actual:    tlsModeOptional,
			converged: false,
			want:      tlsModeOptional,
		},
		{
			name:      "mid-migration converged: advance to desired (disable path)",
			desired:   tlsModeDisabled,
			actual:    tlsModeOptional,
			converged: true,
			want:      tlsModeDisabled,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := computeTargetTLSMode(tt.desired, tt.actual, tt.converged)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestDesiredTLSMode(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		spec *ledgerv1alpha1.TLSConfig
		want string
	}{
		{name: "nil spec → disabled", spec: nil, want: tlsModeDisabled},
		{name: "explicit disabled", spec: &ledgerv1alpha1.TLSConfig{Enabled: false}, want: tlsModeDisabled},
		{name: "explicit enabled", spec: &ledgerv1alpha1.TLSConfig{Enabled: true}, want: tlsModeRequired},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ls := &ledgerv1alpha1.Cluster{Spec: ledgerv1alpha1.ClusterSpec{TLS: tt.spec}}
			require.Equal(t, tt.want, desiredTLSMode(ls))
		})
	}
}

func TestShouldInjectClusterSecret(t *testing.T) {
	t.Parallel()

	require.False(t, shouldInjectClusterSecret(""))
	require.False(t, shouldInjectClusterSecret(tlsModeDisabled))
	require.True(t, shouldInjectClusterSecret(tlsModeOptional))
	require.True(t, shouldInjectClusterSecret(tlsModeRequired))
}

func TestCurrentTLSModeFromStatefulSet(t *testing.T) {
	t.Parallel()

	t.Run("nil sts returns empty", func(t *testing.T) {
		t.Parallel()

		require.Equal(t, "", currentTLSModeFromStatefulSet(nil))
	})

	t.Run("no ledger container returns empty", func(t *testing.T) {
		t.Parallel()

		sts := &appsv1.StatefulSet{
			Spec: appsv1.StatefulSetSpec{
				Template: corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{{Name: "sidecar"}},
					},
				},
			},
		}
		require.Equal(t, "", currentTLSModeFromStatefulSet(sts))
	})

	t.Run("reads TLS_MODE env from ledger container", func(t *testing.T) {
		t.Parallel()

		sts := &appsv1.StatefulSet{
			Spec: appsv1.StatefulSetSpec{
				Template: corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{
							{Name: "ledger", Env: []corev1.EnvVar{{Name: "TLS_MODE", Value: "optional"}}},
						},
					},
				},
			},
		}
		require.Equal(t, tlsModeOptional, currentTLSModeFromStatefulSet(sts))
	})
}

func TestRolloutConverged(t *testing.T) {
	t.Parallel()

	replicas := int32(3)

	t.Run("nil sts is not converged", func(t *testing.T) {
		t.Parallel()
		require.False(t, rolloutConverged(nil))
	})

	t.Run("observedGeneration behind is not converged", func(t *testing.T) {
		t.Parallel()
		sts := &appsv1.StatefulSet{
			ObjectMeta: metav1.ObjectMeta{Generation: 5},
			Spec:       appsv1.StatefulSetSpec{Replicas: &replicas},
			Status: appsv1.StatefulSetStatus{
				ObservedGeneration: 4,
				ReadyReplicas:      3,
				UpdatedReplicas:    3,
			},
		}
		require.False(t, rolloutConverged(sts))
	})

	t.Run("partial rollout not converged", func(t *testing.T) {
		t.Parallel()
		sts := &appsv1.StatefulSet{
			ObjectMeta: metav1.ObjectMeta{Generation: 5},
			Spec:       appsv1.StatefulSetSpec{Replicas: &replicas},
			Status: appsv1.StatefulSetStatus{
				ObservedGeneration: 5,
				ReadyReplicas:      2,
				UpdatedReplicas:    2,
			},
		}
		require.False(t, rolloutConverged(sts))
	})

	t.Run("fully rolled out", func(t *testing.T) {
		t.Parallel()
		sts := &appsv1.StatefulSet{
			ObjectMeta: metav1.ObjectMeta{Generation: 5},
			Spec:       appsv1.StatefulSetSpec{Replicas: &replicas},
			Status: appsv1.StatefulSetStatus{
				ObservedGeneration: 5,
				ReadyReplicas:      3,
				UpdatedReplicas:    3,
			},
		}
		require.True(t, rolloutConverged(sts))
	})
}

func TestTLSMigrationPhase(t *testing.T) {
	t.Parallel()

	require.Equal(t, TLSPhaseDisabled, tlsMigrationPhase(tlsModeDisabled, tlsModeDisabled))
	require.Equal(t, TLSPhaseRequired, tlsMigrationPhase(tlsModeRequired, tlsModeRequired))
	require.Equal(t, TLSPhaseTransitioningToRequired, tlsMigrationPhase(tlsModeRequired, tlsModeOptional))
	require.Equal(t, TLSPhaseTransitioningToDisabled, tlsMigrationPhase(tlsModeDisabled, tlsModeOptional))
}
