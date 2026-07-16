package controller

import (
	"testing"

	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
)

func TestBuildTopologySpreadConstraints_DefaultsLabelSelector(t *testing.T) {
	t.Parallel()

	ledger := &ledgerv1alpha1.Cluster{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "ns"},
		Spec: ledgerv1alpha1.ClusterSpec{
			TopologySpreadConstraints: []corev1.TopologySpreadConstraint{
				{
					MaxSkew:           1,
					TopologyKey:       "topology.kubernetes.io/zone",
					WhenUnsatisfiable: corev1.DoNotSchedule,
				},
			},
		},
	}

	got := buildTopologySpreadConstraints(ledger)
	if assert.Len(t, got, 1) {
		c := got[0]
		assert.Equal(t, int32(1), c.MaxSkew)
		assert.Equal(t, "topology.kubernetes.io/zone", c.TopologyKey)
		assert.Equal(t, corev1.DoNotSchedule, c.WhenUnsatisfiable)
		if assert.NotNil(t, c.LabelSelector) {
			assert.Equal(t, selectorLabels(ledger), c.LabelSelector.MatchLabels)
		}
	}
}

func TestBuildTopologySpreadConstraints_PreservesUserLabelSelector(t *testing.T) {
	t.Parallel()

	userSelector := &metav1.LabelSelector{MatchLabels: map[string]string{"custom": "true"}}
	ledger := &ledgerv1alpha1.Cluster{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "ns"},
		Spec: ledgerv1alpha1.ClusterSpec{
			TopologySpreadConstraints: []corev1.TopologySpreadConstraint{
				{
					MaxSkew:           2,
					TopologyKey:       "kubernetes.io/hostname",
					WhenUnsatisfiable: corev1.ScheduleAnyway,
					LabelSelector:     userSelector,
				},
			},
		},
	}

	got := buildTopologySpreadConstraints(ledger)
	if assert.Len(t, got, 1) {
		c := got[0]
		assert.Equal(t, userSelector, c.LabelSelector, "user-provided selector must not be overridden")
	}
}

func TestBuildTopologySpreadConstraints_DeepCopiesInput(t *testing.T) {
	t.Parallel()

	ledger := &ledgerv1alpha1.Cluster{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "ns"},
		Spec: ledgerv1alpha1.ClusterSpec{
			TopologySpreadConstraints: []corev1.TopologySpreadConstraint{
				{
					MaxSkew:           1,
					TopologyKey:       "topology.kubernetes.io/zone",
					WhenUnsatisfiable: corev1.DoNotSchedule,
				},
			},
		},
	}

	got := buildTopologySpreadConstraints(ledger)
	got[0].MaxSkew = 99

	assert.Equal(t, int32(1), ledger.Spec.TopologySpreadConstraints[0].MaxSkew,
		"mutating the returned slice must not affect the Cluster spec")
}
