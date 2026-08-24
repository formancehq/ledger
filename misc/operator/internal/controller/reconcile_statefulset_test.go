package controller

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
)

func TestBuildStatefulSetSpec_ExplicitRollingUpdate(t *testing.T) {
	t.Parallel()

	ls := &ledgerv1alpha1.Cluster{
		ObjectMeta: metav1.ObjectMeta{Name: "ls", Namespace: "default"},
		Spec:       ledgerv1alpha1.ClusterSpec{},
	}

	spec := buildStatefulSetSpec(ls, "hash", nil, "disabled")

	assert.Equal(t, appsv1.RollingUpdateStatefulSetStrategyType, spec.UpdateStrategy.Type)
	require.NotNil(t, spec.UpdateStrategy.RollingUpdate)
	require.NotNil(t, spec.UpdateStrategy.RollingUpdate.Partition)
	assert.Equal(t, int32(0), *spec.UpdateStrategy.RollingUpdate.Partition,
		"Partition=0 ensures all pods are rolled when the template hash changes")
}

func TestBuildVolumeClaimTemplates_DeletionProtectionLabel(t *testing.T) {
	t.Parallel()

	newLedger := func(protect bool) *ledgerv1alpha1.Cluster {
		return &ledgerv1alpha1.Cluster{
			ObjectMeta: metav1.ObjectMeta{Name: "ls", Namespace: "default"},
			Spec: ledgerv1alpha1.ClusterSpec{
				Persistence: ledgerv1alpha1.PersistenceSpec{DeletionProtection: &protect},
			},
		}
	}

	// When protection is on, every PVC-backed template carries the label so the
	// PVCs the StatefulSet controller provisions are protected from creation,
	// closing the window before the first stamp reconcile.
	protected := buildVolumeClaimTemplates(newLedger(true))
	require.NotEmpty(t, protected)
	for _, tmpl := range protected {
		assert.Equal(t, labelDeletionProtectionValue, tmpl.Labels[labelDeletionProtection],
			"VCT %q must carry the deletion-protection label when protection is on", tmpl.Name)
	}

	// When protection is off, the label must be absent so disabled ledgers are
	// not selected by the admission policy.
	unprotected := buildVolumeClaimTemplates(newLedger(false))
	require.NotEmpty(t, unprotected)
	for _, tmpl := range unprotected {
		_, present := tmpl.Labels[labelDeletionProtection]
		assert.False(t, present, "VCT %q must not carry the deletion-protection label when protection is off", tmpl.Name)
	}
}

func TestWithKubectlRestartedAt(t *testing.T) {
	t.Parallel()

	newTemplate := func(annotations map[string]string) corev1.PodTemplateSpec {
		return corev1.PodTemplateSpec{
			ObjectMeta: metav1.ObjectMeta{Annotations: annotations},
		}
	}

	t.Run("carries the stamp from the existing template", func(t *testing.T) {
		t.Parallel()

		existing := newTemplate(map[string]string{
			annotationKubectlRestartedAt: "2026-08-24T10:00:00Z",
			annotationSpecHash:           "old-hash",
		})
		desired := newTemplate(map[string]string{annotationSpecHash: "new-hash"})

		got := withKubectlRestartedAt(existing, desired)

		assert.Equal(t, "2026-08-24T10:00:00Z", got.Annotations[annotationKubectlRestartedAt],
			"kubectl rollout restart stamp must survive the reconcile (EN-1850)")
		assert.Equal(t, "new-hash", got.Annotations[annotationSpecHash],
			"operator-owned annotations must still come from the desired template")
	})

	t.Run("returns the desired template unchanged when no stamp exists", func(t *testing.T) {
		t.Parallel()

		existing := newTemplate(map[string]string{annotationSpecHash: "old-hash"})
		desired := newTemplate(map[string]string{annotationSpecHash: "new-hash"})

		got := withKubectlRestartedAt(existing, desired)

		assert.Equal(t, desired.Annotations, got.Annotations)
		_, present := got.Annotations[annotationKubectlRestartedAt]
		assert.False(t, present)
	})

	t.Run("a CR-provided value wins over the live stamp", func(t *testing.T) {
		t.Parallel()

		existing := newTemplate(map[string]string{annotationKubectlRestartedAt: "2026-08-24T10:00:00Z"})
		desired := newTemplate(map[string]string{
			annotationKubectlRestartedAt: "2026-08-25T09:00:00Z",
			annotationSpecHash:           "new-hash",
		})

		got := withKubectlRestartedAt(existing, desired)

		assert.Equal(t, "2026-08-25T09:00:00Z", got.Annotations[annotationKubectlRestartedAt],
			"a restartedAt explicitly managed through spec.podAnnotations must not be masked by the live stamp")
	})

	t.Run("does not mutate the desired template's annotations map", func(t *testing.T) {
		t.Parallel()

		existing := newTemplate(map[string]string{annotationKubectlRestartedAt: "2026-08-24T10:00:00Z"})
		sharedAnnotations := map[string]string{annotationSpecHash: "new-hash"}
		desired := newTemplate(sharedAnnotations)

		got := withKubectlRestartedAt(existing, desired)

		assert.Equal(t, "2026-08-24T10:00:00Z", got.Annotations[annotationKubectlRestartedAt])
		_, present := sharedAnnotations[annotationKubectlRestartedAt]
		assert.False(t, present, "the shared desired annotations map must not be mutated")
	})
}
