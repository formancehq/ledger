package controller

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
)

func installLegacySpecHashProof(t *testing.T, ledger *ledgerv1alpha1.Cluster, source *appsv1.StatefulSet, prior *ledgerv1alpha1.ClusterSpec) {
	t.Helper()
	proof := legacySpecHashProof{Version: 1, SourceUID: source.UID, OldSpecHash: source.Spec.Template.Annotations[annotationSpecHash], Spec: prior.DeepCopy()}
	data, err := json.Marshal(proof)
	require.NoError(t, err)
	ledger.Annotations = cloneAnnotations(ledger.Annotations)
	ledger.Annotations[annotationLegacySpecHashProof] = string(data)
}

func TestStorageHashMigrationRejectsUnprovenHistory(t *testing.T) {
	t.Parallel()
	for _, proofState := range []string{"absent", "malformed", "version", "foreign UID", "stale hash", "wrong effective spec", "trailing JSON", "unknown field"} {
		t.Run(proofState, func(t *testing.T) {
			t.Parallel()
			f := newClaimRecoveryFixture(t)
			f.ledger.Spec.Persistence.Data.Size = resource.MustParse("100Gi")
			prior := f.ledger.Spec.DeepCopy()
			prior.Persistence.Data.Size = resource.MustParse("150Gi")
			oldHash := computePodSpecHash(prior, false)
			f.source.CreationTimestamp = metav1.Now()
			f.source.Spec = buildStatefulSetSpec(f.ledger, oldHash, nil, tlsModeDisabled)
			require.NoError(t, f.client.Update(t.Context(), f.source))
			installLegacySpecHashProof(t, f.ledger, f.source, prior)
			var proof legacySpecHashProof
			require.NoError(t, json.Unmarshal([]byte(f.ledger.Annotations[annotationLegacySpecHashProof]), &proof))
			switch proofState {
			case "version":
				proof.Version = 2
			case "foreign UID":
				proof.SourceUID = "different-source"
			case "stale hash":
				proof.OldSpecHash = strings.Repeat("0", 64)
			case "wrong effective spec":
				proof.Spec.ServiceAccount.Create = nil
			}
			data, err := json.Marshal(proof)
			require.NoError(t, err)
			f.ledger.Annotations[annotationLegacySpecHashProof] = string(data)
			switch proofState {
			case "absent":
				delete(f.ledger.Annotations, annotationLegacySpecHashProof)
			case "malformed":
				f.ledger.Annotations[annotationLegacySpecHashProof] = "{"
			case "trailing JSON":
				f.ledger.Annotations[annotationLegacySpecHashProof] += " {}"
			case "unknown field":
				f.ledger.Annotations[annotationLegacySpecHashProof] = strings.TrimSuffix(string(data), "}") + `,"unexpected":true}`
			}
			f.ledger.Spec.Persistence.Data.Size = resource.MustParse("200Gi")
			require.NoError(t, f.client.Update(t.Context(), f.ledger))
			before := f.source.DeepCopy()
			_, err = f.reconciler(f.client).reconcileStatefulSet(t.Context(), f.ledger, computeSpecHash(&f.ledger.Spec), nil)
			require.Error(t, err)
			var got appsv1.StatefulSet
			require.NoError(t, f.client.Get(t.Context(), client.ObjectKeyFromObject(f.source), &got))
			require.Equal(t, before, &got, "no Pod, claim or canonical metadata mutation is allowed before proof validation")
			condition := meta.FindStatusCondition(f.ledger.Status.Conditions, "ClaimTemplatesReady")
			require.NotNil(t, condition)
			reason := "HashMigrationInvalid"
			if proofState == "absent" {
				reason = "HashMigrationRequired"
			}
			require.Equal(t, reason, condition.Reason)
			require.Equal(t, metav1.ConditionFalse, condition.Status)
		})
	}
}

func TestStorageHashMigrationProofDoesNotOverrideWorkload(t *testing.T) {
	t.Parallel()
	for _, workload := range []string{"Pod annotation", "ServiceAccount annotation"} {
		t.Run(workload, func(t *testing.T) {
			t.Parallel()
			f := newClaimRecoveryFixture(t)
			f.ledger.Spec.Persistence.Data.Size = resource.MustParse("100Gi")
			prior := f.ledger.Spec.DeepCopy()
			prior.Persistence.Data.Size = resource.MustParse("150Gi")
			oldHash := computePodSpecHash(prior, false)
			f.source.Spec = buildStatefulSetSpec(f.ledger, oldHash, nil, tlsModeDisabled)
			installLegacySpecHashProof(t, f.ledger, f.source, prior)
			f.ledger.Spec.Persistence.Data.Size = resource.MustParse("200Gi")
			if workload == "Pod annotation" {
				f.ledger.Spec.PodAnnotations = map[string]string{"rollout": "now"}
			} else {
				f.ledger.Spec.ServiceAccount.Annotations = map[string]string{"eks.amazonaws.com/role-arn": "arn:aws:iam::111111111111:role/replacement"}
			}
			hash := computeSpecHash(&f.ledger.Spec)
			desired := buildStatefulSetSpec(f.ledger, hash, nil, tlsModeDisabled).Template
			got, err := f.reconciler(f.client).resolveStorageSpecHash(t.Context(), f.ledger, f.source, desired, hash)
			require.NoError(t, err)
			require.Equal(t, hash, got.Annotations[annotationSpecHash])
		})
	}
}

func TestStorageHashMigrationLeftoverProofDoesNotPoisonCanonicalUpdates(t *testing.T) {
	t.Parallel()
	f := newClaimRecoveryFixture(t)
	f.ledger.Spec.Persistence.Data.Size = resource.MustParse("150Gi")
	prior := f.ledger.Spec.DeepCopy()
	hash := computePodSpecHash(prior, false)
	f.source.Spec = buildStatefulSetSpec(f.ledger, hash, nil, tlsModeDisabled)
	installLegacySpecHashProof(t, f.ledger, f.source, prior)
	f.ledger.Spec.Persistence.Data.Size = resource.MustParse("200Gi")
	canonical := computeSpecHash(&f.ledger.Spec)
	desired := buildStatefulSetSpec(f.ledger, canonical, nil, tlsModeDisabled).Template
	got, err := f.reconciler(f.client).resolveStorageSpecHash(t.Context(), f.ledger, f.source, desired, canonical)
	require.NoError(t, err)
	require.Equal(t, hash, got.Annotations[annotationSpecHash])
	// The journal preserves the Pod hash while recording canonical metadata on
	// the new StatefulSet. The Cluster's proof still refers to the deleted UID.
	f.source.UID = "replacement-source"
	f.source.Spec.Template = got
	recordPodConfigHash(f.source, canonical)
	f.ledger.Annotations[annotationLegacySpecHashProof] = "{stale-proof"
	stable, err := f.reconciler(f.client).resolveStorageSpecHash(t.Context(), f.ledger, f.source, desired, canonical)
	require.NoError(t, err)
	require.Equal(t, got, stable)
	f.ledger.Spec.ServiceAccount.Annotations = map[string]string{"eks.amazonaws.com/role-arn": "arn:aws:iam::111111111111:role/replacement"}
	changed := computeSpecHash(&f.ledger.Spec)
	desired = buildStatefulSetSpec(f.ledger, changed, nil, tlsModeDisabled).Template
	got, err = f.reconciler(f.client).resolveStorageSpecHash(t.Context(), f.ledger, f.source, desired, changed)
	require.NoError(t, err)
	require.Equal(t, changed, got.Annotations[annotationSpecHash])
}

func TestStorageHashMigrationProofRequiresAdmittedTemplateEquality(t *testing.T) {
	t.Parallel()
	f := newClaimRecoveryFixture(t)
	prior := f.ledger.Spec.DeepCopy()
	hash := computePodSpecHash(prior, false)
	f.source.Spec = buildStatefulSetSpec(f.ledger, hash, nil, tlsModeDisabled)
	f.source.Spec.Template.Spec.NodeSelector = map[string]string{"stale-workload": "remove"}
	installLegacySpecHashProof(t, f.ledger, f.source, prior)
	f.ledger.Spec.Persistence.Data.Size = resource.MustParse("200Gi")
	canonical := computeSpecHash(&f.ledger.Spec)
	desired := buildStatefulSetSpec(f.ledger, canonical, nil, tlsModeDisabled).Template
	got, err := f.reconciler(f.client).resolveStorageSpecHash(t.Context(), f.ledger, f.source, desired, canonical)
	require.NoError(t, err)
	require.Equal(t, canonical, got.Annotations[annotationSpecHash])
	require.Empty(t, got.Spec.NodeSelector)
}
