package controller

import (
	"testing"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestCanonicalHashBindingReleasedWriterRequiresHistoricalProof(t *testing.T) {
	t.Parallel()
	f := newClaimRecoveryFixture(t)
	f.ledger.Spec.Persistence.Data.Size = resource.MustParse("100Gi")
	f.ledger.Spec.ServiceAccount.Annotations = map[string]string{"fixture.example/admission": "A"}
	canonicalA := computeSpecHash(&f.ledger.Spec)
	f.source.CreationTimestamp = metav1.Now()
	f.source.Spec = buildStatefulSetSpec(f.ledger, canonicalA, nil, tlsModeDisabled)
	recordPodConfigHash(f.source, canonicalA)
	// A released writer changes the external admission inputs and its full
	// Pod hash, while preserving the unknown canonical metadata from A.
	priorB := f.ledger.Spec.DeepCopy()
	priorB.ServiceAccount.Annotations["fixture.example/admission"] = "B"
	installedB := computePodSpecHash(priorB, false)
	f.source.Spec.Template.Annotations[annotationSpecHash] = installedB
	require.NoError(t, f.client.Update(t.Context(), f.source))
	require.NoError(t, f.client.Update(t.Context(), f.ledger))
	original := f.source.DeepCopy()
	r := f.reconciler(f.client)
	_, err := r.reconcileStatefulSet(t.Context(), f.ledger, canonicalA, nil)
	require.ErrorContains(t, err, "cannot reconstruct the released Pod spec hash")
	condition := meta.FindStatusCondition(f.ledger.Status.Conditions, "ClaimTemplatesReady")
	require.NotNil(t, condition)
	require.Equal(t, "HashMigrationRequired", condition.Reason)
	var live appsv1.StatefulSet
	require.NoError(t, f.client.Get(t.Context(), client.ObjectKeyFromObject(f.source), &live))
	require.Equal(t, original, &live, "stale metadata cannot certify B as A")
	requireNoClaimTemplateRecovery(t, f.client, f.ledger)

	installLegacySpecHashProof(t, f.ledger, f.source, priorB)
	require.NoError(t, f.client.Update(t.Context(), f.ledger))
	_, err = r.reconcileStatefulSet(t.Context(), f.ledger, canonicalA, nil)
	require.NoError(t, err)
	require.NoError(t, f.client.Get(t.Context(), client.ObjectKeyFromObject(f.source), &live))
	require.Equal(t, canonicalA, live.Spec.Template.Annotations[annotationSpecHash], "authentic B proof requires a fresh A rollout")
	require.NotEqual(t, installedB, live.Spec.Template.Annotations[annotationSpecHash])
	require.Equal(t, canonicalA, live.Annotations[annotationPodConfigHash])
	require.Equal(t, canonicalA, live.Annotations[annotationPodConfigSpecHash])
	require.True(t, hasPodConfigHashBinding(&live))
	require.Equal(t, original.UID, live.UID)
	require.Equal(t, original.Spec.VolumeClaimTemplates, live.Spec.VolumeClaimTemplates)
	requireNoClaimTemplateRecovery(t, f.client, f.ledger)
	// Current paired state ignores the proof now referring to the old hash.
	_, err = f.reconciler(f.client).reconcileStatefulSet(t.Context(), f.ledger, canonicalA, nil)
	require.NoError(t, err)
	var again appsv1.StatefulSet
	require.NoError(t, f.client.Get(t.Context(), client.ObjectKeyFromObject(f.source), &again))
	require.Equal(t, live, again)
}

func TestCanonicalHashBindingMissingOrBrokenUsesLegacyMigration(t *testing.T) {
	t.Parallel()
	for _, state := range []string{"missing canonical", "missing association", "wrong association", "both missing"} {
		for _, exact := range []bool{false, true} {
			t.Run(state+map[bool]string{false: "/opaque", true: "/reconstructable"}[exact], func(t *testing.T) {
				t.Parallel()
				f := newClaimRecoveryFixture(t)
				f.ledger.Spec.Persistence.Data.Size = resource.MustParse("100Gi")
				prior := f.ledger.Spec.DeepCopy()
				if !exact {
					prior.Persistence.Data.Size = resource.MustParse("150Gi")
				}
				old := computePodSpecHash(prior, false)
				f.source.Spec = buildStatefulSetSpec(f.ledger, old, nil, tlsModeDisabled)
				f.ledger.Spec.Persistence.Data.Size = resource.MustParse("200Gi")
				canonical := computeSpecHash(&f.ledger.Spec)
				recordPodConfigHash(f.source, canonical)
				switch state {
				case "missing canonical":
					delete(f.source.Annotations, annotationPodConfigHash)
				case "missing association":
					delete(f.source.Annotations, annotationPodConfigSpecHash)
				case "wrong association":
					f.source.Annotations[annotationPodConfigSpecHash] = "different-installed-hash"
				case "both missing":
					delete(f.source.Annotations, annotationPodConfigHash)
					delete(f.source.Annotations, annotationPodConfigSpecHash)
				}
				require.False(t, hasPodConfigHashBinding(f.source))
				before := f.source.DeepCopy()
				desired := buildStatefulSetSpec(f.ledger, canonical, nil, tlsModeDisabled).Template
				helper := preserveStorageSpecHash(f.ledger, f.source, *desired.DeepCopy(), canonical)
				got, err := f.reconciler(f.client).resolveStorageSpecHash(t.Context(), f.ledger, f.source, *desired.DeepCopy(), canonical)
				require.Equal(t, before, f.source, "hash resolution must not mutate the source")
				if exact {
					require.NoError(t, err)
					require.Equal(t, old, helper.Annotations[annotationSpecHash])
					require.Equal(t, old, got.Annotations[annotationSpecHash], "association loss alone cannot force a storage-only rollout")
					recordPodConfigHash(f.source, canonical)
					require.Equal(t, old, f.source.Annotations[annotationPodConfigSpecHash], "bind the retained historical hash")
				} else {
					require.Equal(t, canonical, helper.Annotations[annotationSpecHash])
					require.ErrorContains(t, err, "cannot reconstruct")
					f.ledger.Annotations = map[string]string{annotationLegacySpecHashProof: "{"}
					_, err = f.reconciler(f.client).resolveStorageSpecHash(t.Context(), f.ledger, f.source, *desired.DeepCopy(), canonical)
					require.ErrorContains(t, err, "invalid legacy spec-hash proof")
				}
			})
		}
	}
}

func TestCanonicalHashBindingValidPairTakesPrecedence(t *testing.T) {
	t.Parallel()
	for _, change := range []bool{false, true} {
		t.Run(map[bool]string{false: "unchanged", true: "changed canonical"}[change], func(t *testing.T) {
			t.Parallel()
			f := newClaimRecoveryFixture(t)
			canonical := computeSpecHash(&f.ledger.Spec)
			f.source.Spec = buildStatefulSetSpec(f.ledger, "retained-historical-pod-hash", nil, tlsModeDisabled)
			recordPodConfigHash(f.source, canonical)
			f.source.UID = "replacement-uid"
			f.source.Generation++
			f.ledger.Annotations = map[string]string{annotationLegacySpecHashProof: "{leftover-proof"}
			if change {
				f.ledger.Spec.ServiceAccount.Annotations = map[string]string{"fixture.example/admission": "changed"}
			}
			wanted := computeSpecHash(&f.ledger.Spec)
			desired := buildStatefulSetSpec(f.ledger, wanted, nil, tlsModeDisabled).Template
			got, err := f.reconciler(f.client).resolveStorageSpecHash(t.Context(), f.ledger, f.source, *desired.DeepCopy(), wanted)
			require.NoError(t, err)
			helper := preserveStorageSpecHash(f.ledger, f.source, *desired.DeepCopy(), wanted)
			expected := "retained-historical-pod-hash"
			if change {
				expected = wanted
			}
			require.Equal(t, expected, got.Annotations[annotationSpecHash])
			require.Equal(t, expected, helper.Annotations[annotationSpecHash])
		})
	}
}

func TestCanonicalHashBindingMissingInstalledHashDoesNotTrustMetadata(t *testing.T) {
	t.Parallel()
	f := newClaimRecoveryFixture(t)
	canonical := computeSpecHash(&f.ledger.Spec)
	f.source.Spec = buildStatefulSetSpec(f.ledger, canonical, nil, tlsModeDisabled)
	recordPodConfigHash(f.source, canonical)
	delete(f.source.Spec.Template.Annotations, annotationSpecHash)
	require.False(t, hasPodConfigHashBinding(f.source))
	desired := buildStatefulSetSpec(f.ledger, canonical, nil, tlsModeDisabled).Template
	got, err := f.reconciler(f.client).resolveStorageSpecHash(t.Context(), f.ledger, f.source, *desired.DeepCopy(), canonical)
	require.NoError(t, err)
	require.Equal(t, desired, got)
	require.Equal(t, desired, preserveStorageSpecHash(f.ledger, f.source, *desired.DeepCopy(), canonical))
}

func TestCanonicalHashBindingUnpairedCanonicalHashIsNotHistoricalEvidence(t *testing.T) {
	t.Parallel()
	f := newClaimRecoveryFixture(t)
	f.ledger.Spec.Persistence.Data.Size = resource.MustParse("100Gi")
	f.ledger.Spec.Persistence.Data.StorageClass = "fast"
	f.ledger.Spec.Persistence.WAL.StorageClass = "fast"
	canonical := computeSpecHash(&f.ledger.Spec)
	f.source.Spec = buildStatefulSetSpec(f.ledger, canonical, nil, tlsModeDisabled)
	f.source.Annotations[annotationPodConfigHash] = canonical
	desired := buildStatefulSetSpec(f.ledger, canonical, nil, tlsModeDisabled).Template
	_, err := f.reconciler(f.client).resolveStorageSpecHash(t.Context(), f.ledger, f.source, desired, canonical)
	require.ErrorContains(t, err, "cannot reconstruct", "equal hash strings without a pair do not establish released historical inputs")
}

func TestCanonicalHashBindingSnapshotSurvivesAdoptionWithoutHashDrift(t *testing.T) {
	t.Parallel()
	f := newClaimRecoveryFixture(t)
	f.ledger.Spec.Persistence.Data.Size = resource.MustParse("100Gi")
	prior := f.ledger.Spec.DeepCopy()
	prior.Persistence.Data.Size = resource.MustParse("150Gi")
	installed := computePodSpecHash(prior, false)
	f.source.Spec = buildStatefulSetSpec(f.ledger, installed, nil, tlsModeDisabled)
	f.source.CreationTimestamp = metav1.Now()
	require.NoError(t, f.client.Update(t.Context(), f.source))
	installLegacySpecHashProof(t, f.ledger, f.source, prior)
	f.ledger.Spec.Persistence.Data.Size = resource.MustParse("200Gi")
	require.NoError(t, f.client.Update(t.Context(), f.ledger))
	canonical := computeSpecHash(&f.ledger.Spec)
	r := f.reconciler(f.client)
	_, err := r.reconcileStatefulSet(t.Context(), f.ledger, canonical, nil)
	require.NoError(t, err)
	journal, saved := f.journal(t)
	journalKey := client.ObjectKeyFromObject(journal)
	require.Equal(t, canonical, saved.StatefulSet.Annotations[annotationPodConfigHash])
	require.Equal(t, installed, saved.StatefulSet.Annotations[annotationPodConfigSpecHash])
	require.Equal(t, installed, saved.StatefulSet.Spec.Template.Annotations[annotationSpecHash])
	require.True(t, hasPodConfigHashBinding(&saved.StatefulSet))
	handled, _, err := r.resumeClaimTemplateRecovery(t.Context(), f.latestLedger(t))
	require.True(t, handled)
	require.NoError(t, err)
	replacement := f.replacement(t)
	f.adoptPods(t, replacement)
	replacement.Status.ObservedGeneration = replacement.Generation
	require.NoError(t, f.client.Status().Update(t.Context(), replacement))
	handled, _, err = f.reconciler(f.client).resumeClaimTemplateRecovery(t.Context(), f.latestLedger(t))
	require.True(t, handled)
	require.NoError(t, err)
	require.True(t, f.latestLedger(t).Status.ClaimTemplateRecovery.Completed)
	handled, _, err = f.reconciler(f.client).resumeClaimTemplateRecovery(t.Context(), f.latestLedger(t))
	require.True(t, handled)
	require.NoError(t, err)
	requireNoClaimTemplateRecovery(t, f.client, f.ledger)
	require.True(t, apierrors.IsNotFound(f.client.Get(t.Context(), journalKey, journal)), "cleanup uses the captured digest-derived journal identity")
	before := replacement.Spec.Template.DeepCopy()
	latest := f.latestLedger(t)
	for range 2 {
		_, err = f.reconciler(f.client).reconcileStatefulSet(t.Context(), latest, canonical, nil)
		require.NoError(t, err, "the retained pair remains valid after source UID replacement and stale proof")
		require.NoError(t, f.client.Get(t.Context(), client.ObjectKeyFromObject(replacement), replacement))
		require.Equal(t, *before, replacement.Spec.Template)
		require.Equal(t, installed, replacement.Annotations[annotationPodConfigSpecHash])
		require.True(t, hasPodConfigHashBinding(replacement))
	}
	f.requireStorageUnchanged(t)
}

func TestCanonicalHashBindingReleasedNoOpPublishesRetainedHashPair(t *testing.T) {
	t.Parallel()
	for _, broken := range []bool{false, true} {
		t.Run(map[bool]string{false: "released metadata absent", true: "association broken"}[broken], func(t *testing.T) {
			t.Parallel()
			f := newClaimRecoveryFixture(t)
			f.ledger.Spec.Persistence.Data.Size = resource.MustParse("100Gi")
			old := computePodSpecHash(&f.ledger.Spec, false)
			canonical := computeSpecHash(&f.ledger.Spec)
			require.NotEqual(t, canonical, old)
			f.source.CreationTimestamp = metav1.Now()
			f.source.Spec = buildStatefulSetSpec(f.ledger, old, nil, tlsModeDisabled)
			if broken {
				f.source.Annotations[annotationPodConfigHash] = canonical
				f.source.Annotations[annotationPodConfigSpecHash] = "previous-installed-hash"
			}
			require.NoError(t, f.client.Update(t.Context(), f.source))
			require.NoError(t, f.client.Update(t.Context(), f.ledger))
			before := f.source.Spec.DeepCopy()
			for range 2 {
				_, err := f.reconciler(f.client).reconcileStatefulSet(t.Context(), f.ledger, canonical, nil)
				require.NoError(t, err)
				var live appsv1.StatefulSet
				require.NoError(t, f.client.Get(t.Context(), client.ObjectKeyFromObject(f.source), &live))
				require.Equal(t, *before, live.Spec, "a verified released no-op cannot request Pod restarts")
				require.Equal(t, canonical, live.Annotations[annotationPodConfigHash])
				require.Equal(t, old, live.Annotations[annotationPodConfigSpecHash])
				require.True(t, hasPodConfigHashBinding(&live))
				requireNoClaimTemplateRecovery(t, f.client, f.ledger)
			}
		})
	}
}
