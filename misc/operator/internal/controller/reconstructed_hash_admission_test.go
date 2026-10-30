package controller

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

func TestStorageReconstructedHashAdmissionComparison(t *testing.T) {
	t.Parallel()
	for _, scenario := range []string{"equal", "changed", "denied", "invalid explicit proof"} {
		t.Run(scenario, func(t *testing.T) {
			t.Parallel()
			f := newClaimRecoveryFixture(t)
			f.ledger.Spec.Persistence.Data.Size = resource.MustParse("100Gi")
			oldHash := computePodSpecHash(&f.ledger.Spec, false)
			f.source.Spec = buildStatefulSetSpec(f.ledger, oldHash, nil, tlsModeDisabled)
			f.ledger.Spec.Persistence.Data.Size = resource.MustParse("200Gi")
			require.True(t, legacyStorageSpecHashMatches(f.ledger, f.source), "exercise exact reconstruction, not the opaque-hash fallback")
			before := f.source.DeepCopy()
			if scenario == "invalid explicit proof" {
				f.ledger.Annotations = map[string]string{annotationLegacySpecHashProof: "{"}
			}
			previews := 0
			denied := errors.New("StatefulSet admission unavailable")
			c := interceptor.NewClient(f.client, interceptor.Funcs{
				Update: func(_ context.Context, _ client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
					var options client.UpdateOptions
					options.ApplyOptions(opts)
					require.Equal(t, []string{metav1.DryRunAll}, options.DryRun)
					previews++
					preview := obj.(*appsv1.StatefulSet)
					require.Equal(t, oldHash, preview.Spec.Template.Annotations[annotationSpecHash])
					switch scenario {
					case "changed":
						preview.Spec.Template.Annotations["fixture.example/admission"] = "changed"
					case "denied":
						return denied
					}

					return nil
				},
			})
			canonical := computeSpecHash(&f.ledger.Spec)
			require.NotEqual(t, oldHash, canonical)
			desired := buildStatefulSetSpec(f.ledger, canonical, nil, tlsModeDisabled).Template
			got, err := f.reconciler(c).resolveStorageSpecHash(t.Context(), f.ledger, f.source, desired, canonical)
			switch scenario {
			case "equal":
				require.NoError(t, err)
				require.Equal(t, before.Spec.Template, got, "unchanged admitted storage-only edits keep the installed restart inputs")
			case "changed":
				require.NoError(t, err)
				require.Equal(t, canonical, got.Annotations[annotationSpecHash], "an admitted template change selects the normal canonical rollout")
			case "denied":
				require.ErrorIs(t, err, denied)
			case "invalid explicit proof":
				require.ErrorContains(t, err, "invalid legacy spec-hash proof")
			}
			if scenario == "invalid explicit proof" {
				require.Zero(t, previews, "reconstruction must not bypass invalid supplied history")
			} else {
				require.Equal(t, 1, previews)
			}
			require.Equal(t, before, f.source, "preview and selection must not mutate the source")
			requireNoClaimTemplateRecovery(t, c, f.ledger)
			f.requireStorageUnchanged(t)
		})
	}
}

func TestStorageReconstructedHashAdmissionFailureRetriesBeforeCertification(t *testing.T) {
	t.Parallel()
	f := newClaimRecoveryFixture(t)
	f.ledger.Spec.Persistence.Data.Size = resource.MustParse("100Gi")
	oldHash := computePodSpecHash(&f.ledger.Spec, false)
	f.source.Spec = buildStatefulSetSpec(f.ledger, oldHash, nil, tlsModeDisabled)
	f.source.CreationTimestamp = metav1.Now()
	require.NoError(t, f.client.Update(t.Context(), f.source))
	require.True(t, legacyStorageSpecHashMatches(f.ledger, f.source))
	before := f.source.DeepCopy()
	previews, writes := 0, 0
	denied := errors.New("temporary StatefulSet admission failure")
	c := interceptor.NewClient(f.client, interceptor.Funcs{
		Update: func(ctx context.Context, inner client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
			if _, ok := obj.(*appsv1.StatefulSet); ok {
				var options client.UpdateOptions
				options.ApplyOptions(opts)
				if len(options.DryRun) > 0 {
					previews++
					if previews == 1 {
						return denied
					}
				} else {
					writes++
				}
			}

			return inner.Update(ctx, obj, opts...)
		},
	})
	r := f.reconciler(c)
	canonical := computeSpecHash(&f.ledger.Spec)
	_, err := r.reconcileStatefulSet(t.Context(), f.ledger, canonical, nil)
	require.ErrorIs(t, err, denied)
	require.Equal(t, 1, previews)
	require.Zero(t, writes, "failed preview must precede canonical certification and mutable workload writes")
	condition := meta.FindStatusCondition(f.ledger.Status.Conditions, "ClaimTemplatesReady")
	require.NotNil(t, condition)
	require.Equal(t, "AdmissionCheckFailed", condition.Reason)
	var live appsv1.StatefulSet
	require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(f.source), &live))
	require.Equal(t, before, &live)
	requireNoClaimTemplateRecovery(t, c, f.ledger)
	_, err = r.reconcileStatefulSet(t.Context(), f.ledger, canonical, nil)
	require.NoError(t, err, "recovered admission must permit the original migration")
	require.Equal(t, 2, previews)
	require.Equal(t, 1, writes)
	require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(f.source), &live))
	require.Equal(t, before.Spec.Template, live.Spec.Template)
	require.Equal(t, before.UID, live.UID)
	require.Equal(t, canonical, live.Annotations[annotationPodConfigHash])
	require.Equal(t, oldHash, live.Annotations[annotationPodConfigSpecHash])
	require.True(t, hasPodConfigHashBinding(&live))
	condition = meta.FindStatusCondition(f.ledger.Status.Conditions, "ClaimTemplatesReady")
	require.NotNil(t, condition)
	require.Equal(t, "Ready", condition.Reason)
	requireNoClaimTemplateRecovery(t, c, f.ledger)
	f.requireStorageUnchanged(t)
}
