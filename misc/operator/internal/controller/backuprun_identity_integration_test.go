//go:build integration

package controller

import (
	"testing"

	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
)

func TestBackupRunAdmissionRejectsParentChanges(t *testing.T) {
	t.Parallel()
	for _, phase := range []ledgerv1alpha1.BackupRunPhase{"", ledgerv1alpha1.BackupRunPhasePending, ledgerv1alpha1.BackupRunPhaseRunning, ledgerv1alpha1.BackupRunPhaseSucceeded, ledgerv1alpha1.BackupRunPhaseFailed} {
		for _, method := range []string{"update", "patch"} {
			t.Run(string(phase)+"/"+method, func(t *testing.T) {
				t.Parallel()
				run := &ledgerv1alpha1.BackupRun{
					ObjectMeta: metav1.ObjectMeta{Name: "manual", Namespace: createTestNamespace(t)},
					Spec:       ledgerv1alpha1.BackupRunSpec{BackupRef: "original"},
				}
				require.NoError(t, k8sClient.Create(t.Context(), run))
				require.Equal(t, ledgerv1alpha1.BackupRunTypeFull, run.Spec.Type)
				run.Status.Phase = phase
				require.NoError(t, k8sClient.Status().Update(t.Context(), run))
				original := run.DeepCopy()
				run.Spec.BackupRef = "replacement"
				run.Labels = map[string]string{"example.com/note": "changed"}
				var err error
				if method == "patch" {
					err = k8sClient.Patch(t.Context(), run, client.MergeFrom(original))
				} else {
					err = k8sClient.Update(t.Context(), run)
				}
				require.True(t, apierrors.IsInvalid(err), "parent changes must fail admission: %v", err)
				require.Contains(t, err.Error(), "spec.backupRef")
				require.Contains(t, err.Error(), "backupRef is immutable")
				require.NoError(t, k8sClient.Get(t.Context(), client.ObjectKeyFromObject(run), run))
				require.Equal(t, original.Spec, run.Spec)
				require.Equal(t, original.Status, run.Status)
				require.Equal(t, original.Labels, run.Labels, "rejected combined edits must be atomic")
				// Same-parent updates and metadata management remain supported.
				require.NoError(t, k8sClient.Update(t.Context(), run))
				run.Labels = map[string]string{"example.com/note": "allowed"}
				require.NoError(t, k8sClient.Update(t.Context(), run))
			})
		}
	}
}

// Use raw objects and patches because the typed value cannot represent an absent spec.
func TestBackupRunAdmissionRequiresSpec(t *testing.T) {
	t.Parallel()
	t.Run("create", func(t *testing.T) {
		t.Parallel()
		run := &unstructured.Unstructured{Object: map[string]interface{}{
			"apiVersion": ledgerv1alpha1.GroupVersion.String(), "kind": "BackupRun",
			"metadata": map[string]interface{}{"name": "missing-spec", "namespace": createTestNamespace(t)},
		}}
		err := k8sClient.Create(t.Context(), run)
		require.True(t, apierrors.IsInvalid(err), "spec-less creates must fail admission: %v", err)
		require.Contains(t, err.Error(), "spec: Required value")
	})
	for _, method := range []string{"update", "merge-patch", "json-patch"} {
		t.Run(method, func(t *testing.T) {
			t.Parallel()
			run := &ledgerv1alpha1.BackupRun{
				ObjectMeta: metav1.ObjectMeta{Name: "manual", Namespace: createTestNamespace(t)},
				Spec:       ledgerv1alpha1.BackupRunSpec{BackupRef: "original"},
			}
			require.NoError(t, k8sClient.Create(t.Context(), run))
			run.Status.Phase = ledgerv1alpha1.BackupRunPhaseRunning
			require.NoError(t, k8sClient.Status().Update(t.Context(), run))
			original := run.DeepCopy()
			var err error
			switch method {
			case "update":
				raw := &unstructured.Unstructured{}
				raw.SetGroupVersionKind(ledgerv1alpha1.GroupVersion.WithKind("BackupRun"))
				require.NoError(t, k8sClient.Get(t.Context(), client.ObjectKeyFromObject(run), raw))
				delete(raw.Object, "spec")
				err = k8sClient.Update(t.Context(), raw)
			case "merge-patch":
				err = k8sClient.Patch(t.Context(), run, client.RawPatch(types.MergePatchType, []byte(`{"spec":null}`)))
			case "json-patch":
				err = k8sClient.Patch(t.Context(), run, client.RawPatch(types.JSONPatchType, []byte(`[{"op":"remove","path":"/spec"}]`)))
			}
			require.True(t, apierrors.IsInvalid(err), "spec removal must fail admission: %v", err)
			require.Contains(t, err.Error(), "spec: Required value")
			require.NoError(t, k8sClient.Get(t.Context(), client.ObjectKeyFromObject(run), run))
			require.Equal(t, original.Spec, run.Spec)
			require.Equal(t, original.Status, run.Status)
			// A subsequent re-add attempt must still compare against the original parent.
			err = k8sClient.Patch(t.Context(), run, client.RawPatch(types.MergePatchType, []byte(`{"spec":{"backupRef":"replacement"}}`)))
			require.True(t, apierrors.IsInvalid(err), "re-adding a different parent must fail admission: %v", err)
			require.Contains(t, err.Error(), "backupRef is immutable")
			require.NoError(t, k8sClient.Get(t.Context(), client.ObjectKeyFromObject(run), run))
			require.Equal(t, original.Spec, run.Spec)
			require.Equal(t, original.Status, run.Status)
		})
	}
}
