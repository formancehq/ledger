//go:build integration

package controller

import (
	"testing"

	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"

	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
)

func TestLedgerAdmissionRejectsTargetChanges(t *testing.T) {
	t.Parallel()

	for _, lifecycle := range []string{"new", "pending", "ready", "deleting"} {
		for _, target := range []string{"name", "clusterRef", "both with promotion and indexes"} {
			t.Run(lifecycle+"/"+target, func(t *testing.T) {
				t.Parallel()

				ledger := newLedger("identity", createTestNamespace(t), "cluster-a", "ledger-a")
				ledger.Spec.Mode = "mirror"
				ledger.Spec.MirrorSource = &ledgerv1alpha1.MirrorSourceSpec{
					HTTP: &ledgerv1alpha1.HTTPMirrorSource{BaseURL: "https://source.example.com"},
				}
				if lifecycle == "deleting" {
					// Keep the resource in deletion so admission is exercised before
					// a controller can finish finalization.
					ledger.Finalizers = []string{"ledger.formance.com/test-admission"}
				}
				require.NoError(t, k8sClient.Create(ctx, ledger))
				switch lifecycle {
				case "pending":
					ledger.Status.Phase = ledgerv1alpha1.LedgerPhasePending
					require.NoError(t, k8sClient.Status().Update(ctx, ledger))
				case "ready":
					ledger.Status.Phase = ledgerv1alpha1.LedgerPhaseReady
					require.NoError(t, k8sClient.Status().Update(ctx, ledger))
				case "deleting":
					require.NoError(t, k8sClient.Delete(ctx, ledger))
					t.Cleanup(func() {
						var current ledgerv1alpha1.Ledger
						if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(ledger), &current); apierrors.IsNotFound(err) {
							return
						} else {
							require.NoError(t, err)
						}
						current.Finalizers = nil
						require.NoError(t, k8sClient.Update(ctx, &current))
					})
				}
				require.NoError(t, k8sClient.Get(ctx, client.ObjectKeyFromObject(ledger), ledger))
				if lifecycle == "deleting" {
					require.False(t, ledger.DeletionTimestamp.IsZero())
				}
				original := ledger.DeepCopy()
				if target != "clusterRef" {
					ledger.Spec.Name = "ledger-b"
				}
				if target != "name" {
					ledger.Spec.ClusterRef = "cluster-b"
				}
				if target == "both with promotion and indexes" {
					ledger.Spec.Mode = "normal"
					ledger.Spec.MirrorSource = nil
					ledger.Spec.Indexes = &ledgerv1alpha1.LedgerIndexesSpec{Account: []string{"asset"}}
				}

				err := k8sClient.Update(ctx, ledger)
				require.True(t, apierrors.IsInvalid(err), "target changes must fail admission: %v", err)
				if target != "clusterRef" {
					require.Contains(t, err.Error(), "spec.name")
					require.Contains(t, err.Error(), "name is immutable")
				}
				if target != "name" {
					require.Contains(t, err.Error(), "spec.clusterRef")
					require.Contains(t, err.Error(), "clusterRef is immutable")
				}
				var stored ledgerv1alpha1.Ledger
				require.NoError(t, k8sClient.Get(ctx, client.ObjectKeyFromObject(ledger), &stored))
				require.Equal(t, original.Spec, stored.Spec, "rejection must preserve the entire stored spec")
			})
		}
	}
}

func TestLedgerAdmissionAllowsSupportedUpdates(t *testing.T) {
	t.Parallel()

	ledger := newLedger("mutable", createTestNamespace(t), "cluster-a", "ledger-a")
	ledger.Spec.Mode = "mirror"
	ledger.Spec.MirrorSource = &ledgerv1alpha1.MirrorSourceSpec{
		HTTP: &ledgerv1alpha1.HTTPMirrorSource{BaseURL: "https://source.example.com"},
	}
	require.NoError(t, k8sClient.Create(ctx, ledger), "initial mirror creation must remain valid")

	for _, step := range []struct {
		name   string
		mutate func(*ledgerv1alpha1.Ledger)
	}{
		{name: "equal identity", mutate: func(*ledgerv1alpha1.Ledger) {}},
		{name: "manage indexes", mutate: func(ledger *ledgerv1alpha1.Ledger) {
			ledger.Spec.Indexes = &ledgerv1alpha1.LedgerIndexesSpec{Transaction: []string{"reference"}}
		}},
		{name: "edit indexes", mutate: func(ledger *ledgerv1alpha1.Ledger) {
			ledger.Spec.Indexes = &ledgerv1alpha1.LedgerIndexesSpec{Account: []string{"asset"}}
		}},
		{name: "clear managed indexes", mutate: func(ledger *ledgerv1alpha1.Ledger) {
			ledger.Spec.Indexes = &ledgerv1alpha1.LedgerIndexesSpec{}
		}},
		{name: "unmanage indexes", mutate: func(ledger *ledgerv1alpha1.Ledger) {
			ledger.Spec.Indexes = nil
		}},
		{name: "promote mirror", mutate: func(ledger *ledgerv1alpha1.Ledger) {
			ledger.Spec.Mode = "normal"
			ledger.Spec.MirrorSource = nil
		}},
	} {
		t.Run(step.name, func(t *testing.T) {
			step.mutate(ledger)
			want := ledger.DeepCopy()
			require.NoError(t, k8sClient.Update(ctx, ledger))
			require.NoError(t, k8sClient.Get(ctx, client.ObjectKeyFromObject(ledger), ledger))
			require.Equal(t, want.Spec, ledger.Spec)
		})
	}
}
