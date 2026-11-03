//go:build integration

package controller

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
)

// Exercise both CRD selectableFields and the real manager cache index. Fake
// clients alone cannot prove either API-server selection or index registration.
func TestBackupRunParentSelectorIntegration(t *testing.T) {
	t.Parallel()
	mgr, err := ctrl.NewManager(testEnv.Config, ctrl.Options{
		Scheme:  k8sClient.Scheme(),
		Metrics: metricsserver.Options{BindAddress: "0"},
	})
	require.NoError(t, err)
	require.NoError(t, (&BackupRunReconciler{Client: mgr.GetClient(), APIReader: mgr.GetAPIReader(), Scheme: mgr.GetScheme()}).SetupWithManager(mgr))
	cacheCtx, stopCache := context.WithCancel(t.Context())
	cacheDone := make(chan error, 1)
	go func() { cacheDone <- mgr.GetCache().Start(cacheCtx) }()
	t.Cleanup(func() {
		stopCache()
		require.NoError(t, <-cacheDone)
	})
	// Start only the cache: exercise production index registration without
	// running the executor controller against nonexistent Ledger Pods.
	cachedClient := mgr.GetClient()
	namespace := createTestNamespace(t)
	otherNamespace := createTestNamespace(t)
	for _, run := range []*ledgerv1alpha1.BackupRun{
		{ObjectMeta: metav1.ObjectMeta{Name: "manual", Namespace: namespace}, Spec: ledgerv1alpha1.BackupRunSpec{BackupRef: "parent"}},
		{ObjectMeta: metav1.ObjectMeta{Name: "misleading-label", Namespace: namespace, Labels: map[string]string{ledgerv1alpha1.LabelBackup: "other"}}, Spec: ledgerv1alpha1.BackupRunSpec{BackupRef: "parent"}},
		{ObjectMeta: metav1.ObjectMeta{Name: "other-parent", Namespace: namespace, Labels: map[string]string{ledgerv1alpha1.LabelBackup: "parent"}}, Spec: ledgerv1alpha1.BackupRunSpec{BackupRef: "other"}},
		{ObjectMeta: metav1.ObjectMeta{Name: "other-namespace", Namespace: otherNamespace}, Spec: ledgerv1alpha1.BackupRunSpec{BackupRef: "parent"}},
	} {
		require.NoError(t, k8sClient.Create(t.Context(), run))
	}

	for _, tc := range []struct {
		name   string
		reader client.Reader
	}{
		{"api-server", k8sClient},
		{"manager-cache", cachedClient},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Wait for all four cache observations, not just matching objects, so
			// an accidentally broad selector cannot pass before unrelated runs arrive.
			requireEventually(t, func() bool {
				var all ledgerv1alpha1.BackupRunList
				if err := tc.reader.List(t.Context(), &all, client.InNamespace(namespace)); err != nil {
					return false
				}
				var other ledgerv1alpha1.BackupRun
				return len(all.Items) == 3 && tc.reader.Get(t.Context(), client.ObjectKey{Namespace: otherNamespace, Name: "other-namespace"}, &other) == nil
			})
			runs, err := listBackupRuns(t.Context(), tc.reader, client.ObjectKey{Namespace: namespace, Name: "parent"})
			require.NoError(t, err)
			names := make([]string, 0, len(runs))
			for _, run := range runs {
				require.Equal(t, namespace, run.Namespace)
				names = append(names, run.Name)
			}
			require.ElementsMatch(t, []string{"manual", "misleading-label"}, names)
			runs, err = listBackupRuns(t.Context(), tc.reader, client.ObjectKey{Namespace: namespace, Name: "absent"})
			require.NoError(t, err)
			require.Empty(t, runs)
		})
	}
}
