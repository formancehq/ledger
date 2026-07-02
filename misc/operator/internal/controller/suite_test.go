//go:build integration

package controller

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	ledgerv1alpha1 "github.com/formance/ledger/operator/api/v1alpha1"
)

var (
	testEnv   *envtest.Environment
	k8sClient client.Client
	ctx       context.Context
	cancel    context.CancelFunc
	nsCounter atomic.Int64
)

func TestMain(m *testing.M) {
	ctrl.SetLogger(zap.New(zap.UseDevMode(true)))

	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		panic(fmt.Sprintf("adding client-go scheme: %v", err))
	}
	if err := ledgerv1alpha1.AddToScheme(scheme); err != nil {
		panic(fmt.Sprintf("adding ledger scheme: %v", err))
	}

	testEnv = &envtest.Environment{
		CRDDirectoryPaths: []string{
			filepath.Join("..", "..", "config", "crd", "bases"),
		},
		Scheme: scheme,
	}

	cfg, err := testEnv.Start()
	if err != nil {
		panic(fmt.Sprintf("starting envtest: %v", err))
	}

	k8sClient, err = client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		panic(fmt.Sprintf("creating client: %v", err))
	}

	ctx, cancel = context.WithCancel(context.Background())

	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme: scheme,
		Metrics: metricsserver.Options{
			BindAddress: "0",
		},
	})
	if err != nil {
		panic(fmt.Sprintf("creating manager: %v", err))
	}

	if err := (&ClusterReconciler{
		Client:   mgr.GetClient(),
		Scheme:   mgr.GetScheme(),
		Recorder: mgr.GetEventRecorderFor("cluster-controller"),
	}).SetupWithManager(mgr); err != nil {
		panic(fmt.Sprintf("setting up Cluster controller: %v", err))
	}

	if err := (&LedgerClusterAgentReconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
	}).SetupWithManager(mgr); err != nil {
		panic(fmt.Sprintf("setting up LedgerClusterAgent controller: %v", err))
	}

	if err := (&LedgerBackupReconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
	}).SetupWithManager(mgr); err != nil {
		panic(fmt.Sprintf("setting up LedgerBackup controller: %v", err))
	}

	// NOTE: LedgerBackupRunReconciler is intentionally NOT wired into envtest because
	// it would attempt to exec ledgerctl in non-existent pods. Tests in this suite
	// cover LedgerBackup → Run creation, scheduling, and retention. End-to-end Run
	// execution is covered by chainsaw e2e tests against a real cluster.

	go func() {
		if err := mgr.Start(ctx); err != nil {
			panic(fmt.Sprintf("running manager: %v", err))
		}
	}()

	code := m.Run()

	cancel()
	if err := testEnv.Stop(); err != nil {
		panic(fmt.Sprintf("stopping envtest: %v", err))
	}

	os.Exit(code)
}

// createTestNamespace creates a unique namespace for test isolation.
func createTestNamespace(t *testing.T) string {
	t.Helper()
	name := fmt.Sprintf("test-%d", nsCounter.Add(1))
	ns := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: name},
	}
	require.NoError(t, k8sClient.Create(ctx, ns))
	t.Cleanup(func() {
		_ = k8sClient.Delete(ctx, ns) //nolint:errcheck // best-effort cleanup
	})
	return name
}

// newCluster returns a minimal valid Cluster CR.
func newCluster(name, namespace string) *ledgerv1alpha1.Cluster {
	replicas := int32(3)
	return &ledgerv1alpha1.Cluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: ledgerv1alpha1.ClusterSpec{
			Replicas: &replicas,
		},
	}
}

// newLedgerClusterAgent returns a cluster-scoped LedgerClusterAgent with a label selector.
func newLedgerClusterAgent(name string, scopes []string, matchLabels map[string]string) *ledgerv1alpha1.LedgerClusterAgent {
	return &ledgerv1alpha1.LedgerClusterAgent{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Spec: ledgerv1alpha1.LedgerClusterAgentSpec{
			Scopes: scopes,
			Selector: metav1.LabelSelector{
				MatchLabels: matchLabels,
			},
		},
	}
}

// newLedgerClusterAgentWithAdditional returns a cluster-scoped LedgerClusterAgent
// that distributes its Secret to the given additional namespaces (regardless of
// matched services). Useful for tests that only need a Secret to exist somewhere.
func newLedgerClusterAgentWithAdditional(name string, scopes []string, matchLabels map[string]string, additional ...string) *ledgerv1alpha1.LedgerClusterAgent {
	agent := newLedgerClusterAgent(name, scopes, matchLabels)
	agent.Spec.AdditionalNamespaces = additional

	return agent
}

// requireEventually wraps require.Eventually with standard timeouts for envtest.
func requireEventually(t *testing.T, condition func() bool, msgAndArgs ...interface{}) {
	t.Helper()
	require.Eventually(t, condition, 10*time.Second, 250*time.Millisecond, msgAndArgs...)
}
