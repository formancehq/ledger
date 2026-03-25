//go:build integration

package controller

import (
	"context"
	"encoding/json"
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
	"k8s.io/client-go/dynamic"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	benchmarkv1alpha1 "github.com/formancehq/ledger-v3-poc/misc/benchmark-operator/api/v1alpha1"
)

var (
	testEnv       *envtest.Environment
	k8sClient     client.Client
	dynamicClient dynamic.Interface
	ctx           context.Context
	cancel        context.CancelFunc
	nsCounter     atomic.Int64
)

func TestMain(m *testing.M) {
	ctrl.SetLogger(zap.New(zap.UseDevMode(true)))

	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		panic(fmt.Sprintf("adding client-go scheme: %v", err))
	}
	if err := benchmarkv1alpha1.AddToScheme(scheme); err != nil {
		panic(fmt.Sprintf("adding benchmark scheme: %v", err))
	}

	testEnv = &envtest.Environment{
		CRDDirectoryPaths: []string{
			filepath.Join("..", "..", "config", "crd", "bases"),
			filepath.Join("..", "..", "config", "testdata"),
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

	dynamicClient, err = dynamic.NewForConfig(cfg)
	if err != nil {
		panic(fmt.Sprintf("creating dynamic client: %v", err))
	}

	ctx, cancel = context.WithCancel(context.Background())

	// Grafana client with no-op URL (tests won't call Grafana).
	grafana := NewGrafanaClient(Config{
		GrafanaURL: "http://localhost:0",
	})

	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme: scheme,
		Metrics: metricsserver.Options{
			BindAddress: "0",
		},
	})
	if err != nil {
		panic(fmt.Sprintf("creating manager: %v", err))
	}

	if err := (&BenchmarkReconciler{
		Client:  mgr.GetClient(),
		Scheme:  mgr.GetScheme(),
		Dynamic: dynamicClient,
		Grafana: grafana,
	}).SetupWithManager(mgr); err != nil {
		panic(fmt.Sprintf("setting up Benchmark controller: %v", err))
	}

	if err := (&TestRunReconciler{
		Dynamic:        dynamicClient,
		Grafana:        grafana,
		WatchNamespace: "",
	}).SetupWithManager(mgr); err != nil {
		panic(fmt.Sprintf("setting up TestRun controller: %v", err))
	}

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

// testServiceGVR is the GVR for the test Service CRD (example.com/v1alpha1/services).
var testServiceGVR = parseGVR("example.com/v1alpha1", "services")

// newBenchmark returns a minimal valid Benchmark CR with one test resource.
func newBenchmark(name, namespace string) *benchmarkv1alpha1.Benchmark {
	serviceManifest, _ := json.Marshal(map[string]any{
		"apiVersion": "example.com/v1alpha1",
		"kind":       "Service",
		"spec": map[string]any{
			"replicas": 1,
		},
	})
	trSpec, _ := json.Marshal(map[string]any{
		"script": map[string]any{
			"configMap": map[string]any{
				"name": "test-script",
				"file": "test.js",
			},
		},
	})

	return &benchmarkv1alpha1.Benchmark{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: benchmarkv1alpha1.BenchmarkSpec{
			Resources: []benchmarkv1alpha1.ResourceEntry{
				{
					Manifest: runtime.RawExtension{Raw: serviceManifest},
					ReadyCondition: benchmarkv1alpha1.ReadyCondition{
						FieldPath: "status.phase",
						Value:     "Running",
					},
				},
			},
			TestRun: runtime.RawExtension{Raw: trSpec},
		},
	}
}

// requireEventually wraps require.Eventually with standard timeouts for envtest.
func requireEventually(t *testing.T, condition func() bool, msgAndArgs ...interface{}) {
	t.Helper()
	require.Eventually(t, condition, 10*time.Second, 250*time.Millisecond, msgAndArgs...)
}
