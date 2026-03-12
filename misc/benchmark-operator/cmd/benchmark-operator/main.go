package main

import (
	"flag"
	"os"

	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	"k8s.io/client-go/dynamic"

	benchmarkv1alpha1 "github.com/formancehq/ledger-v3-poc/misc/benchmark-operator/api/v1alpha1"
	"github.com/formancehq/ledger-v3-poc/misc/benchmark-operator/internal/controller"
)

type flags struct {
	metricsAddr    string
	probeAddr      string
	leaderElect    bool
	watchNamespace string
}

func main() {
	var f flags
	flag.StringVar(&f.metricsAddr, "metrics-bind-address", ":8080", "The address the metric endpoint binds to.")
	flag.StringVar(&f.probeAddr, "health-probe-bind-address", ":8081", "The address the probe endpoint binds to.")
	flag.BoolVar(&f.leaderElect, "leader-elect", false, "Enable leader election for controller manager.")
	flag.StringVar(&f.watchNamespace, "watch-namespace", "", "Namespace to watch. Empty string watches all namespaces.")

	opts := zap.Options{Development: true}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))
	setupLog := ctrl.Log.WithName("setup")

	scheme := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(benchmarkv1alpha1.AddToScheme(scheme))

	cfg := controller.LoadConfigFromEnv()

	mgrOpts := ctrl.Options{
		Scheme: scheme,
		Metrics: metricsserver.Options{
			BindAddress: f.metricsAddr,
		},
		HealthProbeBindAddress: f.probeAddr,
		LeaderElection:         f.leaderElect,
		LeaderElectionID:       "benchmark-operator.formance.com",
	}
	if f.watchNamespace != "" {
		mgrOpts.Cache.DefaultNamespaces = map[string]cache.Config{
			f.watchNamespace: {},
		}
	}

	restConfig := ctrl.GetConfigOrDie()

	mgr, err := ctrl.NewManager(restConfig, mgrOpts)
	if err != nil {
		setupLog.Error(err, "unable to start manager")
		os.Exit(1)
	}

	dynamicClient, err := dynamic.NewForConfig(restConfig)
	if err != nil {
		setupLog.Error(err, "unable to create dynamic client")
		os.Exit(1)
	}

	grafana := controller.NewGrafanaClient(cfg)

	if err = (&controller.BenchmarkReconciler{
		Client:  mgr.GetClient(),
		Scheme:  mgr.GetScheme(),
		Dynamic: dynamicClient,
		Grafana: grafana,
		Ledger:  &controller.LedgerClient{},
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "Benchmark")
		os.Exit(1)
	}

	if err = (&controller.TestRunReconciler{
		Dynamic:        dynamicClient,
		Grafana:        grafana,
		WatchNamespace: f.watchNamespace,
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "TestRun")
		os.Exit(1)
	}

	if err = mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up health check")
		os.Exit(1)
	}
	if err = mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up ready check")
		os.Exit(1)
	}

	setupLog.Info("starting manager")
	if err = mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		setupLog.Error(err, "problem running manager")
		os.Exit(1)
	}
}
