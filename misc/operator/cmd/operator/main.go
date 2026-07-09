package main

import (
	"flag"
	"os"

	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	ledgerv1alpha1 "github.com/formance/ledger/operator/api/v1alpha1"
	"github.com/formance/ledger/operator/internal/controller"
)

type flags struct {
	metricsAddr    string
	probeAddr      string
	leaderElect    bool
	dev            bool
	watchNamespace string
}

func main() {
	var f flags
	flag.StringVar(&f.metricsAddr, "metrics-bind-address", ":8080", "The address the metric endpoint binds to.")
	flag.StringVar(&f.probeAddr, "health-probe-bind-address", ":8081", "The address the probe endpoint binds to.")
	flag.BoolVar(&f.leaderElect, "leader-elect", false, "Enable leader election for controller manager.")
	flag.StringVar(&f.watchNamespace, "watch-namespace", "", "Namespace to watch. Empty string watches all namespaces.")
	flag.BoolVar(&f.dev, "dev", false, "Enable development mode (verbose logging).")

	opts := zap.Options{}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()

	opts.Development = f.dev
	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))
	setupLog := ctrl.Log.WithName("setup")

	scheme := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(ledgerv1alpha1.AddToScheme(scheme))

	// Discover the operator's own namespace up-front. Canonical seed
	// Secrets live there and are accessed via an uncached APIReader (see
	// CredentialsReconciler.APIReader), so --watch-namespace scope
	// stays exactly what the user asked for — the operator namespace is
	// NOT added to the cache.
	operatorNamespace, err := controller.DiscoverOperatorNamespace()
	if err != nil {
		setupLog.Error(err, "unable to determine operator namespace")
		os.Exit(1)
	}

	mgrOpts := ctrl.Options{
		Scheme: scheme,
		Metrics: metricsserver.Options{
			BindAddress: f.metricsAddr,
		},
		HealthProbeBindAddress: f.probeAddr,
		LeaderElection:         f.leaderElect,
		LeaderElectionID:       "ledger-operator.formance.com",
	}
	if f.watchNamespace != "" {
		mgrOpts.Cache.DefaultNamespaces = map[string]cache.Config{
			f.watchNamespace: {},
		}
	}

	cfg := ctrl.GetConfigOrDie()

	mgr, err := ctrl.NewManager(cfg, mgrOpts)
	if err != nil {
		setupLog.Error(err, "unable to start manager")
		os.Exit(1)
	}

	clientset, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		setupLog.Error(err, "unable to create kubernetes clientset")
		os.Exit(1)
	}

	dynamicClient, err := dynamic.NewForConfig(cfg)
	if err != nil {
		setupLog.Error(err, "unable to create dynamic client")
		os.Exit(1)
	}

	if err = (&controller.ClusterReconciler{
		Client:    mgr.GetClient(),
		Scheme:    mgr.GetScheme(),
		Config:    cfg,
		Clientset: clientset,
		Recorder:  mgr.GetEventRecorderFor("cluster-controller"),
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "Cluster")
		os.Exit(1)
	}

	if err = (&controller.CredentialsReconciler{
		Client:            mgr.GetClient(),
		Scheme:            mgr.GetScheme(),
		OperatorNamespace: operatorNamespace,
		APIReader:         mgr.GetAPIReader(),
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "Credentials")
		os.Exit(1)
	}

	if err = (&controller.LedgerReconciler{
		Client:    mgr.GetClient(),
		Scheme:    mgr.GetScheme(),
		Dynamic:   dynamicClient,
		Config:    cfg,
		Clientset: clientset,
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "Ledger")
		os.Exit(1)
	}

	if err = (&controller.BackupReconciler{
		Client:    mgr.GetClient(),
		Scheme:    mgr.GetScheme(),
		Config:    cfg,
		Clientset: clientset,
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "Backup")
		os.Exit(1)
	}

	if err = (&controller.BackupRunReconciler{
		Client:    mgr.GetClient(),
		Scheme:    mgr.GetScheme(),
		Clientset: clientset,
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "BackupRun")
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
