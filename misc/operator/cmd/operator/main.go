package main

import (
	"flag"
	"os"

	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/kubernetes"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	ledgerv1alpha1 "github.com/formancehq/ledger-v3-poc/operator/api/v1alpha1"
	"github.com/formancehq/ledger-v3-poc/operator/internal/controller"
)

type flags struct {
	metricsAddr      string
	probeAddr        string
	leaderElect      bool
	watchNamespace   string
	secretsNamespace string
}

func main() {
	var f flags
	flag.StringVar(&f.metricsAddr, "metrics-bind-address", ":8080", "The address the metric endpoint binds to.")
	flag.StringVar(&f.probeAddr, "health-probe-bind-address", ":8081", "The address the probe endpoint binds to.")
	flag.BoolVar(&f.leaderElect, "leader-elect", false, "Enable leader election for controller manager.")
	flag.StringVar(&f.watchNamespace, "watch-namespace", "", "Namespace to watch. Empty string watches all namespaces.")
	flag.StringVar(&f.secretsNamespace, "secrets-namespace", "", "Namespace for agent key secrets. Defaults to POD_NAMESPACE or 'default'.")

	opts := zap.Options{Development: true}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))
	setupLog := ctrl.Log.WithName("setup")

	scheme := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(ledgerv1alpha1.AddToScheme(scheme))

	secretsNS := f.secretsNamespace
	if secretsNS == "" {
		secretsNS = os.Getenv("POD_NAMESPACE")
	}
	if secretsNS == "" {
		secretsNS = "default"
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
		if secretsNS != f.watchNamespace {
			mgrOpts.Cache.DefaultNamespaces[secretsNS] = cache.Config{}
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

	if err = (&controller.LedgerServiceReconciler{
		Client:    mgr.GetClient(),
		Scheme:    mgr.GetScheme(),
		Config:    cfg,
		Clientset: clientset,
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "LedgerService")
		os.Exit(1)
	}

	if err = (&controller.LedgerClusterAgentReconciler{
		Client:           mgr.GetClient(),
		Scheme:           mgr.GetScheme(),
		SecretsNamespace: secretsNS,
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "LedgerClusterAgent")
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
