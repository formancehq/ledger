/*
Copyright 2022.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"flag"
	"os"

	traefik "github.com/traefik/traefik/v2/pkg/provider/kubernetes/crd/traefik/v1alpha1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"

	authcomponentsv1beta2 "github.com/formancehq/operator/apis/auth.components/v1beta2"
	benthoscomponentsv1beta2 "github.com/formancehq/operator/apis/benthos.components/v1beta2"
	componentsv1beta2 "github.com/formancehq/operator/apis/components/v1beta2"
	benthos_components "github.com/formancehq/operator/controllers/benthos.components"
	components "github.com/formancehq/operator/controllers/components"
	"github.com/formancehq/operator/pkg/controllerutils"

	_ "k8s.io/client-go/plugin/pkg/client/auth"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	stackv1beta2 "github.com/formancehq/operator/apis/stack/v1beta2"
	"github.com/formancehq/operator/controllers/stack"
	//+kubebuilder:scaffold:imports
)

var (
	scheme   = runtime.NewScheme()
	setupLog = ctrl.Log.WithName("setup")
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(componentsv1beta2.AddToScheme(scheme))
	utilruntime.Must(authcomponentsv1beta2.AddToScheme(scheme))
	utilruntime.Must(traefik.AddToScheme(scheme))
	utilruntime.Must(benthoscomponentsv1beta2.AddToScheme(scheme))
	utilruntime.Must(stackv1beta2.AddToScheme(scheme))
	//+kubebuilder:scaffold:scheme
}

func main() {
	var (
		metricsAddr          string
		enableLeaderElection bool
		probeAddr            string
		dnsName              string
		issuerRefName        string
		issuerRefKind        string
	)
	flag.StringVar(&metricsAddr, "metrics-bind-address", ":8080", "The address the metric endpoint binds to.")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "The address the probe endpoint binds to.")
	flag.BoolVar(&enableLeaderElection, "leader-elect", false,
		"Enable leader election for controller manager. "+
			"Enabling this will ensure there is only one active controller manager.")
	flag.StringVar(&dnsName, "dns-name", "", "")
	flag.StringVar(&issuerRefName, "issuer-ref-name", "", "")
	flag.StringVar(&issuerRefKind, "issuer-ref-kind", "ClusterIssuer", "")

	opts := zap.Options{
		Development: true,
	}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:                 scheme,
		MetricsBindAddress:     metricsAddr,
		Port:                   9443,
		HealthProbeBindAddress: probeAddr,
		LeaderElection:         enableLeaderElection,
		LeaderElectionID:       "68fe8eef.formance.com",
		// LeaderElectionReleaseOnCancel defines if the leader should step down voluntarily
		// when the Manager ends. This requires the binary to immediately end when the
		// Manager is stopped, otherwise, this setting is unsafe. Setting this significantly
		// speeds up voluntary leader transitions as the new leader don't have to wait
		// LeaseDuration time first.
		//
		// In the default scaffold provided, the program ends immediately after
		// the manager stops, so would be fine to enable this option. However,
		// if you are doing or is intended to do any operation such as perform cleanups
		// after the manager stops then its usage might be unsafe.
		// LeaderElectionReleaseOnCancel: true,
	})
	if err != nil {
		setupLog.Error(err, "unable to start manager")
		os.Exit(1)
	}

	stackMutator := stack.NewMutator(mgr.GetClient(), mgr.GetScheme(), []string{
		dnsName,
	})
	if err = controllerutils.NewReconciler(mgr.GetClient(), mgr.GetScheme(), stackMutator).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "Stack")
		os.Exit(1)
	}
	authMutator := components.NewMutator(mgr.GetClient(), mgr.GetScheme())
	if err = controllerutils.NewReconciler(mgr.GetClient(), mgr.GetScheme(), authMutator).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "Auth")
		os.Exit(1)
	}
	ledgerMutator := components.NewLedgerMutator(mgr.GetClient(), mgr.GetScheme())
	if err = controllerutils.NewReconciler(mgr.GetClient(), mgr.GetScheme(), ledgerMutator).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "Ledger")
		os.Exit(1)
	}
	paymentsMutator := components.NewPaymentsMutator(mgr.GetClient(), mgr.GetScheme())
	if err = controllerutils.NewReconciler(mgr.GetClient(), mgr.GetScheme(), paymentsMutator).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "Payments")
		os.Exit(1)
	}
	webhooksMutator := components.NewWebhooksMutator(mgr.GetClient(), mgr.GetScheme())
	if err = controllerutils.NewReconciler(mgr.GetClient(), mgr.GetScheme(), webhooksMutator).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "Webhooks")
		os.Exit(1)
	}
	searchMutator := components.NewSearchMutator(mgr.GetClient(), mgr.GetScheme())
	if err = controllerutils.NewReconciler(mgr.GetClient(), mgr.GetScheme(), searchMutator).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "Search")
		os.Exit(1)
	}
	controlMutator := components.NewControlMutator(mgr.GetClient(), mgr.GetScheme())
	if err = controllerutils.NewReconciler(mgr.GetClient(), mgr.GetScheme(), controlMutator).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "Control")
		os.Exit(1)
	}
	walletsMutator := components.NewWalletsMutator(mgr.GetClient(), mgr.GetScheme())
	if err = controllerutils.NewReconciler(mgr.GetClient(), mgr.GetScheme(), walletsMutator).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "Wallets")
		os.Exit(1)
	}
	counterpartiesMutator := components.NewCounterpartiesMutator(mgr.GetClient(), mgr.GetScheme())
	if err = controllerutils.NewReconciler(mgr.GetClient(), mgr.GetScheme(), counterpartiesMutator).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "Counterparties")
		os.Exit(1)
	}
	serverMutator := benthos_components.NewServerMutator(mgr.GetClient(), mgr.GetScheme())
	if err = controllerutils.NewReconciler(mgr.GetClient(), mgr.GetScheme(), serverMutator).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "Server")
		os.Exit(1)
	}
	orchestrationMutator := components.NewOrchestrationMutator(mgr.GetClient(), mgr.GetScheme())
	if err = controllerutils.NewReconciler(mgr.GetClient(), mgr.GetScheme(), orchestrationMutator).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "Orchestration")
		os.Exit(1)
	}

	if err = (&stackv1beta2.Stack{}).SetupWebhookWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create webhook", "webhook", "Stack")
		os.Exit(1)
	}
	if err = (&stackv1beta2.Configuration{}).SetupWebhookWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create webhook", "webhook", "Configuration")
		os.Exit(1)
	}
	//+kubebuilder:scaffold:builder

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up health check")
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up ready check")
		os.Exit(1)
	}

	setupLog.Info("starting manager")
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		setupLog.Error(err, "problem running manager")
		os.Exit(1)
	}
}
