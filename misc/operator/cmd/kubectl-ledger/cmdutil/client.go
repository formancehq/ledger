package cmdutil

import (
	"github.com/spf13/cobra"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/kubernetes"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/client"

	ledgerv1alpha1 "github.com/formance/ledger/operator/api/v1alpha1"
)

// Options holds global flags shared by all subcommands.
type Options struct {
	kubeconfig string
	context    string
	namespace  string
	output     string
}

// AddFlags binds persistent flags to the root command.
func (o *Options) AddFlags(cmd *cobra.Command) {
	cmd.PersistentFlags().StringVar(&o.kubeconfig, "kubeconfig", "", "Path to kubeconfig file")
	cmd.PersistentFlags().StringVar(&o.context, "context", "", "Kubernetes context to use")
	cmd.PersistentFlags().StringVarP(&o.namespace, "namespace", "n", "", "Kubernetes namespace")
	cmd.PersistentFlags().StringVarP(&o.output, "output", "o", "table", "Output format: table, json, yaml")
}

// OutputFormat returns the selected output format.
func (o *Options) OutputFormat() string {
	return o.output
}

func (o *Options) clientConfig() clientcmd.ClientConfig {
	rules := clientcmd.NewDefaultClientConfigLoadingRules()
	if o.kubeconfig != "" {
		rules.ExplicitPath = o.kubeconfig
	}
	overrides := &clientcmd.ConfigOverrides{}
	if o.context != "" {
		overrides.CurrentContext = o.context
	}
	if o.namespace != "" {
		overrides.Context.Namespace = o.namespace
	}

	return clientcmd.NewNonInteractiveDeferredLoadingClientConfig(rules, overrides)
}

// RESTConfig returns a Kubernetes REST config resolved from flags.
func (o *Options) RESTConfig() (*rest.Config, error) {
	return o.clientConfig().ClientConfig()
}

// ResolvedNamespace returns the namespace resolved from flags or kubeconfig.
func (o *Options) ResolvedNamespace() (string, error) {
	ns, _, err := o.clientConfig().Namespace()

	return ns, err
}

// CRDClient creates a controller-runtime client with the LedgerService CRD scheme.
func (o *Options) CRDClient() (client.Client, error) {
	rc, err := o.RESTConfig()
	if err != nil {
		return nil, err
	}

	s := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(s))
	utilruntime.Must(ledgerv1alpha1.AddToScheme(s))

	return client.New(rc, client.Options{Scheme: s})
}

// Clientset creates a standard Kubernetes clientset.
func (o *Options) Clientset() (kubernetes.Interface, error) {
	rc, err := o.RESTConfig()
	if err != nil {
		return nil, err
	}

	return kubernetes.NewForConfig(rc)
}
