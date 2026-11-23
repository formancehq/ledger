package cmdutil

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
)

func TestResolveResourceNamesNamespaceResolution(t *testing.T) {
	for _, kind := range []string{"Cluster", "Backup", "Credentials"} {
		t.Run(kind, func(t *testing.T) {
			for _, explicit := range []bool{false, true} {
				t.Run(map[bool]string{false: "interactive", true: "explicit"}[explicit], func(t *testing.T) {
					scheme := runtime.NewScheme()
					require.NoError(t, ledgerv1alpha1.AddToScheme(scheme))
					var clientCalls, listCalls, promptCalls int
					crdClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(
						&ledgerv1alpha1.Credentials{ObjectMeta: metav1.ObjectMeta{Name: "first"}},
						&ledgerv1alpha1.Credentials{ObjectMeta: metav1.ObjectMeta{Name: "second"}},
					).WithInterceptorFuncs(interceptor.Funcs{
						List: func(ctx context.Context, underlying client.WithWatch, list client.ObjectList, options ...client.ListOption) error {
							listCalls++
							resolved := &client.ListOptions{}
							resolved.ApplyOptions(options)
							require.Empty(t, resolved.Namespace)
							require.IsType(t, &ledgerv1alpha1.CredentialsList{}, list)

							return underlying.List(ctx, list, options...)
						},
					}).Build()
					opts := &Options{
						kubeconfig: filepath.Join(t.TempDir(), "missing-kubeconfig"),
						newCRDClient: func() (client.Client, error) {
							clientCalls++

							return crdClient, nil
						},
						selectResource: func(prompt string, options []string) (string, error) {
							promptCalls++
							require.Equal(t, "Select a Credentials", prompt)
							require.Equal(t, []string{"first", "second"}, options)

							return "second", nil
						},
					}
					_, namespaceErr := opts.ResolvedNamespace()
					require.Error(t, namespaceErr)
					var args []string
					if explicit {
						args = []string{"provided"}
					}
					var name, namespace string
					var err error
					switch kind {
					case "Cluster":
						name, namespace, err = ResolveClusterName(t.Context(), opts, args)
					case "Backup":
						name, namespace, err = ResolveBackupName(t.Context(), opts, args)
					case "Credentials":
						name, err = ResolveCredentialsName(t.Context(), opts, args)
					}
					if kind == "Credentials" {
						require.NoError(t, err)
						if explicit {
							require.Equal(t, "provided", name)
						} else {
							require.Equal(t, "second", name)
							require.Equal(t, 1, clientCalls)
							require.Equal(t, 1, listCalls)
							require.Equal(t, 1, promptCalls)

							return
						}
					} else {
						require.EqualError(t, err, "resolving namespace: "+namespaceErr.Error())
						require.Empty(t, name)
						require.Empty(t, namespace)
					}
					require.Zero(t, clientCalls)
					require.Zero(t, listCalls)
					require.Zero(t, promptCalls)
				})
			}
		})
	}
}

func TestResolveResourceNames(t *testing.T) {
	t.Parallel()

	for _, kind := range []string{"Cluster", "Backup", "Credentials"} {
		t.Run(kind, func(t *testing.T) {
			for _, scenario := range []string{"explicit", "empty", "single", "multiple", "cancel", "list error", "client error"} {
				t.Run(scenario, func(t *testing.T) {
					scheme := runtime.NewScheme()
					require.NoError(t, ledgerv1alpha1.AddToScheme(scheme))
					failure := errors.New("injected failure")
					var clientCalls, listCalls, promptCalls int
					var names []string
					switch scenario {
					case "single":
						names = []string{"first"}
					case "multiple", "cancel":
						names = []string{"first", "second"}
					}
					objects := make([]client.Object, 0)
					for _, name := range append(names, "foreign") {
						metadata := metav1.ObjectMeta{Name: name, Namespace: "selected"}
						if name == "foreign" {
							metadata.Namespace = "other"
						}
						switch kind {
						case "Cluster":
							objects = append(objects, &ledgerv1alpha1.Cluster{ObjectMeta: metadata})
						case "Backup":
							objects = append(objects, &ledgerv1alpha1.Backup{ObjectMeta: metadata})
						case "Credentials":
							if name != "foreign" {
								metadata.Namespace = ""
								objects = append(objects, &ledgerv1alpha1.Credentials{ObjectMeta: metadata})
							}
						}
					}
					crdClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).WithInterceptorFuncs(interceptor.Funcs{
						List: func(ctx context.Context, underlying client.WithWatch, list client.ObjectList, options ...client.ListOption) error {
							listCalls++
							resolved := &client.ListOptions{}
							resolved.ApplyOptions(options)
							if kind == "Credentials" {
								require.Empty(t, resolved.Namespace)
								require.IsType(t, &ledgerv1alpha1.CredentialsList{}, list)
							} else {
								require.Equal(t, "selected", resolved.Namespace)
								if kind == "Cluster" {
									require.IsType(t, &ledgerv1alpha1.ClusterList{}, list)
								} else {
									require.IsType(t, &ledgerv1alpha1.BackupList{}, list)
								}
							}
							if scenario == "list error" {
								return failure
							}

							return underlying.List(ctx, list, options...)
						},
					}).Build()
					opts := &Options{
						namespace: "selected",
						newCRDClient: func() (client.Client, error) {
							clientCalls++
							if scenario == "client error" {
								return nil, failure
							}

							return crdClient, nil
						},
						selectResource: func(prompt string, options []string) (string, error) {
							promptCalls++
							require.Equal(t, "Select a "+kind, prompt)
							require.Equal(t, names, options)
							if scenario == "cancel" {
								return "", failure
							}

							return "second", nil
						},
					}
					var args []string
					if scenario == "explicit" {
						args = []string{"provided"}
					}
					var name, namespace string
					var err error
					switch kind {
					case "Cluster":
						name, namespace, err = ResolveClusterName(t.Context(), opts, args)
					case "Backup":
						name, namespace, err = ResolveBackupName(t.Context(), opts, args)
					case "Credentials":
						name, err = ResolveCredentialsName(t.Context(), opts, args)
					}
					noun := map[string]string{"Cluster": "ledger", "Backup": "backup", "Credentials": "credentials"}[kind]
					switch scenario {
					case "explicit", "single", "multiple":
						require.NoError(t, err)
						require.Equal(t, map[string]string{"explicit": "provided", "single": "first", "multiple": "second"}[scenario], name)
						if kind != "Credentials" {
							require.Equal(t, "selected", namespace)
						}
					case "empty":
						message := "no " + kind + " resources found"
						if kind != "Credentials" {
							message += " in namespace \"selected\""
						}
						require.EqualError(t, err, message)
					case "client error":
						require.EqualError(t, err, "creating client: injected failure")
					case "list error":
						plural := noun + "s"
						if kind == "Credentials" {
							plural = noun
						}
						require.EqualError(t, err, "listing "+plural+": injected failure")
					case "cancel":
						require.EqualError(t, err, "failed to select "+noun+": injected failure")
					}
					if err != nil {
						require.Empty(t, name)
						require.Empty(t, namespace)
						if scenario != "empty" {
							require.ErrorIs(t, err, failure)
						}
					}
					expectedClientCalls, expectedListCalls, expectedPromptCalls := 1, 1, 0
					switch scenario {
					case "explicit":
						expectedClientCalls, expectedListCalls = 0, 0
					case "client error":
						expectedListCalls = 0
					case "multiple", "cancel":
						expectedPromptCalls = 1
					}
					require.Equal(t, expectedClientCalls, clientCalls)
					require.Equal(t, expectedListCalls, listCalls)
					require.Equal(t, expectedPromptCalls, promptCalls)
				})
			}
		})
	}
}
