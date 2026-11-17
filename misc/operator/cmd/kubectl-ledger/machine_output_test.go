package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/pterm/pterm"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	"sigs.k8s.io/yaml"
)

func TestReadCommandMachineOutput(t *testing.T) {
	for _, scenario := range []struct {
		name            string
		args            []string
		resource        string
		kind            string
		namespaced      bool
		get             bool
		resolveResource string
		resolveKind     string
		ambiguous       bool
	}{
		{name: "list", args: []string{"list"}, resource: "clusters", kind: "Cluster", namespaced: true},
		{name: "get", args: []string{"get", "audit"}, resource: "clusters", kind: "Cluster", namespaced: true, get: true},
		{name: "credentials", args: []string{"credentials", "list"}, resource: "credentials", kind: "Credentials"},
		{name: "backups", args: []string{"backup", "list"}, resource: "backups", kind: "Backup", namespaced: true},
		{name: "runs", args: []string{"backup", "runs", "audit"}, resource: "backupruns", kind: "BackupRun", namespaced: true},
		{name: "get resolved", args: []string{"get"}, resource: "clusters", kind: "Cluster", namespaced: true, get: true, resolveResource: "clusters", resolveKind: "Cluster"},
		{name: "runs resolved", args: []string{"backup", "runs"}, resource: "backupruns", kind: "BackupRun", namespaced: true, resolveResource: "backups", resolveKind: "Backup"},
		{name: "get ambiguous", args: []string{"get"}, resource: "clusters", kind: "Cluster", namespaced: true, get: true, resolveResource: "clusters", resolveKind: "Cluster", ambiguous: true},
		{name: "runs ambiguous", args: []string{"backup", "runs"}, resource: "backupruns", kind: "BackupRun", namespaced: true, resolveResource: "backups", resolveKind: "Backup", ambiguous: true},
	} {
		for _, format := range []string{"json", "yaml"} {
			for _, fail := range []bool{false, true} {
				if scenario.ambiguous && fail {
					continue
				}
				t.Run(fmt.Sprintf("%s/%s/failure=%t", scenario.name, format, fail), func(t *testing.T) {
					outputPath := filepath.Join(t.TempDir(), "stdout")
					output, err := os.Create(outputPath)
					require.NoError(t, err)
					stdout, infoWriter, errorWriter := os.Stdout, pterm.Info.Writer, pterm.Error.Writer
					os.Stdout, pterm.Info.Writer, pterm.Error.Writer = output, output, output
					t.Cleanup(func() {
						os.Stdout, pterm.Info.Writer, pterm.Error.Writer = stdout, infoWriter, errorWriter
						assert.NoError(t, output.Close())
					})
					operationPath := "/apis/ledger.formance.com/v1alpha1/"
					if scenario.namespaced {
						operationPath += "namespaces/default/"
					}
					operationPath += scenario.resource
					kind := scenario.kind + "List"
					if scenario.get {
						operationPath += "/audit"
						kind = scenario.kind
					}
					var operationCalls atomic.Int32
					var resolutionCalls atomic.Int32
					resolutionPath := "/apis/ledger.formance.com/v1alpha1/namespaces/default/" + scenario.resolveResource
					server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
						writer.Header().Set("Content-Type", "application/json")
						var body string
						switch request.URL.Path {
						case "/api":
							body = `{"apiVersion":"v1","kind":"APIVersions","versions":[]}`
						case "/apis":
							body = `{"apiVersion":"v1","kind":"APIGroupList","groups":[{"name":"ledger.formance.com","versions":[{"groupVersion":"ledger.formance.com/v1alpha1","version":"v1alpha1"}],"preferredVersion":{"groupVersion":"ledger.formance.com/v1alpha1","version":"v1alpha1"}}]}`
						case "/apis/ledger.formance.com/v1alpha1":
							resources := fmt.Sprintf(`{"name":%q,"namespaced":%t,"kind":%q,"verbs":["get","list"]}`, scenario.resource, scenario.namespaced, scenario.kind)
							if scenario.resolveResource != "" && scenario.resolveResource != scenario.resource {
								resources += fmt.Sprintf(`,{"name":%q,"namespaced":true,"kind":%q,"verbs":["get","list"]}`, scenario.resolveResource, scenario.resolveKind)
							}
							body = fmt.Sprintf(`{"apiVersion":"v1","kind":"APIResourceList","groupVersion":"ledger.formance.com/v1alpha1","resources":[%s]}`, resources)
						case resolutionPath:
							assert.NotEmpty(t, scenario.resolveResource)
							assert.Equal(t, http.MethodGet, request.Method)
							resolutionCalls.Add(1)
							items := fmt.Sprintf(`{"apiVersion":"ledger.formance.com/v1alpha1","kind":%q,"metadata":{"name":"audit","namespace":"default"}}`, scenario.resolveKind)
							if scenario.ambiguous {
								items += fmt.Sprintf(`,{"apiVersion":"ledger.formance.com/v1alpha1","kind":%q,"metadata":{"name":"other","namespace":"default"}}`, scenario.resolveKind)
							}
							body = fmt.Sprintf(`{"apiVersion":"ledger.formance.com/v1alpha1","kind":%q,"items":[%s]}`, scenario.resolveKind+"List", items)
						case operationPath:
							assert.Equal(t, http.MethodGet, request.Method)
							operationCalls.Add(1)
							switch {
							case fail:
								writer.WriteHeader(http.StatusForbidden)
								body = `{"apiVersion":"v1","kind":"Status","status":"Failure","message":"machine output operation denied","reason":"Forbidden","code":403}`
							case scenario.get:
								body = fmt.Sprintf(`{"apiVersion":"ledger.formance.com/v1alpha1","kind":%q,"metadata":{"name":"audit","namespace":"default"}}`, kind)
							default:
								body = fmt.Sprintf(`{"apiVersion":"ledger.formance.com/v1alpha1","kind":%q,"items":[{"apiVersion":"ledger.formance.com/v1alpha1","kind":%q,"metadata":{"name":"audit","namespace":"default"}}]}`, kind, scenario.kind)
							}
						default:
							t.Errorf("unexpected API request: %s %s", request.Method, request.URL.Path)
							writer.WriteHeader(http.StatusNotFound)
						}
						_, err := writer.Write([]byte(body))
						assert.NoError(t, err)
					}))
					t.Cleanup(server.Close)
					config := clientcmdapi.Config{
						Clusters:       map[string]*clientcmdapi.Cluster{"test": {Server: server.URL}},
						Contexts:       map[string]*clientcmdapi.Context{"test": {Cluster: "test", Namespace: "default"}},
						CurrentContext: "test",
					}
					kubeconfig := filepath.Join(t.TempDir(), "kubeconfig")
					require.NoError(t, clientcmd.WriteToFile(config, kubeconfig))
					command := newRootCommand()
					command.SetArgs(append([]string{"--kubeconfig", kubeconfig, "-o", format}, scenario.args...))
					err = command.ExecuteContext(t.Context())
					if scenario.resolveResource != "" {
						require.EqualValues(t, 1, resolutionCalls.Load())
					} else {
						require.Zero(t, resolutionCalls.Load())
					}
					data, readErr := os.ReadFile(outputPath)
					require.NoError(t, readErr)
					require.NotContains(t, string(data), "Fetching")
					require.NotContains(t, string(data), "Using")
					if scenario.ambiguous {
						noun := map[string]string{"Cluster": "ledger", "Backup": "backup"}[scenario.resolveKind]
						require.EqualError(t, err, "multiple "+scenario.resolveKind+" resources found: specify an explicit "+noun+" name with -o "+format)
						require.Zero(t, operationCalls.Load())
						require.Empty(t, string(data))

						return
					}
					require.EqualValues(t, 1, operationCalls.Load())
					if fail {
						require.ErrorContains(t, err, "machine output operation denied")
						require.Contains(t, string(data), "Failed to")

						return
					}
					require.NoError(t, err)
					var decoded map[string]any
					if format == "json" {
						require.NoError(t, json.Unmarshal(data, &decoded))
					} else {
						require.NoError(t, yaml.UnmarshalStrict(data, &decoded))
					}
					require.NotContains(t, decoded, "apiVersion")
					require.NotContains(t, decoded, "kind")
					var resource map[string]any
					if scenario.get {
						resource = decoded
					} else {
						items, ok := decoded["items"].([]any)
						require.True(t, ok)
						require.Len(t, items, 1)
						resource, ok = items[0].(map[string]any)
						require.True(t, ok)
					}
					metadata, ok := resource["metadata"].(map[string]any)
					require.True(t, ok)
					require.Equal(t, "audit", metadata["name"])
					if scenario.namespaced {
						require.Equal(t, "default", metadata["namespace"])
					}
				})
			}
		}
	}
}
