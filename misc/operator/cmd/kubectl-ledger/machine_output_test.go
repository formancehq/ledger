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
		name       string
		args       []string
		resource   string
		kind       string
		namespaced bool
		get        bool
	}{
		{name: "list", args: []string{"list"}, resource: "clusters", kind: "Cluster", namespaced: true},
		{name: "get", args: []string{"get", "audit"}, resource: "clusters", kind: "Cluster", namespaced: true, get: true},
		{name: "credentials", args: []string{"credentials", "list"}, resource: "credentials", kind: "Credentials"},
		{name: "backups", args: []string{"backup", "list"}, resource: "backups", kind: "Backup", namespaced: true},
		{name: "runs", args: []string{"backup", "runs", "audit"}, resource: "backupruns", kind: "BackupRun", namespaced: true},
	} {
		for _, format := range []string{"json", "yaml"} {
			for _, fail := range []bool{false, true} {
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
					server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
						writer.Header().Set("Content-Type", "application/json")
						var body string
						switch request.URL.Path {
						case "/api":
							body = `{"apiVersion":"v1","kind":"APIVersions","versions":[]}`
						case "/apis":
							body = `{"apiVersion":"v1","kind":"APIGroupList","groups":[{"name":"ledger.formance.com","versions":[{"groupVersion":"ledger.formance.com/v1alpha1","version":"v1alpha1"}],"preferredVersion":{"groupVersion":"ledger.formance.com/v1alpha1","version":"v1alpha1"}}]}`
						case "/apis/ledger.formance.com/v1alpha1":
							body = fmt.Sprintf(`{"apiVersion":"v1","kind":"APIResourceList","groupVersion":"ledger.formance.com/v1alpha1","resources":[{"name":%q,"namespaced":%t,"kind":%q,"verbs":["get","list"]}]}`, scenario.resource, scenario.namespaced, scenario.kind)
						case operationPath:
							assert.Equal(t, http.MethodGet, request.Method)
							operationCalls.Add(1)
							if fail {
								writer.WriteHeader(http.StatusForbidden)
								body = `{"apiVersion":"v1","kind":"Status","status":"Failure","message":"machine output operation denied","reason":"Forbidden","code":403}`
							} else if scenario.get {
								body = fmt.Sprintf(`{"apiVersion":"ledger.formance.com/v1alpha1","kind":%q,"metadata":{"name":"audit","namespace":"default"}}`, kind)
							} else {
								body = fmt.Sprintf(`{"apiVersion":"ledger.formance.com/v1alpha1","kind":%q,"items":[]}`, kind)
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
					require.EqualValues(t, 1, operationCalls.Load())
					data, readErr := os.ReadFile(outputPath)
					require.NoError(t, readErr)
					require.NotContains(t, string(data), "Fetching")
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
					require.Equal(t, "ledger.formance.com/v1alpha1", decoded["apiVersion"])
					require.Equal(t, kind, decoded["kind"])
					if scenario.get {
						require.Equal(t, "audit", decoded["metadata"].(map[string]any)["name"])
					} else {
						require.Empty(t, decoded["items"])
					}
				})
			}
		}
	}
}
