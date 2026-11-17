package main

import (
	"bytes"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/pterm/pterm"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
)

func TestCommandProgress(t *testing.T) {
	for _, scenario := range []struct {
		name     string
		args     []string
		method   string
		path     string
		response string
		progress string
		outcome  string
		failure  string
	}{
		{
			name: "list", args: []string{"list"}, method: http.MethodGet,
			path:     "/apis/ledger.formance.com/v1alpha1/namespaces/default/clusters",
			response: `{"apiVersion":"ledger.formance.com/v1alpha1","kind":"ClusterList","items":[]}`,
			progress: "Fetching Cluster resources...", outcome: "No Cluster resources found.", failure: "Failed to list Cluster resources",
		},
		{
			name: "delete", args: []string{"delete", "audit", "--yes"}, method: http.MethodDelete,
			path:     "/apis/ledger.formance.com/v1alpha1/namespaces/default/clusters/audit",
			response: `{"apiVersion":"v1","kind":"Status","status":"Success"}`,
			progress: "Deleting Cluster...", outcome: "deleted", failure: "Failed to delete Cluster",
		},
	} {
		for _, fail := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/failure=%t", scenario.name, fail), func(t *testing.T) {
				var output bytes.Buffer
				infoWriter, errorWriter, successWriter := pterm.Info.Writer, pterm.Error.Writer, pterm.Success.Writer
				pterm.Info.Writer, pterm.Error.Writer, pterm.Success.Writer = &output, &output, &output
				t.Cleanup(func() {
					pterm.Info.Writer, pterm.Error.Writer, pterm.Success.Writer = infoWriter, errorWriter, successWriter
				})
				var operationCalls atomic.Int32
				server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
					writer.Header().Set("Content-Type", "application/json")
					var body string
					switch {
					case request.URL.Path == "/api":
						body = `{"apiVersion":"v1","kind":"APIVersions","versions":[]}`
					case request.URL.Path == "/apis":
						body = `{"apiVersion":"v1","kind":"APIGroupList","groups":[{"name":"ledger.formance.com","versions":[{"groupVersion":"ledger.formance.com/v1alpha1","version":"v1alpha1"}],"preferredVersion":{"groupVersion":"ledger.formance.com/v1alpha1","version":"v1alpha1"}}]}`
					case request.Method == scenario.method && request.URL.Path == scenario.path:
						operationCalls.Add(1)
						body = scenario.response
						if fail {
							writer.WriteHeader(http.StatusForbidden)
							body = `{"apiVersion":"v1","kind":"Status","status":"Failure","message":"progress operation denied","reason":"Forbidden","code":403}`
						}
					case request.URL.Path == "/apis/ledger.formance.com/v1alpha1":
						body = `{"apiVersion":"v1","kind":"APIResourceList","groupVersion":"ledger.formance.com/v1alpha1","resources":[{"name":"clusters","singularName":"cluster","namespaced":true,"kind":"Cluster","verbs":["get","list","delete"]}]}`
					case request.Method == http.MethodGet && strings.HasSuffix(request.URL.Path, "/clusters/audit"):
						body = `{"apiVersion":"ledger.formance.com/v1alpha1","kind":"Cluster","metadata":{"name":"audit","namespace":"default"}}`
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
				command.SetArgs(append([]string{"--kubeconfig", kubeconfig}, scenario.args...))
				err := command.ExecuteContext(t.Context())
				require.EqualValues(t, 1, operationCalls.Load())
				require.Contains(t, output.String(), scenario.progress)
				if fail {
					require.ErrorContains(t, err, "progress operation denied")
					require.Contains(t, output.String(), scenario.failure)
					require.NotContains(t, output.String(), scenario.outcome)
				} else {
					require.NoError(t, err)
					require.Contains(t, output.String(), scenario.outcome)
					require.NotContains(t, output.String(), scenario.failure)
				}
			})
		}
	}
}
