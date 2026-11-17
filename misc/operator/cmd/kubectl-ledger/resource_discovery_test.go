package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
)

func TestLogsAllDiscoversOwnedLivePods(t *testing.T) {
	var clusterReads, podLists, ownedLogs, foreignLogs atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		assert.Equal(t, http.MethodGet, request.Method)
		writer.Header().Set("Content-Type", "application/json")
		var body string
		switch request.URL.Path {
		case "/api":
			body = `{"apiVersion":"v1","kind":"APIVersions","versions":[]}`
		case "/apis":
			body = `{"apiVersion":"v1","kind":"APIGroupList","groups":[{"name":"ledger.formance.com","versions":[{"groupVersion":"ledger.formance.com/v1alpha1","version":"v1alpha1"}],"preferredVersion":{"groupVersion":"ledger.formance.com/v1alpha1","version":"v1alpha1"}}]}`
		case "/apis/ledger.formance.com/v1alpha1":
			body = `{"apiVersion":"v1","kind":"APIResourceList","groupVersion":"ledger.formance.com/v1alpha1","resources":[{"name":"clusters","singularName":"cluster","namespaced":true,"kind":"Cluster","verbs":["get","list"]}]}`
		case "/apis/ledger.formance.com/v1alpha1/namespaces/ledger/clusters/audit":
			clusterReads.Add(1)
			body = `{"apiVersion":"ledger.formance.com/v1alpha1","kind":"Cluster","metadata":{"name":"audit","namespace":"ledger","uid":"cluster-uid"},"spec":{"additionalLabels":{"app.kubernetes.io/name":"rejected-desired-name"}}}`
		case "/apis/apps/v1/namespaces/ledger/statefulsets/ledger-audit":
			body = `{"apiVersion":"apps/v1","kind":"StatefulSet","metadata":{"name":"ledger-audit","namespace":"ledger","uid":"sts-uid","ownerReferences":[{"apiVersion":"ledger.formance.com/v1alpha1","kind":"Cluster","name":"audit","uid":"cluster-uid","controller":true}]},"spec":{"selector":{"matchLabels":{"app.kubernetes.io/name":"custom","app.kubernetes.io/instance":"custom-instance","extra":"value"},"matchExpressions":[{"key":"zone","operator":"In","values":["west"]}]}}}`
		case "/api/v1/namespaces/ledger/pods":
			podLists.Add(1)
			assert.Equal(t, "app.kubernetes.io/instance=custom-instance,app.kubernetes.io/name=custom,extra=value,zone in (west)", request.URL.Query().Get("labelSelector"))
			body = `{"apiVersion":"v1","kind":"PodList","items":[{"metadata":{"name":"ledger-audit-0","namespace":"ledger","ownerReferences":[{"apiVersion":"apps/v1","kind":"StatefulSet","name":"ledger-audit","uid":"sts-uid","controller":true}]}},{"metadata":{"name":"foreign-0","namespace":"ledger","ownerReferences":[{"apiVersion":"apps/v1","kind":"StatefulSet","name":"foreign","uid":"foreign-uid","controller":true}]}}]}`
		case "/api/v1/namespaces/ledger/pods/ledger-audit-0/log":
			ownedLogs.Add(1)
			writer.Header().Set("Content-Type", "text/plain")
			body = "owned pod logs\n"
		case "/api/v1/namespaces/ledger/pods/foreign-0/log":
			foreignLogs.Add(1)
			body = "foreign pod logs\n"
		default:
			t.Errorf("unexpected API request: %s %s", request.Method, request.URL.Path)
			writer.WriteHeader(http.StatusNotFound)
		}
		_, err := io.WriteString(writer, body)
		assert.NoError(t, err)
	}))
	t.Cleanup(server.Close)
	config := clientcmdapi.Config{
		Clusters:       map[string]*clientcmdapi.Cluster{"test": {Server: server.URL}},
		Contexts:       map[string]*clientcmdapi.Context{"test": {Cluster: "test", Namespace: "ledger"}},
		CurrentContext: "test",
	}
	kubeconfig := filepath.Join(t.TempDir(), "kubeconfig")
	require.NoError(t, clientcmd.WriteToFile(config, kubeconfig))
	command := newRootCommand()
	command.SetArgs([]string{"--kubeconfig", kubeconfig, "logs", "audit", "--all"})
	require.NoError(t, command.ExecuteContext(t.Context()))
	require.EqualValues(t, 1, clusterReads.Load())
	require.EqualValues(t, 1, podLists.Load())
	require.EqualValues(t, 1, ownedLogs.Load())
	require.Zero(t, foreignLogs.Load())
}
