package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/httpstream"
	"k8s.io/apimachinery/pkg/util/httpstream/spdy"
	"k8s.io/apimachinery/pkg/util/remotecommand"
	"k8s.io/client-go/kubernetes"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestLedgerctlExecOutputWrappers(t *testing.T) {
	t.Parallel()

	for _, caller := range []string{"ledger", "cluster"} {
		for _, tlsMode := range []string{tlsModeDisabled, tlsModeOptional, tlsModeRequired} {
			for _, execFails := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/%s/failure=%t", caller, tlsMode, execFails), func(t *testing.T) {
					t.Parallel()
					cluster, sts := scaleDownOwnershipFixture()
					sts.Spec.Template.Spec.Containers = []corev1.Container{{
						Name: ledgerContainer, Env: []corev1.EnvVar{{Name: "TLS_MODE", Value: tlsMode}},
					}}
					pod := ownedPod(sts, 0)
					requests := make(chan url.Values, 1)
					serverErrors := make(chan error, 1)
					server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
						if request.Method == http.MethodGet && request.URL.Path == "/api/v1/namespaces/ns/pods/"+pod.Name {
							writer.Header().Set("Content-Type", "application/json")
							if err := json.NewEncoder(writer).Encode(pod); err != nil {
								t.Errorf("encoding pod: %v", err)
							}

							return
						}
						if request.Method != http.MethodPost || request.URL.Path != "/api/v1/namespaces/ns/pods/"+pod.Name+"/exec" {
							http.Error(writer, "unexpected request", http.StatusNotFound)

							return
						}
						requests <- request.URL.Query()
						serverErrors <- serveLedgerctlExecStreams(writer, request, execFails)
					}))
					t.Cleanup(server.Close)
					config := &rest.Config{Host: server.URL}
					clientset, err := kubernetes.NewForConfig(config)
					require.NoError(t, err)
					current := fake.NewClientBuilder().WithScheme(ownershipTestScheme(t)).WithObjects(sts).Build()
					ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
					defer cancel()
					var output string
					if caller == "ledger" {
						reconciler := &LedgerReconciler{Client: current, Config: config, Clientset: clientset}
						output, err = reconciler.ledgerctlExecOutput(ctx, cluster, 9999, "ledgers", "list", "--json")
					} else {
						reconciler := &ClusterReconciler{Client: current, Config: config, Clientset: clientset}
						output, err = reconciler.ledgerctlExecOutput(ctx, cluster, 9999, "ledgers", "list", "--json")
					}
					if execFails {
						require.ErrorContains(t, err, "ledgerctl ledgers: exec failed:")
						require.ErrorContains(t, err, "exit code 7")
						require.ErrorContains(t, err, "command stderr")
						require.Empty(t, output)
					} else {
						require.NoError(t, err)
						require.Equal(t, "command stdout", output)
					}
					select {
					case err := <-serverErrors:
						require.NoError(t, err)
					case <-ctx.Done():
						t.Fatal(ctx.Err())
					}
					query := <-requests
					require.Equal(t, ledgerContainer, query.Get("container"))
					require.Equal(t, "true", query.Get("stdout"))
					require.Equal(t, "true", query.Get("stderr"))
					require.Equal(t, ledgerctlCommand(podSelfServerAddr(headlessServiceName(cluster.Name), 9999), tlsMode, "ledgers", "list", "--json"), query["command"])
				})
			}
		}
	}
}

func serveLedgerctlExecStreams(writer http.ResponseWriter, request *http.Request, execFails bool) (retErr error) {
	if _, err := httpstream.Handshake(request, writer, []string{remotecommand.StreamProtocolV4Name}); err != nil {
		return err
	}
	type pendingStream struct {
		stream    httpstream.Stream
		replySent <-chan struct{}
	}
	streams := make(chan pendingStream, 3)
	connection := spdy.NewResponseUpgrader().UpgradeResponse(writer, request, func(stream httpstream.Stream, replySent <-chan struct{}) error {
		streams <- pendingStream{stream: stream, replySent: replySent}

		return nil
	})
	if connection == nil {
		return errors.New("SPDY upgrade failed")
	}
	defer func() {
		retErr = errors.Join(retErr, connection.Close())
	}()
	byType := make(map[string]httpstream.Stream, 3)
	for range 3 {
		select {
		case pending := <-streams:
			select {
			case <-pending.replySent:
				byType[pending.stream.Headers().Get(corev1.StreamType)] = pending.stream
			case <-request.Context().Done():
				return request.Context().Err()
			}
		case <-request.Context().Done():
			return request.Context().Err()
		}
	}
	for _, kind := range []string{corev1.StreamTypeStdout, corev1.StreamTypeStderr, corev1.StreamTypeError} {
		if byType[kind] == nil {
			return fmt.Errorf("missing %s stream", kind)
		}
	}
	if _, err := io.WriteString(byType[corev1.StreamTypeStdout], "command stdout"); err != nil {
		return err
	}
	if err := byType[corev1.StreamTypeStdout].Close(); err != nil {
		return err
	}
	if _, err := io.WriteString(byType[corev1.StreamTypeStderr], "command stderr"); err != nil {
		return err
	}
	if err := byType[corev1.StreamTypeStderr].Close(); err != nil {
		return err
	}
	status := metav1.Status{Status: metav1.StatusSuccess}
	if execFails {
		status = metav1.Status{
			Status: metav1.StatusFailure, Reason: remotecommand.NonZeroExitCodeReason,
			Details: &metav1.StatusDetails{Causes: []metav1.StatusCause{{Type: remotecommand.ExitCodeCauseType, Message: "7"}}},
		}
	}

	if err := json.NewEncoder(byType[corev1.StreamTypeError]).Encode(status); err != nil {
		return err
	}
	connection.RemoveStreams(byType[corev1.StreamTypeError])

	return nil
}

func TestLedgerctlExecOutputOwnershipWrappers(t *testing.T) {
	t.Parallel()
	for _, caller := range []string{"ledger", "cluster"} {
		for _, foreign := range []string{"StatefulSet", "Pod"} {
			t.Run(caller+"/"+foreign, func(t *testing.T) {
				t.Parallel()
				cluster, sts := scaleDownOwnershipFixture()
				pod := ownedPod(sts, 0)
				wantError := errResourceNameConflict
				if foreign == "StatefulSet" {
					sts.OwnerReferences = nil
				} else {
					pod.OwnerReferences = nil
					wantError = errPodNotOwned
				}
				current := fake.NewClientBuilder().WithScheme(ownershipTestScheme(t)).WithObjects(sts).Build()
				clientset := k8sfake.NewClientset(pod)
				var output string
				var err error
				if caller == "ledger" {
					reconciler := &LedgerReconciler{Client: current, Clientset: clientset,
						exec: func(context.Context, *rest.Config, kubernetes.Interface, string, string, string, []string) (*execResult, error) {
							t.Fatal("ownership refusal must not execute commands")

							return nil, nil
						},
					}
					output, err = reconciler.ledgerctlExecOutput(t.Context(), cluster, 8888, "status")
				} else {
					reconciler := &ClusterReconciler{Client: current, Clientset: clientset}
					output, err = reconciler.ledgerctlExecOutput(t.Context(), cluster, 8888, "status")
				}
				require.ErrorIs(t, err, wantError)
				require.Empty(t, output)
			})
		}
	}
}

func TestLedgerctlExecOutputInjectedExecutor(t *testing.T) {
	t.Parallel()
	for _, execFails := range []bool{false, true} {
		t.Run(fmt.Sprintf("failure=%t", execFails), func(t *testing.T) {
			t.Parallel()
			cluster, sts := scaleDownOwnershipFixture()
			sts.Spec.Template.Spec.Containers = []corev1.Container{{
				Name: ledgerContainer, Env: []corev1.EnvVar{{Name: "TLS_MODE", Value: tlsModeRequired}},
			}}
			current := fake.NewClientBuilder().WithScheme(ownershipTestScheme(t)).WithObjects(sts).Build()
			clientset := k8sfake.NewClientset(ownedPod(sts, 0))
			config := &rest.Config{Host: "https://unused.test"}
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			injectedErr := errors.New("injected exec failed")
			calls := 0
			reconciler := &LedgerReconciler{Client: current, Config: config, Clientset: clientset,
				exec: func(actualCtx context.Context, actualConfig *rest.Config, actualClientset kubernetes.Interface, namespace, pod, container string, command []string) (*execResult, error) {
					calls++
					require.Same(t, ctx, actualCtx)
					require.Same(t, config, actualConfig)
					require.Same(t, clientset, actualClientset)
					require.Equal(t, cluster.Namespace, namespace)
					require.Equal(t, podName(cluster.Name, 0), pod)
					require.Equal(t, ledgerContainer, container)
					require.Equal(t, ledgerctlCommand(podSelfServerAddr(headlessServiceName(cluster.Name), 9999), tlsModeRequired, "ledgers", "list"), command)
					if execFails {
						return &execResult{Stdout: "partial output"}, injectedErr
					}

					return &execResult{Stdout: "injected stdout", Stderr: "injected stderr"}, nil
				},
			}
			output, err := reconciler.ledgerctlExecOutput(ctx, cluster, 9999, "ledgers", "list")
			require.Equal(t, 1, calls)
			if execFails {
				require.ErrorIs(t, err, injectedErr)
				require.ErrorContains(t, err, "ledgerctl ledgers")
				require.Empty(t, output)
			} else {
				require.NoError(t, err)
				require.Equal(t, "injected stdout", output)
			}
		})
	}
}
