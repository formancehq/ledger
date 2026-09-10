package controller

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
)

// These tests are launched by the root module's TestOperatorIndexOwnership
// E2E harness, which supplies a leased Ledger node and freshly built CLI.
// Kubernetes persistence is intercepted; index creation/deletion is real.
func TestLedgerIndexOwnershipRealStatusInterruption(t *testing.T) {
	t.Parallel()
	for _, retryDesired := range []bool{false, true} {
		t.Run(fmt.Sprintf("retryDesired=%t", retryDesired), func(t *testing.T) {
			t.Parallel()
			testRealIndexStatusInterruption(t, retryDesired)
		})
	}
}

func testRealIndexStatusInterruption(t *testing.T, retryDesired bool) {
	t.Helper()
	h := newRealIndexHarness(t)
	writes := 0
	interruption := errors.New("injected one-shot status persistence interruption")
	c := h.client(interceptor.Funcs{
		SubResourceUpdate: func(ctx context.Context, c client.Client, resource string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
			if resource == "status" {
				writes++
				if writes == 1 {
					return interruption
				}
			}

			return c.SubResource(resource).Update(ctx, obj, opts...)
		},
	})
	_, err := h.controller(c).Reconcile(t.Context(), h.request())
	require.ErrorIs(t, err, interruption)
	require.True(t, h.indexExists(), "create committed before the status interruption")
	stored := h.read(c)
	require.Empty(t, stored.Status.AppliedIndexes, "failed status write must not leak the in-memory owned set")
	fresh := h.controller(c)
	if retryDesired {
		_, err = fresh.Reconcile(t.Context(), h.request())
		require.NoError(t, err)
		stored = h.read(c)
		require.Equal(t, []string{"reference"}, stored.Status.AppliedIndexes, "restart recovers ownership while the desired index remains declared")
		require.True(t, h.indexExists())
	}

	// Withdraw the declaration either before the fresh controller first runs
	// or after its initial reconciliation recovered ownership.
	stored.Spec.Indexes = &ledgerv1alpha1.LedgerIndexesSpec{}
	require.NoError(t, c.Update(t.Context(), stored))
	_, err = fresh.Reconcile(t.Context(), h.request())
	require.NoError(t, err)
	expectedWrites := 2
	if retryDesired {
		expectedWrites++
	}
	require.Equal(t, expectedWrites, writes, "the persistence failure must occur only once")
	require.Empty(t, h.read(c).Status.AppliedIndexes)
	require.False(t, h.indexExists(), "restart must recover durable ownership and drop the withdrawn managed index")
}

func TestLedgerIndexOwnershipRealExternalNotAdopted(t *testing.T) {
	t.Parallel()
	for _, raceCreate := range []bool{false, true} {
		t.Run(fmt.Sprintf("createdAfterList=%t", raceCreate), func(t *testing.T) {
			t.Parallel()
			h := newRealIndexHarness(t)
			createExternal := func() {
				_, err := h.run("indexes", "create", "--ledger", h.ledger.Spec.Name, "--type", "reference")
				require.NoError(t, err)
			}
			if raceCreate {
				h.afterExec = func(script string) {
					if strings.Contains(script, "'indexes' 'list'") {
						h.afterExec = nil
						createExternal()
					}
				}
			} else {
				createExternal()
			}
			c := h.client(interceptor.Funcs{})
			_, err := h.controller(c).Reconcile(t.Context(), h.request())
			require.NoError(t, err)
			stored := h.read(c)
			require.Empty(t, stored.Status.AppliedIndexes, "an external index cannot become operator-owned even when created between list and create")
			if raceCreate {
				condition := meta.FindStatusCondition(stored.Status.Conditions, conditionIndexesSynced)
				require.NotNil(t, condition)
				require.Equal(t, "Error", condition.Reason, "the create conflict must be surfaced")
			}
			stored.Spec.Indexes = &ledgerv1alpha1.LedgerIndexesSpec{}
			require.NoError(t, c.Update(t.Context(), stored))
			_, err = h.controller(c).Reconcile(t.Context(), h.request())
			require.NoError(t, err)
			require.True(t, h.indexExists(), "an external index must survive explicit removal from spec")
			require.Empty(t, h.read(c).Status.AppliedIndexes)
		})
	}
}

func TestLedgerIndexOwnershipRealExternalReplacementNotDropped(t *testing.T) {
	t.Parallel()
	h := newRealIndexHarness(t)
	c := h.client(interceptor.Funcs{})
	_, err := h.controller(c).Reconcile(t.Context(), h.request())
	require.NoError(t, err)
	stored := h.read(c)
	require.Equal(t, []string{"reference"}, stored.Status.AppliedIndexes)
	stored.Spec.Indexes = &ledgerv1alpha1.LedgerIndexesSpec{}
	require.NoError(t, c.Update(t.Context(), stored))
	replacements := 0
	h.afterExec = func(script string) {
		if !strings.Contains(script, "'indexes' 'list'") {
			return
		}
		h.afterExec = nil
		replacements++
		_, err := h.run("indexes", "drop", "--ledger", h.ledger.Spec.Name, "--type", "reference")
		require.NoError(t, err)
		_, err = h.run("indexes", "create", "--ledger", h.ledger.Spec.Name, "--type", "reference")
		require.NoError(t, err)
	}
	_, err = h.controller(c).Reconcile(t.Context(), h.request())
	require.NoError(t, err)
	require.Equal(t, 1, replacements)
	// Replacement happens after the full registry list, before audit attribution.
	// Unlike replacement after attribution, this is detected and not dropped.
	require.Empty(t, h.read(c).Status.AppliedIndexes)
	require.True(t, h.indexExists())
}

func TestLedgerIndexOwnershipRealLostCreateResponse(t *testing.T) {
	t.Parallel()
	h := newRealIndexHarness(t)
	c := h.client(interceptor.Funcs{})
	r := h.controller(c)
	lostResponses := 0
	r.exec = func(ctx context.Context, cfg *rest.Config, clientset kubernetes.Interface, namespace, pod, container string, command []string) (*execResult, error) {
		result, err := h.podExec(ctx, cfg, clientset, namespace, pod, container, command)
		if err == nil && strings.Contains(command[2], "'indexes' 'create'") && lostResponses == 0 {
			lostResponses++

			return result, errors.New("injected lost create response after commit")
		}

		return result, err
	}
	_, err := r.Reconcile(t.Context(), h.request())
	require.NoError(t, err)
	require.Equal(t, 1, lostResponses)
	require.True(t, h.indexExists())
	stored := h.read(c)
	require.Empty(t, stored.Status.AppliedIndexes)
	condition := meta.FindStatusCondition(stored.Status.Conditions, conditionIndexesSynced)
	require.NotNil(t, condition)
	require.Contains(t, condition.Message, "injected lost create response after commit")
	stored.Spec.Indexes = &ledgerv1alpha1.LedgerIndexesSpec{}
	require.NoError(t, c.Update(t.Context(), stored))
	_, err = h.controller(c).Reconcile(t.Context(), h.request())
	require.NoError(t, err)
	require.False(t, h.indexExists(), "a lost create response must not lose the committed owner")
}

func TestLedgerIndexOwnershipRealRecreatedCRDoesNotAdopt(t *testing.T) {
	t.Parallel()
	h := newRealIndexHarness(t)
	c := h.client(interceptor.Funcs{})
	_, err := h.controller(c).Reconcile(t.Context(), h.request())
	require.NoError(t, err)
	require.Equal(t, []string{"reference"}, h.read(c).Status.AppliedIndexes)
	// Recreate the Kubernetes resource with the same namespace/name and spec,
	// but a new UID and empty status; the service-side index is still present.
	h.ledger = h.ledger.DeepCopy()
	h.ledger.UID = types.UID(uuid.NewString())
	c = h.client(interceptor.Funcs{})
	_, err = h.controller(c).Reconcile(t.Context(), h.request())
	require.NoError(t, err)
	stored := h.read(c)
	require.Empty(t, stored.Status.AppliedIndexes)
	stored.Spec.Indexes = &ledgerv1alpha1.LedgerIndexesSpec{}
	require.NoError(t, c.Update(t.Context(), stored))
	_, err = h.controller(c).Reconcile(t.Context(), h.request())
	require.NoError(t, err)
	require.True(t, h.indexExists(), "ownership belongs to the old UID, not the reused CR name")
	require.Empty(t, h.read(c).Status.AppliedIndexes)
}

type realIndexHarness struct {
	t             *testing.T
	cli, endpoint string
	ledger        *ledgerv1alpha1.Ledger
	scheme        *runtime.Scheme
	afterExec     func(string)
}

func newRealIndexHarness(t *testing.T) *realIndexHarness {
	t.Helper()
	endpoint, cli := os.Getenv("LEDGER_INDEX_TEST_SERVER"), os.Getenv("LEDGERCTL_BINARY")
	if endpoint == "" || cli == "" {
		t.Skip("run nix develop --command go test -tags=e2e ./tests/e2e/cluster -run TestOperatorIndexOwnership from the repository root")
	}
	scheme := runtime.NewScheme()
	require.NoError(t, ledgerv1alpha1.AddToScheme(scheme))
	require.NoError(t, appsv1.AddToScheme(scheme))
	name := "ownership-" + uuid.NewString()
	h := &realIndexHarness{t: t, endpoint: endpoint, cli: cli, scheme: scheme, ledger: &ledgerv1alpha1.Ledger{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "test", UID: types.UID(uuid.NewString()), Finalizers: []string{ledgerFinalizer}},
		Spec:       ledgerv1alpha1.LedgerCRDSpec{ClusterRef: "test-cluster", Name: name, Indexes: &ledgerv1alpha1.LedgerIndexesSpec{Transaction: []string{"reference"}}},
		Status:     ledgerv1alpha1.LedgerCRDStatus{Phase: ledgerv1alpha1.LedgerPhaseReady},
	}}
	_, err := h.run("ledgers", "create", "--name", name)
	require.NoError(t, err)

	return h
}

func (h *realIndexHarness) client(funcs interceptor.Funcs) client.Client {
	return fake.NewClientBuilder().WithScheme(h.scheme).WithObjects(h.ledger).WithStatusSubresource(h.ledger).WithInterceptorFuncs(funcs).Build()
}

func (h *realIndexHarness) controller(c client.Client) *LedgerReconciler {
	cluster := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "ledger.formance.com/v1alpha1", "kind": "Cluster",
		"metadata": map[string]any{"name": "test-cluster", "namespace": "test"},
		"status":   map[string]any{"phase": "Running"},
	}}

	return &LedgerReconciler{Client: c, Scheme: h.scheme, Dynamic: dynamicfake.NewSimpleDynamicClient(h.scheme, cluster), exec: h.podExec}
}

func (h *realIndexHarness) request() ctrl.Request {
	return ctrl.Request{NamespacedName: client.ObjectKeyFromObject(h.ledger)}
}

func (h *realIndexHarness) read(c client.Client) *ledgerv1alpha1.Ledger {
	h.t.Helper()
	ledger := &ledgerv1alpha1.Ledger{}
	require.NoError(h.t, c.Get(h.t.Context(), h.request().NamespacedName, ledger))

	return ledger
}

func (h *realIndexHarness) run(args ...string) (string, error) {
	cmd := exec.CommandContext(h.t.Context(), h.cli, append(args, "--server", h.endpoint, "--insecure")...)
	cmd.Env = append(os.Environ(), "OTEL_SDK_DISABLED=true")
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Run(); err != nil {
		return stdout.String(), fmt.Errorf("ledgerctl: %w (stdout: %s) (stderr: %s)", err, stdout.String(), stderr.String())
	}

	return stdout.String(), nil
}

func (h *realIndexHarness) indexExists() bool {
	h.t.Helper()
	out, err := h.run("indexes", "list", "--ledger", h.ledger.Spec.Name, "--json")
	require.NoError(h.t, err)
	actual, err := parseActualIndexes(out)
	require.NoError(h.t, err)
	_, present := actual["reference"]

	return present
}

func (h *realIndexHarness) podExec(ctx context.Context, _ *rest.Config, _ kubernetes.Interface, _, _, _ string, command []string) (*execResult, error) {
	// Execute the production shell command with its exact subcommand/flags,
	// substituting only the transport address and binary outside Kubernetes.
	require.Len(h.t, command, 3)
	end := strings.LastIndex(command[2], " --server ")
	require.Positive(h.t, end)
	script := strings.Replace(command[2][:end], "./ledgerctl ", shellSingleQuote(h.cli)+" ", 1) + " --server " + shellSingleQuote(h.endpoint) + " --insecure"
	cmd := exec.CommandContext(ctx, "/bin/sh", "-c", script)
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	err := cmd.Run()
	result := &execResult{Stdout: stdout.String(), Stderr: stderr.String()}
	if err != nil {
		return result, fmt.Errorf("ledgerctl: %w (stdout: %s) (stderr: %s)", err, result.Stdout, result.Stderr)
	}
	if h.afterExec != nil {
		h.afterExec(script)
	}

	return result, nil
}
