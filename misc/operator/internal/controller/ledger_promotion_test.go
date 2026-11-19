package controller

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
)

// This fixture preserves endpoint and StatefulSet/Pod ownership checks while
// injecting only Kubernetes persistence and the external pod-exec transport.
func newPromotionHarness(t *testing.T) *realIndexHarness {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, ledgerv1alpha1.AddToScheme(scheme))
	require.NoError(t, appsv1.AddToScheme(scheme))
	ledger := newLedger("promotion", "test", "test-cluster", "promotion")
	ledger.Finalizers = []string{ledgerFinalizer}
	ledger.Generation = 2
	ledger.Spec.Mode = "normal"
	ledger.Status.Phase = ledgerv1alpha1.LedgerPhaseReady
	ledger.Status.Mode = "mirror"

	return &realIndexHarness{t: t, scheme: scheme, ledger: ledger}
}

func TestLedgerPromotionObservationFailuresRecover(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name, output string
		err          error
	}{
		{name: "transport unavailable", err: errors.New("injected one-shot observation failure")},
		{name: "malformed", output: `{"name":"promotion",`},
		{name: "empty", output: ""},
		{name: "null", output: "null"},
		{name: "missing mode", output: `{"name":"promotion"}`},
		{name: "unknown mode", output: `{"name":"promotion","mode":"UNKNOWN"}`},
		{name: "missing identity", output: `{"mode":"NORMAL"}`},
		{name: "wrong identity", output: `{"name":"other","mode":"NORMAL"}`},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newPromotionHarness(t)
			c := h.client(interceptor.Funcs{})
			reads, promotions := 0, 0
			exec := func(_ context.Context, _ *rest.Config, _ kubernetes.Interface, _, _, _ string, command []string) (*execResult, error) {
				if strings.Contains(command[2], "'ledgers' 'get'") {
					reads++
					if reads == 1 {
						return &execResult{Stdout: tt.output}, tt.err
					}

					return &execResult{Stdout: `{"name":"promotion","mode":"MIRROR"}`}, nil
				}
				require.Contains(t, command[2], "'ledgers' 'promote'")
				promotions++

				return &execResult{}, nil
			}
			r := h.controller(c)
			r.exec = exec
			result, err := r.Reconcile(t.Context(), h.request())
			require.NoError(t, err)
			require.Positive(t, result.RequeueAfter)
			require.Equal(t, 0, promotions, "ambiguous observation cannot authorize mutation")
			stored := h.read(c)
			require.Equal(t, "mirror", stored.Status.Mode)
			require.Empty(t, stored.Status.AppliedSpecHash)
			condition := meta.FindStatusCondition(stored.Status.Conditions, conditionLedgerSynced)
			require.NotNil(t, condition)
			require.Equal(t, "False", string(condition.Status))
			fresh := h.controller(c)
			fresh.exec = exec
			result, err = fresh.Reconcile(t.Context(), h.request())
			require.NoError(t, err)
			require.True(t, result.Requeue)
			require.Equal(t, 2, reads, "the failed observation must recover on the next pass")
			require.Equal(t, 1, promotions)
			stored = h.read(c)
			require.Equal(t, "normal", stored.Status.Mode)
			require.NotEmpty(t, stored.Status.AppliedSpecHash)
			require.Empty(t, stored.Status.Message)
		})
	}
}

// Pod environment can select a lagging replica or a historical checkpoint.
// Model observations that report MIRROR until each read override is pinned.
func TestLedgerPromotionObservationOverridesReadEnvironment(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name                         string
		staleConsistency, checkpoint bool
	}{
		{name: "stale consistency", staleConsistency: true},
		{name: "historical checkpoint", checkpoint: true},
		{name: "both overrides", staleConsistency: true, checkpoint: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newPromotionHarness(t)
			c := h.client(interceptor.Funcs{})
			reads, promotions := 0, 0
			r := h.controller(c)
			r.exec = func(_ context.Context, _ *rest.Config, _ kubernetes.Interface, _, _, _ string, command []string) (*execResult, error) {
				if strings.Contains(command[2], "'ledgers' 'get'") {
					reads++
					stale := tt.staleConsistency && !strings.Contains(command[2], "'--consistency' 'linearizable'")
					historical := tt.checkpoint && !strings.Contains(command[2], "'--checkpoint-id' '0'")
					mode := "NORMAL"
					if stale || historical {
						mode = "MIRROR"
					}

					return &execResult{Stdout: `{"name":"promotion","mode":"` + mode + `"}`}, nil
				}
				require.Contains(t, command[2], "'ledgers' 'promote'")
				promotions++

				return nil, errors.New("ledger is already normal")
			}
			result, err := r.Reconcile(t.Context(), h.request())
			require.NoError(t, err)
			require.Equal(t, 1, reads)
			require.Zero(t, promotions, "recovery must observe committed mode without retrying promotion")
			require.True(t, result.Requeue)
			stored := h.read(c)
			require.Equal(t, "normal", stored.Status.Mode)
			require.Equal(t, computeLedgerSpecHash(&stored.Spec), stored.Status.AppliedSpecHash)
			require.Empty(t, stored.Status.Message)
		})
	}
}

func TestLedgerPromotionFailureRetries(t *testing.T) {
	t.Parallel()
	h := newPromotionHarness(t)
	c := h.client(interceptor.Funcs{})
	reads, promotions := 0, 0
	failure := errors.New("injected promotion rejection before commit")
	exec := func(_ context.Context, _ *rest.Config, _ kubernetes.Interface, _, _, _ string, command []string) (*execResult, error) {
		if strings.Contains(command[2], "'ledgers' 'get'") {
			reads++

			return &execResult{Stdout: `{"name":"promotion","mode":"MIRROR"}`}, nil
		}
		require.Contains(t, command[2], "'ledgers' 'promote'")
		promotions++
		if promotions == 1 {
			return nil, failure
		}

		return &execResult{}, nil
	}
	r := h.controller(c)
	r.exec = exec
	result, err := r.Reconcile(t.Context(), h.request())
	require.NoError(t, err)
	require.Positive(t, result.RequeueAfter)
	require.Equal(t, "mirror", h.read(c).Status.Mode)
	condition := meta.FindStatusCondition(h.read(c).Status.Conditions, conditionLedgerSynced)
	require.NotNil(t, condition)
	require.Equal(t, "PromotionFailed", condition.Reason)
	require.Contains(t, condition.Message, failure.Error())
	fresh := h.controller(c)
	fresh.exec = exec
	result, err = fresh.Reconcile(t.Context(), h.request())
	require.NoError(t, err)
	require.True(t, result.Requeue)
	require.Equal(t, 2, promotions)
	require.Equal(t, 2, reads)
	require.Equal(t, "normal", h.read(c).Status.Mode)
	condition = meta.FindStatusCondition(h.read(c).Status.Conditions, conditionLedgerSynced)
	require.NotNil(t, condition)
	require.Equal(t, "True", string(condition.Status))
}

func newRealPromotionHarness(t *testing.T) *realIndexHarness {
	t.Helper()
	h := newRealIndexHarness(t)
	sourceURL := os.Getenv("LEDGER_PROMOTION_TEST_SOURCE")
	require.NotEmpty(t, sourceURL)
	h.ledger.Name += "-mirror"
	h.ledger.Spec.Name += "-mirror"
	_, err := h.run("ledgers", "create", "--mode", "mirror", "--mirror-base-url", sourceURL, h.ledger.Spec.Name)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		_, err := h.run("transactions", "get", "0", "--ledger", h.ledger.Spec.Name)

		return err == nil
	}, 20*time.Second, 100*time.Millisecond, "mirror must ingest the source log before promotion")
	h.ledger.Spec.Mode = "mirror"
	h.ledger.Spec.MirrorSource = &ledgerv1alpha1.MirrorSourceSpec{
		HTTP: &ledgerv1alpha1.HTTPMirrorSource{BaseURL: sourceURL},
	}
	// Persist the creation hash so index convergence also proves that recovery
	// replaces the old mirror hash rather than bypassing immutable-spec drift.
	h.ledger.Status.AppliedSpecHash = computeLedgerSpecHash(&h.ledger.Spec)
	h.ledger.Spec.Mode = "normal"
	h.ledger.Status.Mode = "mirror"
	h.ledger.Generation = 2

	return h
}

func TestLedgerPromotionRealStatusInterruption(t *testing.T) {
	t.Parallel()
	h := newRealPromotionHarness(t)
	writes := 0
	interruption := errors.New("injected promotion status persistence failure")
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
	promotions := 0
	h.afterExec = func(script string) {
		if strings.Contains(script, "'ledgers' 'promote'") {
			promotions++
		}
	}
	_, err := h.controller(c).Reconcile(t.Context(), h.request())
	require.ErrorIs(t, err, interruption)
	requireRealLedgerMode(t, h, "NORMAL")
	require.Equal(t, "mirror", h.read(c).Status.Mode, "failed status persistence cannot certify completion")
	fresh := h.controller(c)
	result, err := fresh.Reconcile(t.Context(), h.request())
	require.NoError(t, err)
	require.True(t, result.Requeue)
	require.Equal(t, "normal", h.read(c).Status.Mode)
	require.Equal(t, 1, promotions, "fresh controller observes completion without another mutation")
	_, err = fresh.Reconcile(t.Context(), h.request())
	require.NoError(t, err)
	require.Equal(t, 3, writes, "one failed persistence followed by promotion and index convergence")
	require.True(t, h.indexExists())
	require.Equal(t, []string{"reference"}, h.read(c).Status.AppliedIndexes)
}

func requireRealLedgerMode(t *testing.T, h *realIndexHarness, mode string) {
	t.Helper()
	out, err := h.run("ledgers", "get", "--json", h.ledger.Spec.Name)
	require.NoError(t, err)
	var actual struct {
		Mode string `json:"mode"`
	}
	require.NoError(t, json.Unmarshal([]byte(out), &actual))
	require.Equal(t, mode, actual.Mode)
}

// The existing root TestOperatorIndexOwnership harness runs this test against
// a real Ledger process and CLI; Kubernetes status persistence is intercepted.
func TestLedgerPromotionRealLostResponse(t *testing.T) {
	t.Parallel()
	h := newRealPromotionHarness(t)
	c := h.client(interceptor.Funcs{})
	promotionAttempts := 0
	lost := errors.New("injected lost promotion acknowledgement after commit")
	exec := func(ctx context.Context, cfg *rest.Config, clientset kubernetes.Interface, namespace, pod, container string, command []string) (*execResult, error) {
		result, err := h.podExec(ctx, cfg, clientset, namespace, pod, container, command)
		if strings.Contains(command[2], "'ledgers' 'promote'") {
			promotionAttempts++
			if err == nil && promotionAttempts == 1 {
				return result, lost
			}
		}

		return result, err
	}
	r := h.controller(c)
	r.exec = exec
	result, err := r.Reconcile(t.Context(), h.request())
	require.NoError(t, err)
	require.Positive(t, result.RequeueAfter)
	require.Equal(t, 1, promotionAttempts)
	requireRealLedgerMode(t, h, "NORMAL")
	stored := h.read(c)
	require.Equal(t, "mirror", stored.Status.Mode)
	condition := meta.FindStatusCondition(stored.Status.Conditions, conditionLedgerSynced)
	require.NotNil(t, condition)
	require.Contains(t, condition.Message, lost.Error())
	// Reconstruct from persisted Kubernetes state, with no in-memory recovery.
	fresh := h.controller(c)
	fresh.exec = exec
	result, err = fresh.Reconcile(t.Context(), h.request())
	require.NoError(t, err)
	require.True(t, result.Requeue)
	stored = h.read(c)
	require.Equal(t, "normal", stored.Status.Mode)
	require.Equal(t, ledgerv1alpha1.LedgerPhaseReady, stored.Status.Phase)
	require.Empty(t, stored.Status.Message)
	require.Equal(t, int64(2), stored.Status.ObservedGeneration)
	require.Equal(t, 1, promotionAttempts, "observed completion must not repeat promotion")
	_, err = fresh.Reconcile(t.Context(), h.request())
	require.NoError(t, err)
	require.True(t, h.indexExists(), "the promotion requeue must reconcile declared indexes")
	require.Equal(t, []string{"reference"}, h.read(c).Status.AppliedIndexes)
}
