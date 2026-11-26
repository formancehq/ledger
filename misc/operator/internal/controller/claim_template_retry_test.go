package controller

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
)

func TestClaimTemplateRetryIntervalsAndWatchWaits(t *testing.T) {
	t.Parallel()
	var retries claimTemplateRetries
	ledger := &ledgerv1alpha1.Cluster{ObjectMeta: metav1.ObjectMeta{Namespace: "test", Name: "ledger", UID: "uid"}}
	now := time.Unix(1000, 0)
	require.Zero(t, retries.wait(ledger, "digest", now))
	for _, delay := range []time.Duration{2 * time.Second, 4 * time.Second, 8 * time.Second, 16 * time.Second, 30 * time.Second, 30 * time.Second} {
		retries.failed(ledger, "digest", now)
		require.Equal(t, delay, retries.wait(ledger, "digest", now))
		state := retries.entries[client.ObjectKeyFromObject(ledger)]
		for range 100 {
			require.Equal(t, delay-time.Nanosecond, retries.wait(ledger, "digest", now.Add(time.Nanosecond)))
		}
		require.Equal(t, state, retries.entries[client.ObjectKeyFromObject(ledger)], "watch wakes must not increment attempts or slide the deadline")
		now = now.Add(delay)
		require.Zero(t, retries.wait(ledger, "digest", now), "the exact deadline allows another attempt")
	}
}

func TestClaimTemplateRetryResetsAndCleansState(t *testing.T) {
	t.Parallel()
	for _, reset := range []string{"payload", "UID", "success", "deleted", "expired"} {
		t.Run(reset, func(t *testing.T) {
			t.Parallel()
			var retries claimTemplateRetries
			ledger := &ledgerv1alpha1.Cluster{ObjectMeta: metav1.ObjectMeta{Namespace: "test", Name: "ledger", UID: "uid"}}
			now := time.Unix(1000, 0)
			digest := "digest"
			retries.failed(ledger, digest, now)
			retries.failed(ledger, digest, now.Add(2*time.Second))
			now = now.Add(2 * time.Second)
			switch reset {
			case "payload":
				digest = "other-digest"
			case "UID":
				ledger.UID = "replacement-uid"
			case "success":
				retries.succeeded(ledger, digest)
			case "deleted":
				retries.forget(client.ObjectKeyFromObject(ledger))
			case "expired":
				now = now.Add(claimTemplateRetryRetention)
			}
			require.Zero(t, retries.wait(ledger, digest, now))
			require.Empty(t, retries.entries)
			retries.failed(ledger, digest, now)
			require.Equal(t, 2*time.Second, retries.wait(ledger, digest, now))
		})
	}
}

func TestClaimTemplateRetryKeepsActiveDeadlinesAndOnePlanPerCluster(t *testing.T) {
	t.Parallel()
	var retries claimTemplateRetries
	now := time.Unix(1000, 0)
	for i := range 1100 {
		ledger := &ledgerv1alpha1.Cluster{ObjectMeta: metav1.ObjectMeta{Namespace: "test", Name: fmt.Sprintf("ledger-%d", i), UID: types.UID(strconv.Itoa(i))}}
		retries.failed(ledger, "digest", now)
	}
	require.Len(t, retries.entries, 1100)
	for i := range 1100 {
		ledger := &ledgerv1alpha1.Cluster{ObjectMeta: metav1.ObjectMeta{Namespace: "test", Name: fmt.Sprintf("ledger-%d", i), UID: types.UID(strconv.Itoa(i))}}
		require.Equal(t, 2*time.Second, retries.wait(ledger, "digest", now), "other Clusters cannot evict an active deadline")
	}
	ledger := &ledgerv1alpha1.Cluster{ObjectMeta: metav1.ObjectMeta{Namespace: "test", Name: "ledger-0", UID: "0"}}
	for i := range 100 {
		digest := strconv.Itoa(i)
		require.Zero(t, retries.wait(ledger, digest, now))
		retries.failed(ledger, digest, now)
	}
	require.Len(t, retries.entries, 1100, "historical payloads must not accumulate entries")
	require.Zero(t, retries.wait(ledger, "99", now.Add(claimTemplateRetryRetention)))
	require.Empty(t, retries.entries)
}

func TestClaimTemplateRetryConcurrentWorkers(t *testing.T) {
	t.Parallel()
	var retries claimTemplateRetries
	var workers sync.WaitGroup
	now := time.Unix(1000, 0)
	for i := range 32 {
		workers.Go(func() {
			ledger := &ledgerv1alpha1.Cluster{ObjectMeta: metav1.ObjectMeta{Namespace: "test", Name: fmt.Sprintf("ledger-%d", i), UID: types.UID(strconv.Itoa(i))}}
			for range 100 {
				retries.failed(ledger, "digest", now)
				retries.wait(ledger, "digest", now)
				retries.succeeded(ledger, "other-digest")
			}
		})
	}
	workers.Wait()
	require.Len(t, retries.entries, 32)
	for _, state := range retries.entries {
		require.Equal(t, 30*time.Second, state.delay)
	}
}

func TestClaimTemplateRetryReconcileForgetsRemovedCluster(t *testing.T) {
	t.Parallel()
	for _, terminating := range []bool{false, true} {
		t.Run(fmt.Sprintf("terminating=%t", terminating), func(t *testing.T) {
			t.Parallel()
			f := newClaimRecoveryFixture(t)
			ledger := f.latestLedger(t)
			if terminating {
				ledger.Finalizers = []string{"test.ledger.formance.com/retain"}
				require.NoError(t, f.client.Update(t.Context(), ledger))
			}
			r := f.reconciler(f.client)
			r.claimTemplateRetries.failed(ledger, "digest", time.Unix(1000, 0))
			require.NoError(t, f.client.Delete(t.Context(), ledger))
			result, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(ledger)})
			require.NoError(t, err)
			require.True(t, result.IsZero())
			require.Empty(t, r.claimTemplateRetries.entries)
		})
	}
}

func TestClaimTemplateRetryHoldDoesNotWriteOrChangeConditions(t *testing.T) {
	t.Parallel()
	f := newClaimRecoveryFixture(t)
	creates, pointerWrites := 0, 0
	failure := errors.New("journal quota denied")
	c := interceptor.NewClient(f.client, interceptor.Funcs{
		Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			if _, ok := obj.(*corev1.ConfigMap); ok {
				creates++

				return failure
			}

			return c.Create(ctx, obj, opts...)
		},
		SubResourcePatch: func(ctx context.Context, c client.Client, subResource string, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
			pointerWrites++

			return c.SubResource(subResource).Patch(ctx, obj, patch, opts...)
		},
	})
	r := f.reconciler(c)
	_, err := r.startClaimTemplateRecovery(t.Context(), f.ledger, f.source, f.claims)
	require.ErrorIs(t, err, failure)
	handled, _, err := r.resumeClaimTemplateRecovery(t.Context(), f.latestLedger(t))
	require.NoError(t, err)
	require.True(t, handled)
	ledger := f.latestLedger(t)
	ledger.Status.Conditions = f.ledger.Status.Conditions
	before := ledger.DeepCopy()
	state := r.claimTemplateRetries.entries[client.ObjectKeyFromObject(ledger)]
	for range 10 {
		result, err := r.startClaimTemplateRecovery(t.Context(), ledger, f.source, f.claims)
		wait, ok := errors.AsType[*claimTemplateRecoveryRetryWait](err)
		require.True(t, ok)
		require.Positive(t, result.RequeueAfter)
		require.Equal(t, wait.after, result.RequeueAfter)
		require.Equal(t, before.Status, ledger.Status, "scheduled waits must preserve every condition and the nil pointer")
	}
	require.Equal(t, state, r.claimTemplateRetries.entries[client.ObjectKeyFromObject(ledger)])
	require.Equal(t, 1, creates)
	require.Equal(t, 2, pointerWrites, "only the initial intent and its safe absence-abort may write")
	requireNoClaimTemplateRecovery(t, f.client, ledger)
	f.requireStorageUnchanged(t)
}

func TestClaimTemplateRetryIntentResponseLossIsRateLimited(t *testing.T) {
	t.Parallel()
	f := newClaimRecoveryFixture(t)
	failure := errors.New("intent response lost")
	c := interceptor.NewClient(f.client, interceptor.Funcs{
		SubResourcePatch: func(ctx context.Context, c client.Client, subResource string, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
			if err := c.SubResource(subResource).Patch(ctx, obj, patch, opts...); err != nil {
				return err
			}

			return failure
		},
	})
	r := f.reconciler(c)
	_, err := r.startClaimTemplateRecovery(t.Context(), f.ledger, f.source, f.claims)
	require.ErrorIs(t, err, failure)
	_, attempted := errors.AsType[*claimTemplateJournalWriteError](err)
	require.True(t, attempted)
	pointer := f.latestLedger(t).Status.ClaimTemplateRecovery
	require.NotNil(t, pointer, "the API committed before losing its response")
	state := r.claimTemplateRetries.entries[client.ObjectKeyFromObject(f.ledger)]
	require.Equal(t, pointer.PayloadSHA256, state.digest)
	require.Equal(t, claimTemplateRetryInitial, state.delay, "one failed intent attempt is counted once")
	require.Positive(t, r.claimTemplateRetries.wait(f.ledger, pointer.PayloadSHA256, time.Now()))
	f.requireStorageUnchanged(t)
}
