package internal

import (
	"context"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

// The operator names every resource it creates with a "ledger-" prefix
// (misc/operator/internal/controller/names.go). The workload addresses
// those resources by name, so these helpers MUST agree with the operator.
// Drift on this contract caused EN-1319: WaitForStatefulSetReady polled a
// non-existent "ledger" StatefulSet for 8 minutes before timing out and
// false-flagging the rollout assertion.

func TestLedgerStatefulSetName_IsPrefixed(t *testing.T) {
	t.Parallel()

	got := LedgerStatefulSetName()
	if got != "ledger-ledger" {
		t.Fatalf("LedgerStatefulSetName() = %q, want %q (operator prefixes resources with %q)",
			got, "ledger-ledger", resourcePrefix)
	}
}

func TestLedgerPodName_MatchesOperatorLayout(t *testing.T) {
	t.Parallel()

	cases := []struct {
		ordinal int
		want    string
	}{
		{0, "ledger-ledger-0"},
		{1, "ledger-ledger-1"},
		{2, "ledger-ledger-2"},
	}
	for _, c := range cases {
		if got := LedgerPodName(c.ordinal); got != c.want {
			t.Errorf("LedgerPodName(%d) = %q, want %q", c.ordinal, got, c.want)
		}
	}
}

func TestPodOrdinal_AcceptsOperatorNames(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		want int
	}{
		// Operator-emitted names — the only shape this helper should accept.
		{"ledger-ledger-0", 0},
		{"ledger-ledger-2", 2},

		// Pre-EN-1319 unprefixed shape — must be rejected so the helper
		// surfaces the misnaming instead of silently picking up stray pods.
		{"ledger-0", -1},

		// Garbage.
		{"", -1},
		{"ledger-ledger-", -1},
		{"ledger-ledger-abc", -1},
		{"unrelated-pod", -1},
	}
	for _, c := range cases {
		if got := PodOrdinal(c.name); got != c.want {
			t.Errorf("PodOrdinal(%q) = %d, want %d", c.name, got, c.want)
		}
	}
}

// TestWaitForStatefulSetReady_NotFoundFailsFast guards the EN-1319 root cause:
// the old implementation `continue`d on a NotFound from the very first poll,
// so a misnamed StatefulSet looked like a slow rollout and burned the whole
// 8-minute deadline. The fix short-circuits on the initial NotFound so a
// naming regression surfaces in seconds.
func TestWaitForStatefulSetReady_NotFoundFailsFast(t *testing.T) {
	t.Parallel()

	clientset := fake.NewSimpleClientset()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	start := time.Now()
	ready := WaitForStatefulSetReady(ctx, clientset, "does-not-exist", 3, 30*time.Second)
	elapsed := time.Since(start)

	if ready {
		t.Fatalf("WaitForStatefulSetReady on missing STS returned true; want false")
	}
	// The first poll lands ~2s in (initial sleep), so anything under ~5s
	// proves we exited on NotFound rather than chewing through the deadline.
	if elapsed > 5*time.Second {
		t.Errorf("WaitForStatefulSetReady took %s, want <5s (must fail fast on initial NotFound)", elapsed)
	}
}

// TestWaitForStatefulSetReady_ReportsReady is the positive control: with a
// healthy fake STS, the helper observes ReadyReplicas == expected and the
// revisions matching, then returns true.
func TestWaitForStatefulSetReady_ReportsReady(t *testing.T) {
	t.Parallel()

	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      LedgerStatefulSetName(),
			Namespace: "default",
		},
		Status: appsv1.StatefulSetStatus{
			ReadyReplicas:   3,
			CurrentRevision: "rev-1",
			UpdateRevision:  "rev-1",
		},
	}
	clientset := fake.NewSimpleClientset(sts)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if !WaitForStatefulSetReady(ctx, clientset, LedgerStatefulSetName(), 3, 10*time.Second) {
		t.Fatalf("WaitForStatefulSetReady returned false for a healthy fake STS")
	}
}

func TestLedgerPodName_PodOrdinal_Roundtrip(t *testing.T) {
	t.Parallel()

	for ordinal := range 7 {
		name := LedgerPodName(ordinal)
		if got := PodOrdinal(name); got != ordinal {
			t.Errorf("PodOrdinal(LedgerPodName(%d)) = %d, want %d (name=%q)",
				ordinal, got, ordinal, name)
		}
	}
}
