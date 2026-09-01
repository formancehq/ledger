package bootstrap

import (
	"context"
	"time"

	"github.com/formancehq/ledger/v3/internal/pkg/worker"
)

// clusterPolicyReconcileInterval is how often the leader re-checks the applied
// cluster policy against the desired one. Short so a fresh leader commits the
// policy — and thereby opens write readiness — promptly.
const clusterPolicyReconcileInterval = 1 * time.Second

// ClusterPolicyReconciler periodically drives the replicated cluster policy
// toward the desired revision while this node is leader. It is periodic rather
// than one-shot on leadership acquisition so a transient proposal failure
// (propose timeout, a momentary write gate, leadership churn) self-heals on the
// next tick instead of leaving the node stuck NOT_SERVING until another
// leadership event — write readiness depends on the policy being committed
// (EN-1827). Reconciliation is idempotent: once the applied revision reaches the
// desired one, every tick is a no-op.
type ClusterPolicyReconciler struct {
	reconcile func(context.Context)
	interval  time.Duration
	w         worker.Worker
}

// NewClusterPolicyReconciler builds a reconciler that invokes reconcile on start
// and on every tick. reconcile receives a context cancelled on Stop and must
// gate on leadership itself.
func NewClusterPolicyReconciler(reconcile func(context.Context)) *ClusterPolicyReconciler {
	return &ClusterPolicyReconciler{
		reconcile: reconcile,
		interval:  clusterPolicyReconcileInterval,
		w:         worker.New(),
	}
}

// Start runs an immediate reconciliation, then one per interval, until Stop.
func (r *ClusterPolicyReconciler) Start() {
	r.w.Run(func(stop <-chan struct{}) {
		ctx := worker.ContextFromStop(stop)
		r.reconcile(ctx)
		worker.RunTicker(stop, r.interval, func() { r.reconcile(ctx) })
	})
}

// Stop signals the background goroutine to stop and waits for it to finish.
func (r *ClusterPolicyReconciler) Stop() {
	r.w.Stop()
}
