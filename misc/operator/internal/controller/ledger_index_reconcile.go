package controller

import (
	"context"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"

	ledgerv1alpha1 "github.com/formance/ledger/operator/api/v1alpha1"
)

// handleIndexReconcile runs index reconciliation for a Ready ledger and folds
// the outcome into the IndexesSynced condition and the returned result.
// baseResult is returned when indexes are already in sync, preserving the
// caller's own requeue decision. When indexes are unmanaged (spec.Indexes ==
// nil) the condition is cleared and baseResult is returned unchanged.
func (r *LedgerReconciler) handleIndexReconcile(ctx context.Context, ledger *ledgerv1alpha1.Ledger, grpcPort int32, baseResult ctrl.Result) ctrl.Result {
	log := ctrl.LoggerFrom(ctx)

	if ledger.Spec.Indexes == nil {
		meta.RemoveStatusCondition(&ledger.Status.Conditions, conditionIndexesSynced)

		return baseResult
	}

	synced, err := r.reconcileIndexes(ctx, ledger, grpcPort)
	if err != nil {
		meta.SetStatusCondition(&ledger.Status.Conditions, metav1.Condition{
			Type:               conditionIndexesSynced,
			Status:             metav1.ConditionFalse,
			Reason:             "Error",
			Message:            err.Error(),
			ObservedGeneration: ledger.Generation,
		})
		log.Error(err, "index reconciliation failed", "name", ledger.Spec.Name)

		return ctrl.Result{RequeueAfter: ledgerRequeueDelay}
	}

	if !synced {
		meta.SetStatusCondition(&ledger.Status.Conditions, metav1.Condition{
			Type:               conditionIndexesSynced,
			Status:             metav1.ConditionFalse,
			Reason:             "InProgress",
			Message:            "index set changed; awaiting convergence",
			ObservedGeneration: ledger.Generation,
		})

		return ctrl.Result{RequeueAfter: ledgerRequeueDelay}
	}

	meta.SetStatusCondition(&ledger.Status.Conditions, metav1.Condition{
		Type:   conditionIndexesSynced,
		Status: metav1.ConditionTrue,
		Reason: "Synced",
		// "Synced" means the declared index set is present in the registry, NOT
		// that every index has finished building. Backfill is async and
		// per-replica (readiness lives in IndexStatus.current_version, not in
		// the registry the operator lists), so a query may still miss a
		// just-created index for a short window after this flips True.
		Message:            "declared index set present in registry; backfill may still be in progress",
		ObservedGeneration: ledger.Generation,
	})

	// Clear any stale message (e.g. a prior "waiting for Cluster for index
	// reconcile") now that the index set has converged.
	ledger.Status.Message = ""

	return baseResult
}

// reconcileIndexes converges the operator-owned index set on a managed ledger
// to spec.Indexes and records the owned set in status.appliedIndexes. It
// returns synced=true only when the ledger already matched the spec (no
// create/drop/schema change issued this pass). Callers must have verified
// spec.Indexes != nil.
//
// Each ledgerctl invocation runs under its own exec timeout (ledgerExecTimeout)
// since the reconcile issues several sequential commands.
func (r *LedgerReconciler) reconcileIndexes(ctx context.Context, ledger *ledgerv1alpha1.Ledger, grpcPort int32) (bool, error) {
	log := ctrl.LoggerFrom(ctx)

	ns := ledger.Namespace
	svc := ledger.Spec.ServiceRef
	pod0 := podName(svc, 0)
	ledgerName := ledger.Spec.Name

	exec := func(args ...string) (string, error) {
		execCtx, cancel := context.WithTimeout(ctx, ledgerExecTimeout)
		defer cancel()

		return r.ledgerctlExecOutput(execCtx, ns, svc, pod0, grpcPort, args...)
	}

	desired := desiredIndexes(ledger.Spec.Indexes)

	listOut, err := exec("indexes", "list", "--ledger", ledgerName, "--json")
	if err != nil {
		return false, err
	}

	actual, err := parseActualIndexes(listOut)
	if err != nil {
		return false, err
	}

	changed := false

	// Reconcile metadata schema first: a metadata index requires its field to
	// be declared in the schema, and a type change must re-declare it. Fetch
	// the schema once, lazily, only when a metadata index is desired.
	var schema *schemaStatus
	for _, mi := range desired {
		if mi.typeFlag != metadataTypeFlag {
			continue
		}

		if schema == nil {
			schemaOut, schemaErr := exec("ledgers", "get-schema", ledgerName, "--json")
			if schemaErr != nil {
				return false, schemaErr
			}

			schema, schemaErr = parseSchema(schemaOut)
			if schemaErr != nil {
				return false, schemaErr
			}
		}

		if metadataFieldNeedsUpdate(schema, mi) {
			if _, setErr := exec(mi.setMetadataTypeArgs(ledgerName)...); setErr != nil {
				return false, setErr
			}

			log.Info("declared metadata field for index",
				"ledger", ledgerName, "target", mi.target, "key", mi.key, "type", mi.mdType)
			changed = true
		}
	}

	diff := diffIndexes(desired, actual, ledger.Status.AppliedIndexes)

	for _, mi := range diff.toCreate {
		if _, createErr := exec(mi.createArgs(ledgerName)...); createErr != nil && !isAlreadyExists(createErr) {
			return false, createErr
		}

		log.Info("created index", "ledger", ledgerName, "index", mi.canonical)
		changed = true
	}

	for _, mi := range diff.toDrop {
		if _, dropErr := exec(mi.dropArgs(ledgerName)...); dropErr != nil && !isLedgerNotFound(dropErr) {
			return false, dropErr
		}

		log.Info("dropped index", "ledger", ledgerName, "index", mi.canonical)
		changed = true
	}

	// Record the operator-owned set so future reconciles scope drops correctly.
	// Only indexes the operator created are recorded — a desired index that
	// already existed is never adopted, so it is never dropped later.
	ledger.Status.AppliedIndexes = nextAppliedIndexes(ledger.Status.AppliedIndexes, diff)

	return !changed, nil
}
