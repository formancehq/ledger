package controller

import (
	"context"
	"errors"
	"sort"

	"github.com/google/uuid"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"

	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
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
	ns := ledger.Namespace
	svc := ledger.Spec.ClusterRef
	pod0 := podName(svc, 0)

	exec := func(args ...string) (string, error) {
		execCtx, cancel := context.WithTimeout(ctx, ledgerExecTimeout)
		defer cancel()

		return r.ledgerctlExecOutput(execCtx, ns, svc, pod0, grpcPort, args...)
	}

	return reconcileIndexesWithExec(ctx, ledger, exec)
}

func reconcileIndexesWithExec(ctx context.Context, ledger *ledgerv1alpha1.Ledger, exec func(...string) (string, error)) (bool, error) {
	log := ctrl.LoggerFrom(ctx)
	ledgerName := ledger.Spec.Name

	if ledger.UID == "" {
		return false, errors.New("index reconciliation requires a persisted Ledger UID")
	}
	creationPrefix := "ledger-operator/index/" + string(ledger.UID) + "/"
	var createdOK, droppedOK []managedIndex
	// Status is an observation reconstructed from the audit, never authority.
	var applied []string
	defer func() {
		ledger.Status.AppliedIndexes = nextAppliedIndexes(applied, indexDiff{toCreate: createdOK, toDrop: droppedOK})
	}()

	desired := desiredIndexes(ledger.Spec.Indexes)

	listOut, err := exec("indexes", "list", "--ledger", ledgerName, "--json")
	if err != nil {
		return false, err
	}

	actual, err := parseActualIndexes(listOut)
	if err != nil {
		return false, err
	}

	attributedOut, err := exec("indexes", "list", "--ledger", ledgerName, "--json", "--creation-key-prefix", creationPrefix)
	if err != nil {
		return false, err
	}
	attributed, err := parseActualIndexes(attributedOut)
	if err != nil {
		return false, err
	}
	for canonical := range attributed {
		applied = append(applied, canonical)
	}
	sort.Strings(applied)

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

	diff := diffIndexes(desired, actual, applied)

	for _, mi := range diff.toCreate {
		if _, createErr := exec(append(mi.createArgs(ledgerName), "--idempotency-key", creationPrefix+uuid.NewString())...); createErr != nil {
			return false, createErr
		}

		log.Info("created index", "ledger", ledgerName, "index", mi.canonical)
		createdOK = append(createdOK, mi)
		changed = true
	}

	for _, mi := range diff.toDrop {
		// Attribution was checked against the current creation in Ledger. A manual
		// replacement after that check can race this unguarded drop; operator-managed
		// indexes must only be changed through the Kubernetes declaration.
		// Issue a drop command only for indexes still present; ones already
		// gone out-of-band need no command but must still be relinquished from
		// ownership below so a later external recreate is not mistaken as ours.
		if actual[mi.canonical] {
			if _, dropErr := exec(mi.dropArgs(ledgerName)...); dropErr != nil && !isLedgerNotFound(dropErr) {
				return false, dropErr
			}

			log.Info("dropped index", "ledger", ledgerName, "index", mi.canonical)
			changed = true
		}

		droppedOK = append(droppedOK, mi)
	}

	// ledger.Status.AppliedIndexes is written by the deferred func above from
	// createdOK/droppedOK, so partial progress on an error path is still
	// recorded. A desired index already present without matching creation audit
	// evidence is not adopted. The next pass reconstructs attribution again.
	return !changed, nil
}
