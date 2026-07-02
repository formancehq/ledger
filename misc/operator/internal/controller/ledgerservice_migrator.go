package controller

import (
	"context"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	ledgerv1alpha1 "github.com/formance/ledger/operator/api/v1alpha1"
)

const (
	// migratedToAnnotation records the Cluster name a LedgerService was
	// migrated to. Once set, the migrator treats the LedgerService as done.
	migratedToAnnotation = "ledger.formance.com/migrated-to"

	// migratedCondition names the status condition set on a migrated
	// LedgerService.
	migratedCondition = "Migrated"
)

// LedgerServiceMigrator is a one-way reconciler that migrates deprecated
// LedgerService resources to Cluster resources of the same name+namespace,
// then annotates the LedgerService so a subsequent reconcile is a no-op.
// It never deletes the LedgerService: operators remove them manually once
// migration has converged.
type LedgerServiceMigrator struct {
	client.Client

	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=ledger.formance.com,resources=ledgerservices,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=ledger.formance.com,resources=ledgerservices/status,verbs=get;update;patch

// Reconcile migrates a LedgerService to a Cluster. Idempotent by design:
// callers must never rely on side effects beyond "Cluster exists and
// LedgerService is annotated".
func (r *LedgerServiceMigrator) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	ls := &ledgerv1alpha1.LedgerService{}
	if err := r.Get(ctx, req.NamespacedName, ls); err != nil {
		return ctrl.Result{}, ignoreNotFound(err)
	}

	if _, done := ls.Annotations[migratedToAnnotation]; done {
		return ctrl.Result{}, nil
	}

	cluster := &ledgerv1alpha1.Cluster{}
	err := r.Get(ctx, types.NamespacedName{Name: ls.Name, Namespace: ls.Namespace}, cluster)
	switch {
	case apierrors.IsNotFound(err):
		cluster = &ledgerv1alpha1.Cluster{
			ObjectMeta: metav1.ObjectMeta{
				Name:      ls.Name,
				Namespace: ls.Namespace,
				Labels:    ls.Labels,
				Annotations: map[string]string{
					"ledger.formance.com/migrated-from": "LedgerService",
				},
			},
			Spec: *ls.Spec.DeepCopy(),
		}
		if err := r.Create(ctx, cluster); err != nil && !apierrors.IsAlreadyExists(err) {
			return ctrl.Result{}, fmt.Errorf("creating Cluster from LedgerService: %w", err)
		}
		logger.Info("migrated LedgerService to Cluster", "name", ls.Name, "namespace", ls.Namespace)
	case err != nil:
		return ctrl.Result{}, fmt.Errorf("looking up existing Cluster: %w", err)
	}

	if ls.Annotations == nil {
		ls.Annotations = map[string]string{}
	}
	ls.Annotations[migratedToAnnotation] = ls.Name
	if err := r.Update(ctx, ls); err != nil {
		return ctrl.Result{}, fmt.Errorf("annotating LedgerService: %w", err)
	}

	meta.SetStatusCondition(&ls.Status.Conditions, metav1.Condition{
		Type:               migratedCondition,
		Status:             metav1.ConditionTrue,
		Reason:             "MigratedToCluster",
		Message:            fmt.Sprintf("Migrated to Cluster/%s. Delete this LedgerService once you have verified the Cluster.", ls.Name),
		ObservedGeneration: ls.Generation,
	})
	if err := r.Status().Update(ctx, ls); err != nil {
		return ctrl.Result{}, fmt.Errorf("updating LedgerService status: %w", err)
	}

	return ctrl.Result{}, nil
}

// SetupWithManager registers the migrator with the manager. The migrator
// only watches LedgerService; the Cluster reconciler owns the downstream
// state.
func (r *LedgerServiceMigrator) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&ledgerv1alpha1.LedgerService{}).
		Complete(r)
}
