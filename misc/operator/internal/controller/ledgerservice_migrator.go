package controller

import (
	"context"
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
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
// then rehomes ownerReferences on every child object from the LedgerService
// to the new Cluster so the Cluster reconciler can take over management.
// It never deletes the LedgerService: operators remove them manually once
// migration has converged.
type LedgerServiceMigrator struct {
	client.Client

	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=ledger.formance.com,resources=ledgerservices,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=ledger.formance.com,resources=ledgerservices/status,verbs=get;update;patch

// dnsEndpointGVK is the GroupVersionKind of the external-dns DNSEndpoint
// CRD the Cluster reconciler may create. The CRD is optional, so the
// migrator tolerates NoKindMatchError / NotFound when listing it.
var dnsEndpointGVK = schema.GroupVersionKind{
	Group:   "externaldns.k8s.io",
	Version: "v1alpha1",
	Kind:    "DNSEndpoint",
}

// Reconcile migrates a LedgerService to a Cluster. Idempotent by design:
// callers must never rely on side effects beyond "Cluster exists, children
// are re-parented, and LedgerService is annotated".
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
		if err := r.Create(ctx, cluster); err != nil {
			if !apierrors.IsAlreadyExists(err) {
				return ctrl.Result{}, fmt.Errorf("creating Cluster from LedgerService: %w", err)
			}
			if err := r.Get(ctx, types.NamespacedName{Name: ls.Name, Namespace: ls.Namespace}, cluster); err != nil {
				return ctrl.Result{}, fmt.Errorf("re-reading existing Cluster after AlreadyExists: %w", err)
			}
		}
		logger.Info("migrated LedgerService to Cluster", "name", ls.Name, "namespace", ls.Namespace)
	case err != nil:
		return ctrl.Result{}, fmt.Errorf("looking up existing Cluster: %w", err)
	}

	if err := r.rehomeChildren(ctx, ls, cluster); err != nil {
		return ctrl.Result{}, fmt.Errorf("rehoming child ownerReferences: %w", err)
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

// rehomeChildren walks every child object kind the Cluster reconciler owns
// and rewrites any controller ownerReference that still points at the
// LedgerService so it points at the new Cluster instead. Without this,
// controllerutil.SetControllerReference would fail in the Cluster reconciler
// ("object already has a different controller") and deleting the source
// LedgerService would garbage-collect the live workload via cascade delete.
//
// Children are matched by ownerReference UID (not by the operator's instance
// label): a LedgerService whose spec.additionalLabels overrode
// app.kubernetes.io/instance would leave its children with a different label
// value, so a label filter here would silently miss every one of them.
// Listing per-kind in the CR's namespace is bounded and the in-memory UID
// check filters out unrelated objects, so this stays cheap.
func (r *LedgerServiceMigrator) rehomeChildren(ctx context.Context, ls *ledgerv1alpha1.LedgerService, cluster *ledgerv1alpha1.Cluster) error {
	nsOnly := []client.ListOption{client.InNamespace(ls.Namespace)}

	var stsList appsv1.StatefulSetList
	if err := r.List(ctx, &stsList, nsOnly...); err != nil {
		return fmt.Errorf("listing StatefulSets: %w", err)
	}
	for i := range stsList.Items {
		if err := r.rehomeOne(ctx, &stsList.Items[i], ls, cluster); err != nil {
			return err
		}
	}

	var svcList corev1.ServiceList
	if err := r.List(ctx, &svcList, nsOnly...); err != nil {
		return fmt.Errorf("listing Services: %w", err)
	}
	for i := range svcList.Items {
		if err := r.rehomeOne(ctx, &svcList.Items[i], ls, cluster); err != nil {
			return err
		}
	}

	var saList corev1.ServiceAccountList
	if err := r.List(ctx, &saList, nsOnly...); err != nil {
		return fmt.Errorf("listing ServiceAccounts: %w", err)
	}
	for i := range saList.Items {
		if err := r.rehomeOne(ctx, &saList.Items[i], ls, cluster); err != nil {
			return err
		}
	}

	var cmList corev1.ConfigMapList
	if err := r.List(ctx, &cmList, nsOnly...); err != nil {
		return fmt.Errorf("listing ConfigMaps: %w", err)
	}
	for i := range cmList.Items {
		if err := r.rehomeOne(ctx, &cmList.Items[i], ls, cluster); err != nil {
			return err
		}
	}

	var secretList corev1.SecretList
	if err := r.List(ctx, &secretList, nsOnly...); err != nil {
		return fmt.Errorf("listing Secrets: %w", err)
	}
	for i := range secretList.Items {
		if err := r.rehomeOne(ctx, &secretList.Items[i], ls, cluster); err != nil {
			return err
		}
	}

	var ingList networkingv1.IngressList
	if err := r.List(ctx, &ingList, nsOnly...); err != nil {
		return fmt.Errorf("listing Ingresses: %w", err)
	}
	for i := range ingList.Items {
		if err := r.rehomeOne(ctx, &ingList.Items[i], ls, cluster); err != nil {
			return err
		}
	}

	var npList networkingv1.NetworkPolicyList
	if err := r.List(ctx, &npList, nsOnly...); err != nil {
		return fmt.Errorf("listing NetworkPolicies: %w", err)
	}
	for i := range npList.Items {
		if err := r.rehomeOne(ctx, &npList.Items[i], ls, cluster); err != nil {
			return err
		}
	}

	// DNSEndpoint (external-dns) is optional — the CRD may not be installed.
	// Tolerate NoKindMatchError / NotFound so a cluster without external-dns
	// does not fail the whole migration.
	dnsList := &unstructured.UnstructuredList{}
	dnsList.SetGroupVersionKind(dnsEndpointGVK)
	if err := r.List(ctx, dnsList, nsOnly...); err != nil {
		if !meta.IsNoMatchError(err) && !apierrors.IsNotFound(err) {
			return fmt.Errorf("listing DNSEndpoints: %w", err)
		}
	} else {
		for i := range dnsList.Items {
			if err := r.rehomeOne(ctx, &dnsList.Items[i], ls, cluster); err != nil {
				return err
			}
		}
	}

	return nil
}

// rehomeOne replaces any ownerReference on obj that points to ls with a
// controller ownerReference to cluster. Returns nil (no update) when the
// object was never owned by ls — the pass is idempotent and cheap on
// unrelated objects picked up by the namespace-wide list.
func (r *LedgerServiceMigrator) rehomeOne(ctx context.Context, obj client.Object, ls *ledgerv1alpha1.LedgerService, cluster *ledgerv1alpha1.Cluster) error {
	refs := obj.GetOwnerReferences()
	changed := false
	next := refs[:0:0]
	for _, ref := range refs {
		if ref.UID == ls.UID {
			changed = true

			continue
		}
		next = append(next, ref)
	}
	if !changed {
		return nil
	}

	isController := true
	blockOwnerDeletion := true
	next = append(next, metav1.OwnerReference{
		APIVersion:         ledgerv1alpha1.GroupVersion.String(),
		Kind:               "Cluster",
		Name:               cluster.Name,
		UID:                cluster.UID,
		Controller:         &isController,
		BlockOwnerDeletion: &blockOwnerDeletion,
	})
	obj.SetOwnerReferences(next)

	if err := r.Update(ctx, obj); err != nil {
		return fmt.Errorf("rehoming %T %s/%s: %w", obj, obj.GetNamespace(), obj.GetName(), err)
	}

	log.FromContext(ctx).Info("rehomed child owner",
		"kind", fmt.Sprintf("%T", obj),
		"name", obj.GetName(),
		"from", "LedgerService/"+ls.Name,
		"to", "Cluster/"+cluster.Name)

	return nil
}

// SetupWithManager registers the migrator with the manager. The migrator
// only watches LedgerService; the Cluster reconciler owns the downstream
// state.
func (r *LedgerServiceMigrator) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&ledgerv1alpha1.LedgerService{}).
		Complete(r)
}
