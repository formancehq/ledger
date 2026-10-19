package controller

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
)

// errResourceNameConflict signals that an object the operator derives from a
// Cluster's base name (see clusterBaseName) already exists and is not
// controlled by that Cluster. spec.resourceName lets two Clusters of one
// namespace resolve to the same base name — two equal overrides, or an override
// equal to another Cluster's default "ledger-<name>" — and lets a base name
// such as "ledger" collide with unrelated objects (e.g. a Ledger v2 Service)
// that carry no controller reference at all. The Cluster must never adopt,
// mutate, delete or exec into an object it does not control.
var errResourceNameConflict = errors.New("resource name conflict")

// errPodNotOwned signals that the pod an exec path targets does not exist or
// is not controlled by the Cluster's StatefulSet that was just inspected. An
// owned StatefulSet does not make every "<base>-<ordinal>" pod its own: while
// the garbage collector removes a deleted same-base-name Cluster's runtime, a
// pod under that name can still be controlled by the old StatefulSet, and
// after the VolumeClaimTemplate orphan-and-recreate path a pod has no
// controller until the new StatefulSet adopts it. Nothing is executed; the
// caller reacts as to any other failed exec.
var errPodNotOwned = errors.New("pod not controlled by the Cluster StatefulSet")

const (
	// conditionResourceNamesAvailable reports whether the Cluster controls (or
	// can create) every object derived from its base name.
	conditionResourceNamesAvailable = "ResourceNamesAvailable"

	// nameConflictRequeueInterval re-checks a conflicting Cluster so it
	// converges once the other owner and its objects are gone: nothing else
	// enqueues it, since it does not own the conflicting objects.
	nameConflictRequeueInterval = time.Minute
)

// notControlledBy reports whether an existing derived object is not controlled
// by cluster: it has no controller reference, or one with another UID. Every
// object the operator creates gets its controller reference in the same
// CreateOrUpdate mutate call that creates it, so an object without one was not
// created by this Cluster and must never be adopted.
func notControlledBy(obj metav1.Object, cluster *ledgerv1alpha1.Cluster) bool {
	return !metav1.IsControlledBy(obj, cluster)
}

// describeOwnership renders why obj (a kind/name already found under a derived
// name) is not owned by cluster, for conflict messages.
func describeOwnership(kind string, obj metav1.Object, cluster *ledgerv1alpha1.Cluster) string {
	ref := metav1.GetControllerOf(obj)
	if ref == nil {
		return fmt.Sprintf("%s/%s exists and is not owned by Cluster %q", kind, obj.GetName(), cluster.Name)
	}

	return fmt.Sprintf("%s/%s is controlled by %s/%s (uid %s), not Cluster %q", kind, obj.GetName(), ref.Kind, ref.Name, ref.UID, cluster.Name)
}

// validateResourceNameOwnership fails the reconcile when any object whose name
// derives from the Cluster base name exists and is not controlled by this
// Cluster (no controller reference, or another controller). It runs before any
// reconcile step that creates, updates or deletes those objects by name
// (including pruneDisabledOptionalServices), so a conflicting Cluster surfaces
// a ResourceNamesAvailable=False condition and never adopts or touches objects
// it did not create. NotFound objects are fine: they will be created in this
// pass. Without this guard, CreateOrUpdate + SetControllerReference would
// silently adopt and overwrite an object with no controller reference, and a
// name-based delete path could remove another owner's object.
func (r *ClusterReconciler) validateResourceNameOwnership(ctx context.Context, ledger *ledgerv1alpha1.Cluster) error {
	type check struct {
		kind string
		obj  client.Object
	}

	// Only names this pass will create or update are checked: a foreign object
	// under a name the Cluster does not use (a disabled optional Service,
	// Ingress or NetworkPolicy, a DNSEndpoint entry with no endpoints) must not
	// block it, and the delete paths for those names skip objects the Cluster
	// does not control (deleteIfExists). The auth-keys ConfigMap exists only
	// while Credentials match, and the cluster Secret only while TLS is at
	// least partially active, so they are left to the adoption-point check in
	// setOwnedControllerReference, which reports the same condition.
	base := clusterBaseName(ledger)
	checks := []check{
		{"StatefulSet", &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: base}}},
		{"Service", &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: base}}},
	}
	if enabled := ledger.Spec.HeadlessService.Enabled; enabled == nil || *enabled {
		checks = append(checks, check{"Service", &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: headlessServiceName(ledger)}}})
	}
	if ledger.Spec.IngressGrpc != nil && ledger.Spec.IngressGrpc.Enabled {
		checks = append(checks, check{"Service", &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: grpcServiceName(ledger)}}})
		if len(ledger.Spec.IngressGrpc.Hosts) > 0 {
			checks = append(checks, check{"Ingress", &networkingv1.Ingress{ObjectMeta: metav1.ObjectMeta{Name: grpcIngressName(ledger)}}})
		}
	}
	if ledger.Spec.Ingress != nil && ledger.Spec.Ingress.Enabled {
		checks = append(checks, check{"Ingress", &networkingv1.Ingress{ObjectMeta: metav1.ObjectMeta{Name: base}}})
	}
	if ledger.Spec.NetworkPolicy != nil && ledger.Spec.NetworkPolicy.Enabled {
		checks = append(checks, check{"NetworkPolicy", &networkingv1.NetworkPolicy{ObjectMeta: metav1.ObjectMeta{Name: base}}})
	}
	// A ServiceAccount the operator does not create is user-managed and may be
	// shared on purpose; only the operator-created one is owned.
	if create := ledger.Spec.ServiceAccount.Create; create == nil || *create {
		checks = append(checks, check{"ServiceAccount", &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: serviceAccountName(ledger)}}})
	}
	for i := range ledger.Spec.DNSEndpoints {
		entry := &ledger.Spec.DNSEndpoints[i]
		// Mirrors reconcileDNSEndpoint: disabled or empty entries are not created.
		if !entry.Enabled || len(entry.Endpoints) == 0 {
			continue
		}
		obj := &unstructured.Unstructured{}
		obj.SetGroupVersionKind(dnsEndpointGVK)
		obj.SetName(dnsEndpointName(ledger, entry.Name))
		checks = append(checks, check{"DNSEndpoint", obj})
	}

	var conflicts []string
	for _, c := range checks {
		key := types.NamespacedName{Namespace: ledger.Namespace, Name: c.obj.GetName()}
		if err := r.Get(ctx, key, c.obj); err != nil {
			// NotFound: created in this pass. NoKindMatch: the optional
			// DNSEndpoint CRD is not installed, so nothing can conflict.
			if ignoreNotFound(err) == nil {
				continue
			}

			return fmt.Errorf("fetching %s %s: %w", c.kind, key, err)
		}
		if notControlledBy(c.obj, ledger) {
			conflicts = append(conflicts, describeOwnership(c.kind, c.obj, ledger))
		}
	}

	if len(conflicts) == 0 {
		return nil
	}

	return nameConflictError(base, conflicts)
}

// reportResourceNameConflict turns an errResourceNameConflict raised by the
// up-front guard or by an adoption point into the loud failure path:
// ResourceNamesAvailable=False (reason NameConflict), phase Error, a warning
// event, and a requeue so the Cluster converges once the conflict is removed
// (nothing else enqueues it, since it does not own the conflicting objects).
// It reports false for any other error.
func (r *ClusterReconciler) reportResourceNameConflict(ctx context.Context, ledger *ledgerv1alpha1.Cluster, err error) (ctrl.Result, bool) {
	if !errors.Is(err, errResourceNameConflict) {
		return ctrl.Result{}, false
	}
	meta.SetStatusCondition(&ledger.Status.Conditions, metav1.Condition{
		Type:               conditionResourceNamesAvailable,
		Status:             metav1.ConditionFalse,
		Reason:             "NameConflict",
		Message:            err.Error(),
		ObservedGeneration: ledger.Generation,
	})
	ledger.Status.Phase = "Error"
	if r.Recorder != nil {
		r.Recorder.Event(ledger, corev1.EventTypeWarning, "ResourceNameConflict", err.Error())
	}
	log.FromContext(ctx).Error(err, "refusing to reconcile objects not controlled by this Cluster")

	return ctrl.Result{RequeueAfter: nameConflictRequeueInterval}, true
}

// nameConflictError builds the actionable errResourceNameConflict message.
func nameConflictError(base string, conflicts []string) error {
	return fmt.Errorf(
		"%w: base name %q resolves to existing objects this Cluster does not control and will not adopt; "+
			"set a unique spec.resourceName (create-time only: recreate this Cluster) or remove the conflicting objects: %s",
		errResourceNameConflict, base, strings.Join(conflicts, "; "),
	)
}

// setOwnedControllerReference is the adoption-point form of the guard, used by
// every Cluster reconciler mutate function instead of a bare
// SetControllerReference. CreateOrUpdate hands the mutate function the live
// object when one exists under the name; an existing object (it carries a
// resourceVersion, which a not-yet-created object never has) that this Cluster
// does not control was not created by it and is refused with
// errResourceNameConflict before anything is written back. This closes the
// window between validateResourceNameOwnership and the write, and protects any
// derived object missing from the up-front list.
func setOwnedControllerReference(ledger *ledgerv1alpha1.Cluster, obj client.Object, scheme *runtime.Scheme, kind string) error {
	if obj.GetResourceVersion() != "" && notControlledBy(obj, ledger) {
		return nameConflictError(clusterBaseName(ledger), []string{describeOwnership(kind, obj, ledger)})
	}

	return controllerutil.SetControllerReference(ledger, obj, scheme)
}

// deleteIfExists deletes a derived object by name if it exists and this
// Cluster controls it. An object under the name that the Cluster does not
// control (another Cluster's, or an unrelated object) is left untouched: a
// name match is never proof of ownership.
func (r *ClusterReconciler) deleteIfExists(ctx context.Context, ledger *ledgerv1alpha1.Cluster, obj client.Object) error {
	err := r.Get(ctx, types.NamespacedName{
		Name:      obj.GetName(),
		Namespace: obj.GetNamespace(),
	}, obj)
	if err != nil {
		return ignoreNotFound(err)
	}
	if notControlledBy(obj, ledger) {
		log.FromContext(ctx).Info("not deleting object not controlled by this Cluster",
			"kind", fmt.Sprintf("%T", obj), "name", obj.GetName())

		return nil
	}

	return deleteInspected(ctx, r.Client, obj)
}

// deleteInspected deletes obj, an object whose ownership was just checked on
// a read, only while the live object under its name is still that object: the
// delete carries a UID precondition. Delete addresses objects by name, so
// without it an owned object replaced by a foreign one between the read and
// the delete (or a stale cached read) would delete the replacement.
//
// The guarantee is identity only: the delete hits the same object (UID) that
// was read, nothing more. The precondition pins the UID, not the
// resourceVersion, so a change to that same object between the read and the
// delete is not detected — in particular an ownerReference change, such as
// someone orphaning it or handing it to another controller, after its
// ownership was checked. That race is accepted: the API server and other
// controllers update these objects continually (a StatefulSet's status, for
// instance), so a resourceVersion precondition would turn every such write
// into a spurious conflict.
//
// A failed precondition (Conflict) is returned as an error so the reconcile
// retries and re-inspects ownership on the next pass; the replacement is never
// deleted. NotFound (already gone) is not an error. opts are applied before
// the precondition, which they cannot override.
func deleteInspected(ctx context.Context, c client.Client, obj client.Object, opts ...client.DeleteOption) error {
	uid := obj.GetUID()
	if uid == "" {
		return fmt.Errorf("invariant: refusing to delete %T %s/%s without the UID of the inspected object", obj, obj.GetNamespace(), obj.GetName())
	}
	err := c.Delete(ctx, obj, append(opts, client.Preconditions{UID: &uid})...)
	switch {
	case apierrors.IsNotFound(err):
		return nil
	case apierrors.IsConflict(err):
		return fmt.Errorf("%T %s/%s was replaced after its ownership was checked, re-inspecting on retry: %w", obj, obj.GetNamespace(), obj.GetName(), err)
	default:
		return err
	}
}

// requirePodControlledBy reads the pod namespace/name directly from the API
// server and fails with errPodNotOwned unless sts, the Cluster's StatefulSet
// whose ownership was just checked, is its controller. The read is uncached so
// it observes a pod replaced since the StatefulSet read.
func requirePodControlledBy(ctx context.Context, clientset kubernetes.Interface, sts *appsv1.StatefulSet, namespace, name string) error {
	if sts == nil || sts.UID == "" {
		return fmt.Errorf("invariant: refusing to exec into pod %s/%s without the UID of the inspected Cluster StatefulSet", namespace, name)
	}
	if clientset == nil {
		return fmt.Errorf("invariant: refusing to exec into pod %s/%s without a client to verify its controller", namespace, name)
	}
	// Bounded like the exec it guards, so a stuck read cannot hold the reconcile.
	readCtx, cancel := context.WithTimeout(ctx, ledgerExecTimeout)
	defer cancel()
	pod, err := clientset.CoreV1().Pods(namespace).Get(readCtx, name, metav1.GetOptions{})
	switch {
	case apierrors.IsNotFound(err):
		// Deliberately not phrased "not found": ledgerctl callers match that
		// text to recognise an absent ledger.
		return fmt.Errorf("%w: pod %s/%s does not exist", errPodNotOwned, namespace, name)
	case err != nil:
		return fmt.Errorf("reading pod %s/%s before exec: %w", namespace, name, err)
	}
	if metav1.IsControlledBy(pod, sts) {
		return nil
	}
	if ref := metav1.GetControllerOf(pod); ref != nil {
		return fmt.Errorf("%w: pod %s/%s is controlled by %s/%s (uid %s), not StatefulSet %s (uid %s)",
			errPodNotOwned, namespace, name, ref.Kind, ref.Name, ref.UID, sts.Name, sts.UID)
	}

	return fmt.Errorf("%w: pod %s/%s has no controller, expected StatefulSet %s (uid %s)",
		errPodNotOwned, namespace, name, sts.Name, sts.UID)
}

// ownedPodExec returns exec guarded by requirePodControlledBy: every call first
// re-reads its target pod and runs nothing unless sts controls it. Every exec
// into a Cluster pod goes through it. A pod replaced between that read and the
// exec stream (the exec API has no UID precondition) is not detected; the
// window is one API round trip.
func ownedPodExec(sts *appsv1.StatefulSet, exec ledgerctlExec) ledgerctlExec {
	return func(ctx context.Context, cfg *rest.Config, clientset kubernetes.Interface,
		namespace, podName, container string, command []string,
	) (*execResult, error) {
		if err := requirePodControlledBy(ctx, clientset, sts, namespace, podName); err != nil {
			return nil, err
		}

		return exec(ctx, cfg, clientset, namespace, podName, container, command)
	}
}
