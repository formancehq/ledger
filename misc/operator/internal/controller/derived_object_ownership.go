package controller

import (
	"context"
	"errors"
	"fmt"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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

// errResourceNameConflict signals that an object under a name the operator
// derives from a Cluster already exists and is not controlled by that
// Cluster. Derived names are not unique by construction: "ledger-<a>-headless"
// is both the headless Service of Cluster "a" and the base name of Cluster
// "a-headless", and any unrelated object in the namespace can hold a derived
// name. A name match is never proof of ownership: the Cluster must never
// adopt, mutate, delete or exec into an object it does not control.
var errResourceNameConflict = errors.New("resource name conflict")

// errPodNotOwned signals that the pod an exec path targets does not exist or
// is not controlled by the Cluster's StatefulSet that was just inspected. An
// owned StatefulSet does not make every "<statefulset>-<ordinal>" pod its own:
// while the garbage collector removes the runtime of a deleted Cluster that is
// recreated under the same name, a pod under that name can still be controlled
// by the old StatefulSet, and after the VolumeClaimTemplate orphan-and-recreate
// path a pod has no controller until the new StatefulSet adopts it. Nothing is
// executed; the caller reacts as to any other failed exec.
var errPodNotOwned = errors.New("pod not controlled by the Cluster StatefulSet")

const (
	// conditionResourceNamesAvailable reports whether the Cluster controls
	// (or could create) every derived object the last pass reconciled.
	conditionResourceNamesAvailable = "ResourceNamesAvailable"

	// nameConflictRequeueInterval re-checks a conflicting Cluster so it
	// converges once the conflicting object is gone: nothing else enqueues
	// it, since it does not own that object.
	nameConflictRequeueInterval = time.Minute
)

// notControlledBy reports whether an existing derived object is not
// controlled by cluster: it has no controller reference, or one with another
// UID. Every object the operator creates gets its controller reference in the
// same CreateOrUpdate mutate call that creates it, so an object without one
// was not created by this Cluster and must never be adopted.
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

// nameConflictError builds the actionable errResourceNameConflict message for
// an object found under a derived name that cluster does not control.
func nameConflictError(kind string, obj metav1.Object, cluster *ledgerv1alpha1.Cluster) error {
	return fmt.Errorf(
		"%w: %s; the operator will not adopt, modify or delete it: remove it, or restore its controller reference to this Cluster",
		errResourceNameConflict, describeOwnership(kind, obj, cluster),
	)
}

// markResourceNamesOwned sets ResourceNamesAvailable=True once a pass has
// reconciled every derived object without meeting one it does not control.
func markResourceNamesOwned(ledger *ledgerv1alpha1.Cluster) {
	meta.SetStatusCondition(&ledger.Status.Conditions, metav1.Condition{
		Type:               conditionResourceNamesAvailable,
		Status:             metav1.ConditionTrue,
		Reason:             "Owned",
		ObservedGeneration: ledger.Generation,
	})
}

// reportResourceNameConflict turns an errResourceNameConflict raised by any
// reconcile step into the loud failure path: ResourceNamesAvailable=False
// (reason NameConflict), phase Error, a warning event, and a requeue so the
// Cluster converges once the conflict is removed. Any other result and error
// are returned unchanged.
func (r *ClusterReconciler) reportResourceNameConflict(ctx context.Context, ledger *ledgerv1alpha1.Cluster, result ctrl.Result, err error) (ctrl.Result, error) {
	if !errors.Is(err, errResourceNameConflict) {
		return result, err
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
	log.FromContext(ctx).Error(err, "refusing to reconcile an object not controlled by this Cluster")

	return ctrl.Result{RequeueAfter: nameConflictRequeueInterval}, nil
}

// setOwnedControllerReference replaces a bare SetControllerReference in every
// Cluster mutate function. CreateOrUpdate hands the mutate function the live
// object when one exists under the name; an existing object (it carries a
// resourceVersion, which a not-yet-created object never has) that this Cluster
// does not control was not created by it and is refused with
// errResourceNameConflict. CreateOrUpdate writes nothing when the mutate
// function fails, so the foreign object is left untouched.
func setOwnedControllerReference(ledger *ledgerv1alpha1.Cluster, obj client.Object, scheme *runtime.Scheme, kind string) error {
	if obj.GetResourceVersion() != "" && notControlledBy(obj, ledger) {
		return nameConflictError(kind, obj, ledger)
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
// The guarantee is identity only: the precondition pins the UID, not the
// resourceVersion, so a change to that same object between the read and the
// delete is not detected, in particular an ownerReference change after its
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
		return fmt.Errorf("invariant: refusing to use pod %s/%s without the UID of the inspected Cluster StatefulSet", namespace, name)
	}
	if clientset == nil {
		return fmt.Errorf("invariant: refusing to use pod %s/%s without a client to verify its controller", namespace, name)
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
		return fmt.Errorf("reading pod %s/%s to verify its controller: %w", namespace, name, err)
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
