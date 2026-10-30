package controller

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	storagev1beta1 "k8s.io/api/storage/v1beta1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"

	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
)

const (
	walStorageCondition  = "WALStorageReady"
	dataStorageCondition = "DataStorageReady"
)

// persistenceStorageReconciler owns only in-place PVC minimum capacity and
// volume-attribute requests. It never creates, deletes, or replaces storage.
// A successful API write is a request, not proof that CSI has completed it.
type persistenceStorageReconciler struct {
	client      client.Client
	reader      client.Reader
	ledger      *ledgerv1alpha1.Cluster
	statefulSet *appsv1.StatefulSet
}

func (r *persistenceStorageReconciler) liveStatefulSet(ctx context.Context) (*appsv1.StatefulSet, error) {
	if r.reader == nil {
		return nil, errors.New("invariant: APIReader is required for direct storage reads")
	}
	var sts appsv1.StatefulSet
	if err := r.reader.Get(ctx, types.NamespacedName{Namespace: r.ledger.Namespace, Name: resourceName(r.ledger.Name)}, &sts); err != nil {
		return nil, fmt.Errorf("getting live StatefulSet: %w", err)
	}
	if notControlledBy(&sts, r.ledger) {
		return nil, nameConflictError("StatefulSet", &sts, r.ledger)
	}
	if r.ledger.UID == "" || sts.UID == "" {
		return nil, fmt.Errorf("StatefulSet %s/%s has no live Cluster or workload identity", sts.Namespace, sts.Name)
	}
	if !sts.DeletionTimestamp.IsZero() {
		return nil, fmt.Errorf("StatefulSet %s/%s is being deleted", sts.Namespace, sts.Name)
	}

	return &sts, nil
}

// validatePVCGroup establishes identity for existing claims, independently of
// desired immutable settings. Declaration drift must not prevent safe growth
// of the live workload, including automatic growth and optimistic retries.
func (r *persistenceStorageReconciler) validatePVCGroup(ctx context.Context, definition persistenceVolumeDefinition, pvcs []*corev1.PersistentVolumeClaim) error {
	if r.statefulSet == nil || r.ledger.UID == "" || !metav1.IsControlledBy(r.statefulSet, r.ledger) {
		return errors.New("live StatefulSet is not controlled by this Cluster")
	}
	templateIndex := slices.IndexFunc(r.statefulSet.Spec.VolumeClaimTemplates, func(pvc corev1.PersistentVolumeClaim) bool { return pvc.Name == definition.Name })
	if templateIndex < 0 {
		return fmt.Errorf("live StatefulSet has no %s PVC template", definition.Name)
	}
	for _, pvc := range pvcs {
		if err := r.validatePVCIdentity(ctx, definition.Name, pvc); err != nil {
			return err
		}
	}

	return nil
}

// validatePVCMigration only diagnoses unsupported desired changes. PVCs are
// selected by identity, never by their class or mode. Use the effective live
// class: Kubernetes may default a claim while leaving its template class nil.
// An omitted desired class preserves that existing selection.
func validatePVCMigration(definition persistenceVolumeDefinition, pvcs []*corev1.PersistentVolumeClaim) error {
	accessMode := corev1.ReadWriteOnce
	if definition.Spec.AccessMode != "" {
		accessMode = corev1.PersistentVolumeAccessMode(definition.Spec.AccessMode)
	}
	for _, pvc := range pvcs {
		if definition.Spec.StorageClass != "" && storageClassValue(pvc.Spec.StorageClassName) != definition.Spec.StorageClass {
			return fmt.Errorf("PVC %s: changing storageClass from %q to %q is unsupported", pvc.Name, storageClassValue(pvc.Spec.StorageClassName), definition.Spec.StorageClass)
		}
		if len(pvc.Spec.AccessModes) != 1 || pvc.Spec.AccessModes[0] != accessMode {
			return fmt.Errorf("PVC %s: changing accessMode is unsupported", pvc.Name)
		}
	}

	return nil
}

func (r *persistenceStorageReconciler) validatePVCIdentity(ctx context.Context, volume string, pvc *corev1.PersistentVolumeClaim) error {
	if pvc.UID == "" || !pvc.DeletionTimestamp.IsZero() {
		return fmt.Errorf("PVC %s has no live identity or is being deleted", pvc.Name)
	}
	// Retain is the default StatefulSet policy: its PVCs have no owner ref.
	// In that case a live, owned Pod must actually reference this claim. A
	// predictable PVC name or a label alone does not establish ownership.
	owned := false
	for _, ref := range pvc.OwnerReferences {
		switch {
		case ref.APIVersion == appsv1.SchemeGroupVersion.String() && ref.Kind == "StatefulSet" && ref.Name == r.statefulSet.Name && ref.UID == r.statefulSet.UID:
			owned = true
		case ref.APIVersion == "v1" && ref.Kind == "Pod":
			var pod corev1.Pod
			if err := r.reader.Get(ctx, types.NamespacedName{Namespace: pvc.Namespace, Name: ref.Name}, &pod); err != nil {
				return fmt.Errorf("validating PVC %s Pod owner: %w", pvc.Name, err)
			}
			if pod.UID != ref.UID || !metav1.IsControlledBy(&pod, r.statefulSet) || !podReferencesPVC(&pod, volume, pvc.Name) {
				return fmt.Errorf("PVC %s has a foreign Pod owner", pvc.Name)
			}
			owned = true
		default:
			return fmt.Errorf("PVC %s has a foreign owner %s %s UID %s", pvc.Name, ref.Kind, ref.Name, ref.UID)
		}
	}
	if owned {
		return nil
	}
	podName, ok := strings.CutPrefix(pvc.Name, volume+"-")
	if !ok {
		return fmt.Errorf("PVC %s does not match the %s claim name", pvc.Name, volume)
	}
	var pod corev1.Pod
	if err := r.reader.Get(ctx, types.NamespacedName{Namespace: pvc.Namespace, Name: podName}, &pod); err != nil {
		return fmt.Errorf("establishing PVC %s ownership from live Pod: %w", pvc.Name, err)
	}
	if !metav1.IsControlledBy(&pod, r.statefulSet) || !podReferencesPVC(&pod, volume, pvc.Name) {
		return fmt.Errorf("PVC %s is not referenced by the owned live Pod %s", pvc.Name, pod.Name)
	}

	return nil
}

// validateCurrentPVC rechecks both ownership objects immediately before a
// write. A UID alone does not fence changes to owner references on that same
// object, and a fresh PVC read must not silently accept a foreign owner.
func (r *persistenceStorageReconciler) validateCurrentPVC(ctx context.Context, definition persistenceVolumeDefinition, pvc *corev1.PersistentVolumeClaim) error {
	live, err := r.liveStatefulSet(ctx)
	if err != nil {
		return err
	}
	if r.statefulSet == nil || live.UID != r.statefulSet.UID {
		return errors.New("StatefulSet identity changed during PVC update; retry with the current workload")
	}
	current := *r
	current.statefulSet = live

	return current.validatePVCGroup(ctx, definition, []*corev1.PersistentVolumeClaim{pvc})
}

func (r *persistenceStorageReconciler) reconcileVolume(ctx context.Context, definition persistenceVolumeDefinition, replicas int32) (bool, error) {
	if definition.Spec.HostPath != nil {
		setStorageCondition(r.ledger, definition.Name, metav1.ConditionTrue, "HostPath", "volume uses hostPath; no PVC request is managed")

		return false, nil
	}
	floor := definition.Spec.Size
	if floor.IsZero() {
		floor = resource.MustParse(definition.DefaultSize)
	}
	if floor.Sign() <= 0 {
		setStorageCondition(r.ledger, definition.Name, metav1.ConditionFalse, "StorageUnsupported", "minimum capacity must be positive")

		return true, nil
	}
	pvcs := make([]*corev1.PersistentVolumeClaim, 0, replicas)
	for ordinal := range replicas {
		pvc := &corev1.PersistentVolumeClaim{}
		name := fmt.Sprintf("%s-%s-%d", definition.Name, r.statefulSet.Name, ordinal)
		if err := r.reader.Get(ctx, types.NamespacedName{Namespace: r.ledger.Namespace, Name: name}, pvc); err != nil {
			setStorageCondition(r.ledger, definition.Name, metav1.ConditionFalse, "PVCGroupNotReady", fmt.Sprintf("getting PVC %s: %v", name, err))

			return true, nil
		}
		pvcs = append(pvcs, pvc)
	}
	if err := r.validatePVCGroup(ctx, definition, pvcs); err != nil {
		setStorageCondition(r.ledger, definition.Name, metav1.ConditionFalse, "StorageUnsupported", err.Error())

		return true, nil
	}
	migrationErr := validatePVCMigration(definition, pvcs)

	var observations []string
	pending := false
	for _, pvc := range pvcs {
		current, err := r.patchPVC(ctx, definition, pvc, floor)
		if err != nil {
			setStorageCondition(r.ledger, definition.Name, metav1.ConditionFalse, "StorageUpdateFailed", strings.Join(append(observations, err.Error()), "; "))

			return true, err
		}
		ready, failed, observation := pvcStorageObservation(current, floor, definition.Spec.VolumeAttributesClassName)
		observations = append(observations, observation)
		if failed {
			setStorageCondition(r.ledger, definition.Name, metav1.ConditionFalse, "StorageUpdateFailed", strings.Join(observations, "; "))

			return true, nil
		}
		pending = pending || !ready
	}
	if migrationErr != nil {
		setStorageCondition(r.ledger, definition.Name, metav1.ConditionFalse, "StorageUnsupported", strings.Join(append(observations, migrationErr.Error()), "; "))

		return true, nil
	}
	reason, status := "Ready", metav1.ConditionTrue
	if pending {
		reason, status = "StorageUpdatePending", metav1.ConditionFalse
	}
	setStorageCondition(r.ledger, definition.Name, status, reason, strings.Join(observations, "; "))

	return pending, nil
}

func (r *persistenceStorageReconciler) patchPVC(ctx context.Context, definition persistenceVolumeDefinition, original *corev1.PersistentVolumeClaim, floor resource.Quantity) (*corev1.PersistentVolumeClaim, error) {
	var current *corev1.PersistentVolumeClaim
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		current = &corev1.PersistentVolumeClaim{}
		if err := r.reader.Get(ctx, client.ObjectKeyFromObject(original), current); err != nil {
			return err
		}
		if current.UID != original.UID {
			return fmt.Errorf("PVC %s was replaced during reconciliation; refusing to update it", original.Name)
		}
		if err := r.validateCurrentPVC(ctx, definition, current); err != nil {
			return err
		}
		base := current.DeepCopy()
		request := current.Spec.Resources.Requests[corev1.ResourceStorage]
		grow := request.Cmp(floor) < 0
		class := definition.Spec.VolumeAttributesClassName
		changeClass := class != storageClassValue(current.Spec.VolumeAttributesClassName)
		if changeClass && class == "" {
			return fmt.Errorf("PVC %s: resetting a non-empty volumeAttributesClassName is unsupported by Kubernetes", current.Name)
		}
		if grow {
			if err := validateExpandableStorageClasses(ctx, r.reader, []*corev1.PersistentVolumeClaim{current}); err != nil {
				return err
			}
			if current.Spec.Resources.Requests == nil {
				current.Spec.Resources.Requests = corev1.ResourceList{}
			}
			current.Spec.Resources.Requests[corev1.ResourceStorage] = floor.DeepCopy()
		}
		if changeClass {
			if err := r.validateVolumeAttributesClass(ctx, current, class); err != nil {
				return err
			}
			current.Spec.VolumeAttributesClassName = &class
		}
		if !grow && !changeClass {
			return nil
		}
		if err := r.client.Patch(ctx, current, client.MergeFromWithOptions(base, client.MergeFromWithOptimisticLock{})); err != nil {
			return err
		}
		// Older API servers can silently drop a feature-gated field. Read the
		// persisted object back rather than interpreting an HTTP success as support.
		if err := r.reader.Get(ctx, client.ObjectKeyFromObject(original), current); err != nil {
			return err
		}
		if current.UID != original.UID {
			return fmt.Errorf("PVC %s was replaced after the storage update", original.Name)
		}
		if changeClass && storageClassValue(current.Spec.VolumeAttributesClassName) != class {
			return fmt.Errorf("PVC %s: Kubernetes did not persist volumeAttributesClassName %q; VolumeAttributesClass capability is unavailable", current.Name, class)
		}

		return nil
	})
	if err != nil {
		return current, fmt.Errorf("updating PVC %s: %w", original.Name, err)
	}

	return current, nil
}

// refreshCondition observes the final requests after the automatic controller
// has run. Its capacity patch must not leave an earlier Ready observation in
// the explicit storage condition from the same reconciliation.
func (r *persistenceStorageReconciler) refreshCondition(ctx context.Context, definition persistenceVolumeDefinition, replicas int32) bool {
	var observations []string
	pending, failed := false, false
	floor := definition.Spec.Size
	if floor.IsZero() {
		floor = resource.MustParse(definition.DefaultSize)
	}
	for ordinal := range replicas {
		var pvc corev1.PersistentVolumeClaim
		name := fmt.Sprintf("%s-%s-%d", definition.Name, r.statefulSet.Name, ordinal)
		if err := r.reader.Get(ctx, types.NamespacedName{Namespace: r.ledger.Namespace, Name: name}, &pvc); err != nil {
			setStorageCondition(r.ledger, definition.Name, metav1.ConditionFalse, "PVCGroupNotReady", err.Error())

			return true
		}
		if err := r.validateCurrentPVC(ctx, definition, &pvc); err != nil {
			setStorageCondition(r.ledger, definition.Name, metav1.ConditionFalse, "StorageUnsupported", err.Error())

			return true
		}
		if err := validatePVCMigration(definition, []*corev1.PersistentVolumeClaim{&pvc}); err != nil {
			setStorageCondition(r.ledger, definition.Name, metav1.ConditionFalse, "StorageUnsupported", err.Error())

			return true
		}
		ready, pvcFailed, observation := pvcStorageObservation(&pvc, floor, definition.Spec.VolumeAttributesClassName)
		observations = append(observations, observation)
		pending, failed = pending || !ready, failed || pvcFailed
	}
	reason, status := "Ready", metav1.ConditionTrue
	if failed {
		reason, status = "StorageUpdateFailed", metav1.ConditionFalse
	} else if pending {
		reason, status = "StorageUpdatePending", metav1.ConditionFalse
	}
	setStorageCondition(r.ledger, definition.Name, status, reason, strings.Join(observations, "; "))

	return pending || failed
}

func (r *persistenceStorageReconciler) validateVolumeAttributesClass(ctx context.Context, pvc *corev1.PersistentVolumeClaim, name string) error {
	driver, err := r.volumeAttributesDriver(ctx, name)
	if err != nil {
		return fmt.Errorf("getting VolumeAttributesClass %s (requires the Kubernetes API and CSI capability): %w", name, err)
	}
	if pvc.Status.Phase != corev1.ClaimBound || pvc.Spec.VolumeName == "" {
		return fmt.Errorf("PVC %s must be bound before changing volumeAttributesClassName", pvc.Name)
	}
	var pv corev1.PersistentVolume
	if err := r.reader.Get(ctx, types.NamespacedName{Name: pvc.Spec.VolumeName}, &pv); err != nil {
		return fmt.Errorf("getting PVC %s bound PV: %w", pvc.Name, err)
	}
	ref := pv.Spec.ClaimRef
	if ref == nil || ref.Name != pvc.Name || ref.Namespace != pvc.Namespace || ref.UID != pvc.UID {
		return fmt.Errorf("PV %s does not bind PVC %s UID %s", pv.Name, pvc.Name, pvc.UID)
	}
	if pv.Spec.CSI == nil || pv.Spec.CSI.Driver == "" || pv.Spec.CSI.Driver != driver {
		return fmt.Errorf("VolumeAttributesClass %s driver %q is incompatible with PVC %s bound CSI volume", name, driver, pvc.Name)
	}
	// CSIDriver has no ControllerModifyVolume support flag. Admission errors
	// and the PVC's ModifyVolumeStatus are the evidence of runtime support.
	return nil
}

func (r *persistenceStorageReconciler) volumeAttributesDriver(ctx context.Context, name string) (string, error) {
	// Prefer the stable API. The pinned client predates its typed VAC, so an
	// unstructured read keeps compatibility with clusters that no longer serve
	// v1beta1 without requiring a Kubernetes dependency upgrade.
	class := &unstructured.Unstructured{}
	class.SetGroupVersionKind(storagev1.SchemeGroupVersion.WithKind("VolumeAttributesClass"))
	err := r.reader.Get(ctx, types.NamespacedName{Name: name}, class)
	if err == nil {
		driver, found, err := unstructured.NestedString(class.Object, "driverName")
		if err != nil {
			return "", err
		}
		if !found || driver == "" {
			return "", errors.New("VolumeAttributesClass has no driverName")
		}

		return driver, nil
	}
	// Authentication/authorization and transient failures remain visible;
	// only an absent API/resource can select the older served version.
	if !meta.IsNoMatchError(err) && !k8serrors.IsNotFound(err) {
		return "", err
	}
	var beta storagev1beta1.VolumeAttributesClass
	if err := r.reader.Get(ctx, types.NamespacedName{Name: name}, &beta); err != nil {
		return "", err
	}

	return beta.DriverName, nil
}

func podReferencesPVC(pod *corev1.Pod, volume, claim string) bool {
	return slices.ContainsFunc(pod.Spec.Volumes, func(v corev1.Volume) bool {
		return v.Name == volume && v.PersistentVolumeClaim != nil && v.PersistentVolumeClaim.ClaimName == claim
	})
}

func storageClassValue(value *string) string {
	if value == nil {
		return ""
	}

	return *value
}

func setStorageCondition(ledger *ledgerv1alpha1.Cluster, volume string, status metav1.ConditionStatus, reason, message string) {
	conditionType := dataStorageCondition
	if volume == "wal" {
		conditionType = walStorageCondition
	}
	meta.SetStatusCondition(&ledger.Status.Conditions, metav1.Condition{
		Type: conditionType, Status: status, Reason: reason, Message: message, ObservedGeneration: ledger.Generation,
	})
}

func pvcStorageObservation(pvc *corev1.PersistentVolumeClaim, floor resource.Quantity, class string) (ready, failed bool, observation string) {
	request := pvc.Spec.Resources.Requests[corev1.ResourceStorage]
	capacity := pvc.Status.Capacity[corev1.ResourceStorage]
	observation = fmt.Sprintf("PVC %s: requested=%s capacity=%s minimum=%s requestedClass=%q currentClass=%q", pvc.Name, request.String(), capacity.String(), floor.String(), storageClassValue(pvc.Spec.VolumeAttributesClassName), storageClassValue(pvc.Status.CurrentVolumeAttributesClassName))
	ready = pvc.Status.Phase == corev1.ClaimBound && pvc.Spec.VolumeName != "" && request.Cmp(floor) >= 0 && capacity.Cmp(request) >= 0 && storageClassValue(pvc.Spec.VolumeAttributesClassName) == class && storageClassValue(pvc.Status.CurrentVolumeAttributesClassName) == class
	if modify := pvc.Status.ModifyVolumeStatus; modify != nil {
		observation += fmt.Sprintf(" modifyTarget=%q modifyStatus=%q", modify.TargetVolumeAttributesClassName, modify.Status)
		ready = false
		// Kubernetes clears ModifyVolumeStatus after success. Unknown future
		// statuses cannot establish successful application either.
		failed = modify.Status == corev1.PersistentVolumeClaimModifyVolumeInfeasible ||
			(modify.Status != corev1.PersistentVolumeClaimModifyVolumePending && modify.Status != corev1.PersistentVolumeClaimModifyVolumeInProgress)
	}
	if status := pvc.Status.AllocatedResourceStatuses[corev1.ResourceStorage]; status != "" {
		ready = false
		failed = failed || status == corev1.PersistentVolumeClaimControllerResizeInfeasible || status == corev1.PersistentVolumeClaimNodeResizeInfeasible
		observation += fmt.Sprintf(" resizeStatus=%q", status)
	}
	var observationSb397 strings.Builder
	for _, condition := range pvc.Status.Conditions {
		if condition.Status != corev1.ConditionTrue {
			continue
		}
		switch condition.Type {
		case corev1.PersistentVolumeClaimControllerResizeError, corev1.PersistentVolumeClaimNodeResizeError, corev1.PersistentVolumeClaimVolumeModifyVolumeError:
			failed = true
			ready = false
		case corev1.PersistentVolumeClaimResizing, corev1.PersistentVolumeClaimFileSystemResizePending, corev1.PersistentVolumeClaimVolumeModifyingVolume:
			ready = false
		default:
			continue
		}
		fmt.Fprintf(&observationSb397, " %s=%s: %s", condition.Type, condition.Reason, condition.Message)
	}
	observation += observationSb397.String()

	return ready, failed, observation
}
