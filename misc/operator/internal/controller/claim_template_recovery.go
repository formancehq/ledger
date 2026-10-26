package controller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
)

const (
	claimTemplateRecoveryKey        = "statefulset.json"
	claimTemplateRecoveryAnnotation = "ledger.formance.com/storage-recovery"
	claimTemplateRequeueInterval    = 2 * time.Second
)

// claimTemplateRecovery is committed before orphan deletion. The replacement
// carries the old workload verbatim: a storage edit must not also change Raft
// membership, TLS, selectors, restart annotations, or the running image.
type claimTemplateRecovery struct {
	Version          int                     `json:"version"`
	SourceUID        types.UID               `json:"sourceUID"`
	SourceGeneration int64                   `json:"sourceGeneration"`
	StatefulSet      appsv1.StatefulSet      `json:"statefulSet"`
	Claims           []retainedClaimIdentity `json:"claims"`
}

type retainedClaimIdentity struct {
	Name       string    `json:"name"`
	UID        types.UID `json:"uid"`
	VolumeName string    `json:"volumeName"`
	PVUID      types.UID `json:"pvUID"`
}

func setClaimTemplatesCondition(ledger *ledgerv1alpha1.Cluster, ready bool, reason, message string) {
	status := metav1.ConditionFalse
	if ready {
		status = metav1.ConditionTrue
	}
	meta.SetStatusCondition(&ledger.Status.Conditions, metav1.Condition{
		Type: "ClaimTemplatesReady", Status: status, Reason: reason,
		Message: message, ObservedGeneration: ledger.Generation,
	})
}

// alignedClaimTemplates changes only supported mutable PVC settings. A lower
// configured floor never lowers an existing template, and auto-expanded live
// claims never feed back into templates. StorageClass and access modes remain
// those of the original template; live PVC migration is validated separately.
func alignedClaimTemplates(existing, desired []corev1.PersistentVolumeClaim) ([]corev1.PersistentVolumeClaim, bool, error) {
	aligned := make([]corev1.PersistentVolumeClaim, 0, len(existing))
	changed := false
	for _, old := range existing {
		i := slices.IndexFunc(desired, func(p corev1.PersistentVolumeClaim) bool { return p.Name == old.Name })
		if i < 0 {
			return nil, false, fmt.Errorf("claim template %s is absent from desired storage layout", old.Name)
		}
		want := desired[i]
		pvc := *old.DeepCopy()
		oldSize, newSize := old.Spec.Resources.Requests[corev1.ResourceStorage], want.Spec.Resources.Requests[corev1.ResourceStorage]
		if newSize.Cmp(oldSize) > 0 {
			if pvc.Spec.Resources.Requests == nil {
				pvc.Spec.Resources.Requests = corev1.ResourceList{}
			}
			pvc.Spec.Resources.Requests[corev1.ResourceStorage] = newSize.DeepCopy()
			changed = true
		}
		oldClass, newClass := "", ""
		if old.Spec.VolumeAttributesClassName != nil {
			oldClass = *old.Spec.VolumeAttributesClassName
		}
		if want.Spec.VolumeAttributesClassName != nil {
			newClass = *want.Spec.VolumeAttributesClassName
		}
		if oldClass != newClass {
			if newClass == "" {
				return nil, false, fmt.Errorf("%s: clearing volumeAttributesClassName is unsupported; select an explicit class", old.Name)
			}
			pvc.Spec.VolumeAttributesClassName = new(newClass)
			changed = true
		}
		aligned = append(aligned, pvc)
	}

	return aligned, changed, nil
}

func claimTemplateRecoveryDigest(ledger *ledgerv1alpha1.Cluster, payload []byte) string {
	domain := "ledger.formance.com/claim-template-recovery/v1\x00" + string(ledger.UID) + "\x00" + ledger.Namespace + "\x00" + resourceName(ledger.Name) + "-storage-recovery\x00"
	digest := sha256.Sum256(append([]byte(domain), payload...))

	return hex.EncodeToString(digest[:])
}

func claimTemplateRecoveryDigestName(ledger *ledgerv1alpha1.Cluster, digest string) string {
	return resourceName(ledger.Name) + "-storage-recovery-" + digest[:32]
}

func validateClaimTemplateRecoveryPointer(ledger *ledgerv1alpha1.Cluster, pointer *ledgerv1alpha1.ClaimTemplateRecoveryStatus) error {
	if pointer == nil || pointer.SourceUID == "" || len(pointer.PayloadSHA256) != sha256.Size*2 {
		return errors.New("invalid storage recovery authorization pointer")
	}
	if _, err := hex.DecodeString(pointer.PayloadSHA256); err != nil || pointer.ConfigMapName != claimTemplateRecoveryDigestName(ledger, pointer.PayloadSHA256) {
		return errors.New("invalid storage recovery digest or journal name")
	}
	if pointer.Completed && pointer.ConfigMapUID == "" {
		return errors.New("completed storage recovery has no bound journal UID")
	}

	return nil
}

func authenticateClaimTemplateRecoveryJournal(ledger *ledgerv1alpha1.Cluster, pointer *ledgerv1alpha1.ClaimTemplateRecoveryStatus, cm *corev1.ConfigMap) ([]byte, error) {
	if cm.Name != pointer.ConfigMapName || cm.Namespace != ledger.Namespace || !metav1.IsControlledBy(cm, ledger) {
		return nil, errors.New("storage recovery journal name or owner differs from authorized pointer")
	}
	if cm.UID == "" || (pointer.ConfigMapUID != "" && pointer.ConfigMapUID != string(cm.UID)) {
		return nil, errors.New("storage recovery ConfigMap identity differs from committed pointer")
	}
	if !cm.DeletionTimestamp.IsZero() || cm.Immutable == nil || !*cm.Immutable {
		return nil, errors.New("storage recovery journal is deleting or not immutable")
	}
	payload := []byte(cm.Data[claimTemplateRecoveryKey])
	if claimTemplateRecoveryDigest(ledger, payload) != pointer.PayloadSHA256 || len(cm.Data) != 1 || len(cm.BinaryData) != 0 {
		return nil, errors.New("storage recovery journal payload differs from authorized digest")
	}

	return payload, nil
}

// claimTemplateJournalWriteError marks an attempted durable intent/journal write.
// Even a subsequent NotFound cannot exclude a delayed server-side commit.
type claimTemplateJournalWriteError struct{ cause error }

func (e *claimTemplateJournalWriteError) Error() string { return e.cause.Error() }
func (e *claimTemplateJournalWriteError) Unwrap() error { return e.cause }

func (r *ClusterReconciler) startClaimTemplateRecovery(ctx context.Context, ledger *ledgerv1alpha1.Cluster, sts *appsv1.StatefulSet, claims []corev1.PersistentVolumeClaim) (result ctrl.Result, reconcileErr error) {
	journalWriteAttempted := false
	recoveryHandedOff := false
	payloadDigest := ""
	defer func() {
		if reconcileErr != nil {
			if _, waiting := errors.AsType[*claimTemplateRecoveryRetryWait](reconcileErr); waiting {
				return
			}
			// persistClaimTemplateRecovery records its own attempted-write
			// failures, including a response lost after the API committed.
			_, recorded := errors.AsType[*claimTemplateJournalWriteError](reconcileErr)
			if journalWriteAttempted && !recorded {
				r.claimTemplateRetries.failed(ledger, payloadDigest, time.Now())
				reconcileErr = &claimTemplateJournalWriteError{cause: reconcileErr}
			}
			if !recoveryHandedOff {
				setClaimTemplatesCondition(ledger, false, "PreparationFailed", reconcileErr.Error())
			}
		} else {
			r.claimTemplateRetries.succeeded(ledger, payloadDigest)
		}
	}()
	sts = sts.DeepCopy()
	if !metav1.IsControlledBy(sts, ledger) || sts.UID == "" || sts.Spec.Replicas == nil || *sts.Spec.Replicas < 1 || !sts.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, fmt.Errorf("StatefulSet %s is not controlled by Cluster %s", sts.Name, ledger.Name)
	}
	live := &appsv1.StatefulSet{}
	if err := r.storageAPIReader().Get(ctx, client.ObjectKeyFromObject(sts), live); err != nil {
		return ctrl.Result{}, fmt.Errorf("reading source StatefulSet before storage preparation: %w", err)
	}
	if live.UID != sts.UID || live.Generation != sts.Generation || !live.DeletionTimestamp.IsZero() || !metav1.IsControlledBy(live, ledger) || !apiequality.Semantic.DeepEqual(live.Spec, sts.Spec) || !rolloutConverged(live) {
		return ctrl.Result{}, errors.New("source StatefulSet changed or is not stable before storage preparation")
	}
	recovery := claimTemplateRecovery{Version: 1, SourceUID: sts.UID, SourceGeneration: sts.Generation}
	recovery.StatefulSet = appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: sts.Name, Namespace: sts.Namespace, Labels: maps.Clone(sts.Labels)},
		Spec:       *sts.Spec.DeepCopy(),
	}
	recovery.StatefulSet.Annotations = cloneAnnotations(sts.Annotations)
	recovery.StatefulSet.Annotations[claimTemplateRecoveryAnnotation] = string(sts.UID)
	recovery.StatefulSet.Spec.VolumeClaimTemplates = make([]corev1.PersistentVolumeClaim, len(claims))
	for i := range claims {
		recovery.StatefulSet.Spec.VolumeClaimTemplates[i] = *claims[i].DeepCopy()
	}
	if err := controllerutil.SetControllerReference(ledger, &recovery.StatefulSet, r.Scheme); err != nil {
		return ctrl.Result{}, err
	}
	// Prove the retained claims through Pods owned by this exact source STS.
	for ordinal := range *sts.Spec.Replicas {
		pod := &corev1.Pod{}
		key := types.NamespacedName{Namespace: sts.Namespace, Name: fmt.Sprintf("%s-%d", sts.Name, ordinal)}
		if err := r.storageAPIReader().Get(ctx, key, pod); err != nil {
			return ctrl.Result{}, fmt.Errorf("reading retained Pod: %w", err)
		}
		if !metav1.IsControlledBy(pod, sts) || !pod.DeletionTimestamp.IsZero() {
			return ctrl.Result{}, fmt.Errorf("pod %s is not a stable dependent of StatefulSet %s", pod.Name, sts.Name)
		}
		for _, claim := range sts.Spec.VolumeClaimTemplates {
			name := fmt.Sprintf("%s-%s", claim.Name, pod.Name)
			if !slices.ContainsFunc(pod.Spec.Volumes, func(v corev1.Volume) bool {
				return v.Name == claim.Name && v.PersistentVolumeClaim != nil && v.PersistentVolumeClaim.ClaimName == name
			}) {
				return ctrl.Result{}, fmt.Errorf("pod %s does not reference retained claim %s", pod.Name, name)
			}
			pvc := &corev1.PersistentVolumeClaim{}
			if err := r.storageAPIReader().Get(ctx, types.NamespacedName{Namespace: sts.Namespace, Name: name}, pvc); err != nil {
				return ctrl.Result{}, err
			}
			if pvc.UID == "" || pvc.Spec.VolumeName == "" || pvc.Status.Phase != corev1.ClaimBound || !pvc.DeletionTimestamp.IsZero() {
				return ctrl.Result{}, fmt.Errorf("claim %s is not stably bound", name)
			}
			storage := persistenceStorageReconciler{reader: r.storageAPIReader(), ledger: ledger, statefulSet: sts}
			if err := storage.validatePVCIdentity(ctx, claim.Name, pvc); err != nil {
				return ctrl.Result{}, fmt.Errorf("validating retained claim owner: %w", err)
			}
			pv := &corev1.PersistentVolume{}
			if err := r.storageAPIReader().Get(ctx, types.NamespacedName{Name: pvc.Spec.VolumeName}, pv); err != nil {
				return ctrl.Result{}, err
			}
			if pv.UID == "" || pv.Spec.ClaimRef == nil || pv.Spec.ClaimRef.UID != pvc.UID || pv.Spec.ClaimRef.Name != pvc.Name || pv.Spec.ClaimRef.Namespace != pvc.Namespace || !pv.DeletionTimestamp.IsZero() {
				return ctrl.Result{}, fmt.Errorf("PV %s does not retain the expected claim binding", pv.Name)
			}
			recovery.Claims = append(recovery.Claims, retainedClaimIdentity{Name: name, UID: pvc.UID, VolumeName: pvc.Spec.VolumeName, PVUID: pv.UID})
		}
	}
	preview := recovery.StatefulSet.DeepCopy()
	preview.Name = ""
	preview.GenerateName = sts.Name + "-storage-preview-"
	if err := r.Create(ctx, preview, client.DryRunAll); err != nil {
		return ctrl.Result{}, fmt.Errorf("checking claim-template API support before orphan deletion: %w", err)
	}
	if !apiequality.Semantic.DeepEqual(preview.Spec, recovery.StatefulSet.Spec) {
		return ctrl.Result{}, errors.New("kubernetes did not preserve the requested replacement StatefulSet spec in admission; check API feature support (including VolumeAttributesClass) and mutating admission webhooks")
	}
	recordPodConfigHash(&recovery.StatefulSet, computeSpecHash(&ledger.Spec))
	data, err := json.Marshal(recovery)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("encoding storage recovery state: %w", err)
	}
	digest := claimTemplateRecoveryDigest(ledger, data)
	payloadDigest = digest
	if after := r.claimTemplateRetries.wait(ledger, digest, time.Now()); after > 0 {
		return ctrl.Result{RequeueAfter: after}, &claimTemplateRecoveryRetryWait{after: after}
	}
	pointer := &ledgerv1alpha1.ClaimTemplateRecoveryStatus{
		SourceUID: string(sts.UID), PayloadSHA256: digest, ConfigMapName: claimTemplateRecoveryDigestName(ledger, digest),
	}
	cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: pointer.ConfigMapName, Namespace: ledger.Namespace}, Immutable: new(true), Data: map[string]string{claimTemplateRecoveryKey: string(data)}}
	if err := controllerutil.SetControllerReference(ledger, cm, r.Scheme); err != nil {
		return ctrl.Result{}, err
	}
	setClaimTemplatesCondition(ledger, false, "Preparing", "Verifying retained storage before recording a StatefulSet replacement")
	if err := r.persistClaimTemplateRecovery(ctx, ledger, nil, pointer); err != nil {
		return ctrl.Result{}, err
	}
	journalWriteAttempted = true
	if err := r.Create(ctx, cm); err != nil && !apierrors.IsAlreadyExists(err) {
		return ctrl.Result{}, fmt.Errorf("persisting storage recovery state: %w", err)
	}
	setClaimTemplatesCondition(ledger, false, "Recreating", "Storage recovery state committed; retaining Pods and claims while replacing the StatefulSet")
	// Resume owns the phase condition from here, including errors. Keep the
	// durable-write fence and retry accounting without relabeling its failure.
	recoveryHandedOff = true
	_, result, err = r.resumeClaimTemplateRecovery(ctx, ledger)

	return result, err
}

func cloneAnnotations(source map[string]string) map[string]string {
	result := maps.Clone(source)
	if result == nil {
		result = make(map[string]string)
	}

	return result
}

func (r *ClusterReconciler) storageAPIReader() client.Reader {
	if r.APIReader != nil {
		return r.APIReader
	}

	return r.Client
}

// persistClaimTemplateRecovery compares the caller's captured whole pointer. A
// fresh resourceVersion alone must never authorize replacing a newer operation.
func (r *ClusterReconciler) persistClaimTemplateRecovery(ctx context.Context, ledger *ledgerv1alpha1.Cluster, expected, recovery *ledgerv1alpha1.ClaimTemplateRecoveryStatus) (persistErr error) {
	writeAttempted := false
	defer func() {
		if persistErr != nil && writeAttempted {
			pointer := recovery
			if pointer == nil {
				pointer = expected
			}
			if pointer != nil {
				r.claimTemplateRetries.failed(ledger, pointer.PayloadSHA256, time.Now())
			}
			persistErr = &claimTemplateJournalWriteError{cause: persistErr}
		}
	}()
	latest := &ledgerv1alpha1.Cluster{}
	if err := r.storageAPIReader().Get(ctx, client.ObjectKeyFromObject(ledger), latest); err != nil {
		return err
	}
	if latest.UID != ledger.UID || !latest.DeletionTimestamp.IsZero() {
		return errors.New("cluster identity changed or is deleting during storage recovery")
	}
	if !apiequality.Semantic.DeepEqual(latest.Status.ClaimTemplateRecovery, expected) {
		return errors.New("storage recovery pointer changed before status transition")
	}
	if expected == nil && recovery != nil && latest.Generation != ledger.Generation {
		return errors.New("cluster generation changed during storage preparation")
	}
	base := latest.DeepCopy()
	latest.Status.ClaimTemplateRecovery = recovery.DeepCopy()
	writeAttempted = true
	if err := r.Status().Patch(ctx, latest, client.MergeFromWithOptions(base, client.MergeFromWithOptimisticLock{})); err != nil {
		return fmt.Errorf("persisting claim-template recovery pointer: %w", err)
	}
	if !apiequality.Semantic.DeepEqual(latest.Status.ClaimTemplateRecovery, recovery) {
		return errors.New("kubernetes did not persist the claim-template recovery pointer; install the updated Cluster CRD before continuing")
	}
	ledger.Status.ClaimTemplateRecovery = recovery.DeepCopy()
	ledger.ResourceVersion = latest.ResourceVersion

	return nil
}

// resumeClaimTemplateRecovery runs before desired-spec validation. Once orphan
// deletion can have happened, even an invalid or reversed spec must not prevent
// restoring the saved workload. New desired changes wait until adoption ends.
func (r *ClusterReconciler) resumeClaimTemplateRecovery(ctx context.Context, ledger *ledgerv1alpha1.Cluster) (handled bool, result ctrl.Result, reconcileErr error) {
	// The initial uncached refresh can fail before journal authentication or
	// binding. It is still a recovery error once preparation has handed off.
	defer func() {
		if reconcileErr != nil {
			setClaimTemplatesCondition(ledger, false, "RecoveryFailed", reconcileErr.Error())
		}
	}()
	reader := r.storageAPIReader()
	if r.APIReader != nil {
		latest := &ledgerv1alpha1.Cluster{}
		if err := reader.Get(ctx, client.ObjectKeyFromObject(ledger), latest); err != nil {
			return true, ctrl.Result{}, err
		}
		if latest.UID != ledger.UID || !latest.DeletionTimestamp.IsZero() {
			return true, ctrl.Result{}, errors.New("cluster identity changed or is deleting during storage recovery")
		}
		ledger.Status.ClaimTemplateRecovery = latest.Status.ClaimTemplateRecovery.DeepCopy()
	}
	// A standalone ConfigMap has no workload authority, even if it carries a
	// Cluster owner reference and a plausible payload. Only status authorizes it.
	pointer := ledger.Status.ClaimTemplateRecovery.DeepCopy()
	if pointer == nil {
		return false, ctrl.Result{}, nil
	}
	handled = true
	result.RequeueAfter = claimTemplateRequeueInterval
	if err := validateClaimTemplateRecoveryPointer(ledger, pointer); err != nil {
		return handled, result, err
	}
	cm := &corev1.ConfigMap{}
	if err := reader.Get(ctx, types.NamespacedName{Namespace: ledger.Namespace, Name: pointer.ConfigMapName}, cm); err != nil {
		if !apierrors.IsNotFound(err) {
			return handled, result, err
		}
		if pointer.Completed {
			err := r.persistClaimTemplateRecovery(ctx, ledger, pointer, nil)
			if err == nil {
				r.claimTemplateRetries.succeeded(ledger, pointer.PayloadSHA256)
			}

			return handled, result, err
		}
		if pointer.ConfigMapUID == "" {
			// Abandon only a journal-less Preparing phase whose original source
			// still exists safely. No old claims or dependencies are read here.
			if r.APIReader == nil {
				return handled, result, errors.New("uncached API reader is required to abandon storage preparation")
			}
			source := &appsv1.StatefulSet{}
			if err := r.APIReader.Get(ctx, types.NamespacedName{Namespace: ledger.Namespace, Name: resourceName(ledger.Name)}, source); err != nil {
				return handled, result, fmt.Errorf("checking source before abandoning storage preparation: %w", err)
			}
			if string(source.UID) != pointer.SourceUID || !source.DeletionTimestamp.IsZero() || !metav1.IsControlledBy(source, ledger) {
				return handled, result, errors.New("source StatefulSet identity changed or is deleting before abandoning storage preparation")
			}

			return handled, result, r.persistClaimTemplateRecovery(ctx, ledger, pointer, nil)
		}

		return handled, result, fmt.Errorf("active storage recovery journal %s is missing", pointer.ConfigMapName)
	}
	if !metav1.IsControlledBy(cm, ledger) {
		return handled, result, fmt.Errorf("storage recovery ConfigMap %s has a foreign owner", cm.Name)
	}
	if cm.UID == "" || (pointer.ConfigMapUID != "" && pointer.ConfigMapUID != string(cm.UID)) {
		return handled, result, errors.New("storage recovery ConfigMap identity differs from committed pointer")
	}
	if pointer.Completed {
		// Cleanup needs only the bound identity. Corrupted payload bytes must
		// not strand an already completed operation.
		if err := r.Delete(ctx, cm, client.Preconditions{UID: &cm.UID, ResourceVersion: &cm.ResourceVersion}); err != nil {
			r.claimTemplateRetries.failed(ledger, pointer.PayloadSHA256, time.Now())

			return handled, result, err
		}

		err := r.persistClaimTemplateRecovery(ctx, ledger, pointer, nil)
		if err == nil {
			r.claimTemplateRetries.succeeded(ledger, pointer.PayloadSHA256)
		}

		return handled, result, err
	}
	payload, err := authenticateClaimTemplateRecoveryJournal(ledger, pointer, cm)
	if err != nil {
		return handled, result, err
	}
	var recovery claimTemplateRecovery
	if err := json.Unmarshal(payload, &recovery); err != nil {
		return handled, result, fmt.Errorf("decoding storage recovery state: %w", err)
	}
	if string(recovery.SourceUID) != pointer.SourceUID {
		return handled, result, errors.New("storage recovery source UID differs from authorized pointer")
	}
	sts := &recovery.StatefulSet
	if recovery.Version != 1 || recovery.SourceUID == "" || sts.Name != resourceName(ledger.Name) || sts.Namespace != ledger.Namespace || !metav1.IsControlledBy(sts, ledger) || sts.Spec.Replicas == nil || *sts.Spec.Replicas < 1 {
		return handled, result, errors.New("invalid StatefulSet identity in storage recovery state")
	}
	if len(recovery.Claims) != int(*sts.Spec.Replicas)*len(sts.Spec.VolumeClaimTemplates) {
		return handled, result, errors.New("incomplete retained claim identities in storage recovery state")
	}
	seen := make(map[string]bool, len(recovery.Claims))
	for _, retained := range recovery.Claims {
		if seen[retained.Name] || retained.UID == "" || retained.VolumeName == "" || retained.PVUID == "" {
			return handled, result, errors.New("invalid retained claim identity in storage recovery state")
		}
		seen[retained.Name] = true
	}
	for ordinal := range *sts.Spec.Replicas {
		for _, template := range sts.Spec.VolumeClaimTemplates {
			if !seen[fmt.Sprintf("%s-%s-%d", template.Name, sts.Name, ordinal)] {
				return handled, result, fmt.Errorf("missing retained claim identity for %s ordinal %d", template.Name, ordinal)
			}
		}
	}
	if pointer.ConfigMapUID == "" {
		active := pointer.DeepCopy()
		active.ConfigMapUID = string(cm.UID)
		if err := r.persistClaimTemplateRecovery(ctx, ledger, pointer, active); err != nil {
			return handled, result, err
		}
		pointer = active
		// Recheck the journal after binding. Its prior observation must not
		// authorize workload writes if it disappeared during the status write.
		cm = &corev1.ConfigMap{}
		if err := reader.Get(ctx, types.NamespacedName{Namespace: ledger.Namespace, Name: pointer.ConfigMapName}, cm); err != nil {
			return handled, result, fmt.Errorf("rereading bound storage recovery journal: %w", err)
		}
		if _, err := authenticateClaimTemplateRecoveryJournal(ledger, pointer, cm); err != nil {
			return handled, result, err
		}
	}
	// Refresh independent dependencies from the trusted saved workload even if
	// live identity checks fail below. A dependency failure must not prevent
	// restoring an orphaned workload from its journal.
	dependencyErr := r.reconcileStorageRecoveryDependencies(ctx, ledger, sts, recovery.Claims)
	defer func() {
		reconcileErr = errors.Join(reconcileErr, dependencyErr)
	}()
	// A still-present source can have removed old ordinals or become unready
	// before orphan deletion. Complete its stale plan before reading obsolete
	// claims, so normal workload reconciliation can repair that source. Once
	// deletion starts or a replacement exists, the saved workload takes priority.
	live := &appsv1.StatefulSet{}
	err = reader.Get(ctx, client.ObjectKeyFromObject(sts), live)
	if err != nil && !apierrors.IsNotFound(err) {
		return handled, result, err
	}
	if err == nil && live.UID == recovery.SourceUID && live.DeletionTimestamp.IsZero() {
		if !metav1.IsControlledBy(live, ledger) {
			return handled, result, errors.New("source StatefulSet has a foreign owner during storage recovery")
		}
		if live.Generation != recovery.SourceGeneration || !rolloutConverged(live) {
			completed := *pointer
			completed.Completed = true
			if err := r.persistClaimTemplateRecovery(ctx, ledger, pointer, &completed); err != nil {
				return handled, result, err
			}
			setClaimTemplatesCondition(ledger, false, "SourceChanged", "Source StatefulSet changed or became unready before deletion; retrying from current state")

			return handled, result, nil
		}
	}
	for _, retained := range recovery.Claims {
		claim := &corev1.PersistentVolumeClaim{}
		if err := reader.Get(ctx, types.NamespacedName{Namespace: ledger.Namespace, Name: retained.Name}, claim); err != nil {
			return handled, result, fmt.Errorf("reading retained claim %s: %w", retained.Name, err)
		}
		if claim.UID != retained.UID || claim.Spec.VolumeName != retained.VolumeName || !claim.DeletionTimestamp.IsZero() {
			return handled, result, fmt.Errorf("retained claim %s identity changed or is deleting", retained.Name)
		}
		pv := &corev1.PersistentVolume{}
		if err := reader.Get(ctx, types.NamespacedName{Name: retained.VolumeName}, pv); err != nil {
			return handled, result, err
		}
		if retained.PVUID == "" || pv.UID != retained.PVUID || pv.Spec.ClaimRef == nil || pv.Spec.ClaimRef.UID != retained.UID || pv.Spec.ClaimRef.Name != claim.Name || pv.Spec.ClaimRef.Namespace != claim.Namespace || !pv.DeletionTimestamp.IsZero() {
			return handled, result, fmt.Errorf("retained PV %s identity or binding changed", retained.VolumeName)
		}
	}
	if apierrors.IsNotFound(err) {
		if err := r.Create(ctx, sts); err != nil {
			return handled, result, fmt.Errorf("restoring StatefulSet from storage recovery state: %w", err)
		}
		setClaimTemplatesCondition(ledger, false, "AwaitingAdoption", "StatefulSet recreated; waiting for retained Pods to be adopted")

		return handled, result, nil
	}
	if err != nil {
		return handled, result, err
	}
	if !metav1.IsControlledBy(live, ledger) {
		return handled, result, fmt.Errorf("StatefulSet %s has a foreign owner during storage recovery", live.Name)
	}
	if !live.DeletionTimestamp.IsZero() {
		setClaimTemplatesCondition(ledger, false, "Recreating", "Waiting for orphan deletion; Pods and claims are retained")

		return handled, result, nil
	}
	if live.UID == recovery.SourceUID {
		orphan := metav1.DeletePropagationOrphan
		if err := r.Delete(ctx, live, &client.DeleteOptions{PropagationPolicy: &orphan, Preconditions: &metav1.Preconditions{UID: &live.UID, ResourceVersion: &live.ResourceVersion}}); err != nil {
			return handled, result, fmt.Errorf("orphaning StatefulSet for storage settings: %w", err)
		}
		setClaimTemplatesCondition(ledger, false, "Recreating", "StatefulSet orphan deletion requested; Pods and claims are retained")

		return handled, result, nil
	}
	if live.Annotations[claimTemplateRecoveryAnnotation] != string(recovery.SourceUID) || !apiequality.Semantic.DeepEqual(live.Spec, sts.Spec) {
		return handled, result, errors.New("replacement StatefulSet does not match committed storage recovery state")
	}
	// Storage recovery restores controller ownership, not workload availability.
	// Unready Pods must remain repairable through normal desired-spec reconciliation.
	if live.Status.ObservedGeneration < live.Generation {
		setClaimTemplatesCondition(ledger, false, "AwaitingAdoption", "Waiting for the StatefulSet controller to observe the replacement generation")

		return handled, result, nil
	}
	for ordinal := range *live.Spec.Replicas {
		pod := &corev1.Pod{}
		key := types.NamespacedName{Namespace: live.Namespace, Name: fmt.Sprintf("%s-%d", live.Name, ordinal)}
		if err := reader.Get(ctx, key, pod); err != nil {
			if apierrors.IsNotFound(err) {
				setClaimTemplatesCondition(ledger, false, "AwaitingAdoption", fmt.Sprintf("Waiting for expected Pod %s to exist and be adopted by the replacement StatefulSet", key.Name))

				return handled, result, nil
			}

			return handled, result, err
		}
		if !metav1.IsControlledBy(pod, live) || !pod.DeletionTimestamp.IsZero() {
			setClaimTemplatesCondition(ledger, false, "AwaitingAdoption", "Waiting for all retained Pods to be adopted by the replacement StatefulSet")

			return handled, result, nil
		}
	}
	completed := *pointer
	completed.Completed = true
	if err := r.persistClaimTemplateRecovery(ctx, ledger, pointer, &completed); err != nil {
		return handled, result, err
	}
	setClaimTemplatesCondition(ledger, true, "Ready", "Claim templates updated and retained Pods adopted; PVC operations have separate storage conditions")

	return handled, result, nil
}
