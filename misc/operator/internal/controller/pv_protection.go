package controller

import (
	"context"
	"encoding/json"
	"fmt"

	kerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// reconcileVolumeProtection brings the deletion-protection label on this
// LedgerService's PVCs and their bound PVs in line with the desired protect
// state (spec.persistence.deletionProtection). When protect is true the
// `ledger.formance.com/deletion-protection: enabled` label is stamped so the
// volume deletion-protection ValidatingAdmissionPolicyBinding selects the
// volumes; when protect is false the label is removed and protection is lifted.
//
// It is a full reconcile, not an idempotent add: flipping deletionProtection
// off must actually unselect the volumes. The work is eventually consistent —
// PVCs that are absent or not yet bound (empty spec.VolumeName) are skipped and
// picked up on a later reconcile once the volume binds.
//
// PVs are cluster-scoped and do not inherit PVC labels, so both sides are
// stamped explicitly. A PV is only touched when its spec.claimRef still points
// at the PVC we resolved it through, so a PV rebound to a different claim is
// never (mis)labeled. Labels are applied with a JSON merge patch (a null value
// removes the key) to avoid clobbering concurrent writes from CSI / the PV
// controller and to sidestep update conflicts.
//
// Note: a PV orphaned after PVC/CR deletion (Released phase) keeps whatever
// label it last carried, because the reconcile only walks live PVCs. That is
// harmless: the PV policy guards Bound PVs only, so a Released orphan is not
// protected regardless of the label, and the reclaim path proceeds.
//
// It returns pending=true when at least one desired PVC is not yet created
// (either direction — a new PVC must be stamped when protecting, or unstamped
// when not, since a scale-out PVC born from a still-labeled immutable VCT after
// an opt-out would otherwise stay protected), or when a PVC is not yet bound and
// protection is on so its PV is still to stamp once it binds. The caller requeues
// on that signal — the controller does not watch PVC/PV binding events, so this
// is what makes reconciliation to the desired label state deterministic.
func reconcileVolumeProtection(ctx context.Context, clientset kubernetes.Interface,
	namespace, stsName string, replicas int32, volumeNames []string, protect bool,
) (bool, error) {
	logger := log.FromContext(ctx)
	pvcClient := clientset.CoreV1().PersistentVolumeClaims(namespace)
	pvClient := clientset.CoreV1().PersistentVolumes()

	pending := false
	for ordinal := range replicas {
		for _, vol := range volumeNames {
			pvcName := fmt.Sprintf("%s-%s-%d", vol, stsName, ordinal)
			pvc, err := pvcClient.Get(ctx, pvcName, metav1.GetOptions{})
			if err != nil {
				if kerrors.IsNotFound(err) {
					// Expected to be created by the StatefulSet controller. Requeue
					// regardless of direction so the PVC is reconciled to the desired
					// label state once it appears: when protecting, the new PVC (and
					// its PV) must be stamped; when not, a PVC born from a still-labeled
					// (immutable) VCT after an opt-out scale-out must be unstamped, or it
					// stays selected by the policy and its deletion is wrongly blocked.
					pending = true

					continue
				}

				return false, fmt.Errorf("getting PVC %s: %w", pvcName, err)
			}

			patch, err := protectionLabelPatch(pvc.Labels, protect)
			if err != nil {
				return false, fmt.Errorf("building deletion-protection patch for PVC %s: %w", pvcName, err)
			}
			if patch != nil {
				if _, err := pvcClient.Patch(ctx, pvcName, types.MergePatchType, patch, metav1.PatchOptions{}); err != nil {
					return false, fmt.Errorf("patching deletion-protection label on PVC %s: %w", pvcName, err)
				}
				logger.Info("reconciled deletion-protection label on PVC", "pvc", pvcName, "protect", protect)
			}

			if pvc.Spec.VolumeName == "" {
				// Not yet bound: the PV to stamp does not exist yet. Signal pending
				// so the caller requeues until it binds (only matters when we want
				// the eventual PV protected).
				pending = pending || protect

				continue
			}

			pv, err := pvClient.Get(ctx, pvc.Spec.VolumeName, metav1.GetOptions{})
			if err != nil {
				if kerrors.IsNotFound(err) {
					continue
				}

				return false, fmt.Errorf("getting PV %s: %w", pvc.Spec.VolumeName, err)
			}

			// Only touch a PV that is still claimed by this PVC; a PV rebound to
			// another claim must neither inherit nor lose our protection label.
			if ref := pv.Spec.ClaimRef; ref == nil || ref.Namespace != namespace || ref.Name != pvcName {
				continue
			}

			pvPatch, err := protectionLabelPatch(pv.Labels, protect)
			if err != nil {
				return false, fmt.Errorf("building deletion-protection patch for PV %s: %w", pv.Name, err)
			}
			if pvPatch != nil {
				if _, err := pvClient.Patch(ctx, pv.Name, types.MergePatchType, pvPatch, metav1.PatchOptions{}); err != nil {
					return false, fmt.Errorf("patching deletion-protection label on PV %s: %w", pv.Name, err)
				}
				logger.Info("reconciled deletion-protection label on bound PV", "pv", pv.Name, "pvc", pvcName, "protect", protect)
			}
		}
	}

	return pending, nil
}

// protectionLabelPatch returns a JSON merge patch that adds (protect) or removes
// (!protect) the deletion-protection label, or nil when the current labels are
// already in the desired state. A null value in a merge patch deletes the key.
func protectionLabelPatch(current map[string]string, protect bool) ([]byte, error) {
	hasDesiredValue := current[labelDeletionProtection] == labelDeletionProtectionValue
	_, present := current[labelDeletionProtection]

	var value any
	switch {
	case protect && hasDesiredValue:
		return nil, nil // already stamped
	case !protect && !present:
		return nil, nil // already absent
	case protect:
		value = labelDeletionProtectionValue
	default:
		value = nil // remove the key
	}

	patch, err := json.Marshal(map[string]any{
		"metadata": map[string]any{
			"labels": map[string]any{labelDeletionProtection: value},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("marshaling deletion-protection label patch: %w", err)
	}

	return patch, nil
}
