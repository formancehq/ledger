package controller

import (
	"context"
	"encoding/json"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// reconcilePVCExpansion grows this Cluster's existing PersistentVolumeClaims to
// match the desired volumeClaimTemplate sizes.
//
// StatefulSet volumeClaimTemplates are immutable, so a size bump in the Cluster
// spec never reaches the running volumes on its own: the normal update path
// leaves the template untouched (see reconcileStatefulSet), and even
// delete-recreating the StatefulSet only re-adopts the existing PVCs — the
// StatefulSet controller never resizes a PVC it already owns. The live disks are
// grown by patching each PVC's spec.resources.requests.storage upward, which
// triggers CSI online expansion (the StorageClass must have
// allowVolumeExpansion: true and the driver must support it).
//
// It is grow-only. A desired size smaller than a PVC's current request is
// rejected with a Warning event and the PVC is left untouched: Kubernetes does
// not support shrinking a PVC and would reject the patch anyway. A missing PVC
// (an ordinal not yet scaled out) is skipped; it is created from the refreshed
// template at the new size once it appears.
//
// It returns templateGrew=true when the desired template size exceeds the
// existing StatefulSet template size for any volume, signalling the caller to
// recreate the StatefulSet (with orphan propagation) so future scale-out
// ordinals inherit the new template size. A nil recorder/object skips eventing
// (unit tests without a recorder).
func reconcilePVCExpansion(
	ctx context.Context,
	clientset kubernetes.Interface,
	recorder record.EventRecorder,
	object runtime.Object,
	namespace, stsName string,
	replicas int32,
	existing, desired []corev1.PersistentVolumeClaim,
) (bool, error) {
	logger := log.FromContext(ctx)
	pvcClient := clientset.CoreV1().PersistentVolumeClaims(namespace)

	existingSizeByName := make(map[string]resource.Quantity, len(existing))
	for i := range existing {
		existingSizeByName[existing[i].Name] = *existing[i].Spec.Resources.Requests.Storage()
	}

	templateGrew := false
	for i := range desired {
		vct := &desired[i]
		desiredSize := vct.Spec.Resources.Requests.Storage()
		if desiredSize.IsZero() {
			continue
		}

		// Decide grow vs shrink from the *template* delta — the user's declared
		// intent for this volume — not from any individual PVC's current request.
		// A PVC manually expanded past the new template size is NOT a shrink
		// request: it must be left alone by the grow-only patch below, never
		// warned about. Comparing per-PVC would (wrongly) flag such a PVC as a
		// shrink on every reconcile. A size increase also needs a StatefulSet
		// recreate (templateGrew): the immutable VCT can only be re-emitted by
		// delete-recreate, and without it a later scale-out ordinal would be born
		// at the old size.
		if oldSize, ok := existingSizeByName[vct.Name]; ok {
			switch cmp := desiredSize.Cmp(oldSize); {
			case cmp < 0:
				// Spec-driven shrink intent: unsupported. Warn once for the volume
				// and leave its PVCs untouched rather than issuing a patch the API
				// server would reject.
				msg := fmt.Sprintf("refusing to shrink volume %q from %s to %s: "+
					"Kubernetes does not support shrinking a PersistentVolumeClaim",
					vct.Name, oldSize.String(), desiredSize.String())
				logger.Info(msg)
				if recorder != nil && object != nil {
					recorder.Event(object, corev1.EventTypeWarning, "VolumeShrinkRejected", msg)
				}

				continue
			case cmp > 0:
				templateGrew = true
			}
			// cmp == 0 falls through: the grow loop is a no-op for matching PVCs
			// but self-heals any PVC that lags the (unchanged) template size.
		}

		// Grow-only: patch each existing PVC strictly below the desired size. A PVC
		// already at or above it (equal, or manually over-expanded) is skipped
		// silently — never shrunk, never warned about.
		for ordinal := range replicas {
			pvcName := fmt.Sprintf("%s-%s-%d", vct.Name, stsName, ordinal)
			pvc, err := pvcClient.Get(ctx, pvcName, metav1.GetOptions{})
			if err != nil {
				if kerrors.IsNotFound(err) {
					// Not yet scaled out; the StatefulSet controller creates it from
					// the refreshed template at the new size.
					continue
				}

				return false, fmt.Errorf("getting PVC %s: %w", pvcName, err)
			}

			current := pvc.Spec.Resources.Requests.Storage()
			if desiredSize.Cmp(*current) <= 0 {
				continue // already at or above the desired size
			}

			patch, err := json.Marshal(map[string]any{
				"spec": map[string]any{
					"resources": map[string]any{
						"requests": map[string]any{
							string(corev1.ResourceStorage): desiredSize.String(),
						},
					},
				},
			})
			if err != nil {
				return false, fmt.Errorf("marshaling storage expansion patch for PVC %s: %w", pvcName, err)
			}
			if _, err := pvcClient.Patch(ctx, pvcName, types.MergePatchType, patch, metav1.PatchOptions{}); err != nil {
				return false, fmt.Errorf("patching storage request on PVC %s: %w", pvcName, err)
			}
			logger.Info("expanded PVC storage request", "pvc", pvcName, "from", current.String(), "to", desiredSize.String())
			if recorder != nil && object != nil {
				recorder.Eventf(object, corev1.EventTypeNormal, "VolumeExpanded",
					"expanded PVC %s from %s to %s", pvcName, current.String(), desiredSize.String())
			}
		}
	}

	return templateGrew, nil
}
