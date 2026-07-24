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

// reconcilePVCExpansion reconciles this Cluster's PersistentVolumeClaim sizes
// toward the desired volumeClaimTemplates, in a single pass over the live PVCs.
// It does three things:
//
//   - Clamps each desired template size UP to the largest existing PVC for that
//     volume, mutating `desired` in place. Volume resizing is grow-only, so a
//     spec size below a live disk is rejected: a VolumeShrinkRejected warning is
//     emitted once for the volume and the template keeps (at least) the current
//     size. Flooring against the *live PVCs* — not the StatefulSet template — is
//     load-bearing: the template is destroyed by the orphan delete-recreate used
//     to refresh a grown template, and on the following reconcile the template is
//     rebuilt straight from the (possibly shrunk) spec. The PVCs survive that
//     delete, so they are the only durable record of the current size. Without
//     this floor, a spec that grows one volume and shrinks another leaks the
//     shrunken size into the recreated template and every future scale-out
//     ordinal (the mixed grow/shrink case). In all operator-managed states the
//     largest PVC equals the template size, so the floor only diverges when a PVC
//     was resized out of band.
//   - Grows each existing PVC whose request is below the clamped desired size via
//     a spec.resources.requests.storage patch, triggering CSI online expansion
//     (the StorageClass must have allowVolumeExpansion: true). A PVC already at or
//     above the desired size is left untouched.
//   - Returns templateGrew=true when the clamped desired template exceeds the
//     existing StatefulSet template for any volume, signalling the caller to
//     recreate the StatefulSet (orphan propagation) so future scale-out ordinals
//     inherit the new size.
//
// existingTemplates is the current StatefulSet's volumeClaimTemplates, or nil
// when the StatefulSet does not exist yet (e.g. the create pass right after a
// recreate) — in which case templateGrew is always false but the clamp and the
// PVC grow still run. A missing PVC (an ordinal not yet scaled out) is skipped;
// it is created from the template at the clamped size. A nil recorder/object
// skips eventing (unit tests without a recorder).
func reconcilePVCExpansion(
	ctx context.Context,
	clientset kubernetes.Interface,
	recorder record.EventRecorder,
	object runtime.Object,
	namespace, stsName string,
	replicas int32,
	existingTemplates, desired []corev1.PersistentVolumeClaim,
) (bool, error) {
	logger := log.FromContext(ctx)
	pvcClient := clientset.CoreV1().PersistentVolumeClaims(namespace)

	existingSizeByName := make(map[string]resource.Quantity, len(existingTemplates))
	for i := range existingTemplates {
		existingSizeByName[existingTemplates[i].Name] = *existingTemplates[i].Spec.Resources.Requests.Storage()
	}

	type livePVC struct {
		name string
		size resource.Quantity
	}

	templateGrew := false
	for i := range desired {
		vct := &desired[i]
		desiredSize := vct.Spec.Resources.Requests.Storage()
		if desiredSize.IsZero() {
			continue
		}

		// Read this volume's live PVCs once: track the largest request (the
		// grow-only floor) and keep the list so we can grow the laggards below.
		var floor resource.Quantity
		var pvcs []livePVC
		for ordinal := range replicas {
			pvcName := fmt.Sprintf("%s-%s-%d", vct.Name, stsName, ordinal)
			pvc, err := pvcClient.Get(ctx, pvcName, metav1.GetOptions{})
			if err != nil {
				if kerrors.IsNotFound(err) {
					continue // not yet scaled out; born from the template at the clamped size
				}

				return false, fmt.Errorf("getting PVC %s: %w", pvcName, err)
			}

			size := *pvc.Spec.Resources.Requests.Storage()
			if size.Cmp(floor) > 0 {
				floor = size
			}
			pvcs = append(pvcs, livePVC{name: pvcName, size: size})
		}

		// Grow-only: never let the template drop below the largest live disk.
		if !floor.IsZero() && desiredSize.Cmp(floor) < 0 {
			msg := fmt.Sprintf("refusing to shrink volume %q from %s to %s: Kubernetes does "+
				"not support shrinking a PersistentVolumeClaim; keeping the current size",
				vct.Name, floor.String(), desiredSize.String())
			logger.Info(msg)
			if recorder != nil && object != nil {
				recorder.Event(object, corev1.EventTypeWarning, "VolumeShrinkRejected", msg)
			}
			vct.Spec.Resources.Requests[corev1.ResourceStorage] = floor
			desiredSize = vct.Spec.Resources.Requests.Storage() // re-read: the map now holds `floor`
		}

		// A size increase must be re-emitted into the immutable template, which
		// only a delete-recreate can do — otherwise a later scale-out ordinal is
		// born at the old size.
		if oldSize, ok := existingSizeByName[vct.Name]; ok && desiredSize.Cmp(oldSize) > 0 {
			templateGrew = true
		}

		for _, p := range pvcs {
			if desiredSize.Cmp(p.size) <= 0 {
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
				return false, fmt.Errorf("marshaling storage expansion patch for PVC %s: %w", p.name, err)
			}
			if _, err := pvcClient.Patch(ctx, p.name, types.MergePatchType, patch, metav1.PatchOptions{}); err != nil {
				return false, fmt.Errorf("patching storage request on PVC %s: %w", p.name, err)
			}
			logger.Info("expanded PVC storage request", "pvc", p.name, "from", p.size.String(), "to", desiredSize.String())
			if recorder != nil && object != nil {
				recorder.Eventf(object, corev1.EventTypeNormal, "VolumeExpanded",
					"expanded PVC %s from %s to %s", p.name, p.size.String(), desiredSize.String())
			}
		}
	}

	return templateGrew, nil
}
