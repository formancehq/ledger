package controller

import (
	"context"
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
)

// volumeClaimTemplateSizeChanges compares existing and desired
// VolumeClaimTemplates by name and reports storage size drift. PVC storage is
// grow-only in Kubernetes, so grows and shrinks call for different handling:
// grown templates are returned with their target size so the caller can expand
// the backing PVCs, shrunk templates are returned by name so the caller can
// surface the ignored request. Templates present on only one side are the
// PVC<->hostPath switch case, handled by volumeClaimTemplatesChanged.
func volumeClaimTemplateSizeChanges(existing, desired []corev1.PersistentVolumeClaim) (map[string]resource.Quantity, []string) {
	desiredSizes := make(map[string]resource.Quantity, len(desired))
	for _, tmpl := range desired {
		desiredSizes[tmpl.Name] = tmpl.Spec.Resources.Requests[corev1.ResourceStorage]
	}

	var grown map[string]resource.Quantity
	var shrunk []string
	for _, tmpl := range existing {
		desiredSize, ok := desiredSizes[tmpl.Name]
		if !ok {
			continue
		}
		existingSize := tmpl.Spec.Resources.Requests[corev1.ResourceStorage]
		switch desiredSize.Cmp(existingSize) {
		case 1:
			if grown == nil {
				grown = map[string]resource.Quantity{}
			}
			grown[tmpl.Name] = desiredSize
		case -1:
			shrunk = append(shrunk, tmpl.Name)
		}
	}

	return grown, shrunk
}

// expandClusterPVCs grows the existing PVCs backing the StatefulSet's grown
// VolumeClaimTemplates, ordinal by ordinal up to the current replica count.
// PVCs not yet provisioned are skipped: the StatefulSet recreated from the
// grown template sizes them correctly at creation. Failures are surfaced as
// Warning events on the Cluster — the storage class must allow volume
// expansion for the resize to be admitted.
func (r *ClusterReconciler) expandClusterPVCs(ctx context.Context, ledger *ledgerv1alpha1.Cluster, sts *appsv1.StatefulSet, grown map[string]resource.Quantity) error {
	logger := log.FromContext(ctx)

	replicas := int32(1)
	if sts.Spec.Replicas != nil {
		replicas = *sts.Spec.Replicas
	}

	for tmplName, size := range grown {
		for ordinal := range replicas {
			pvcName := fmt.Sprintf("%s-%s-%d", tmplName, sts.Name, ordinal)
			pvc := &corev1.PersistentVolumeClaim{}
			err := r.Get(ctx, types.NamespacedName{Name: pvcName, Namespace: sts.Namespace}, pvc)
			if apierrors.IsNotFound(err) {
				continue
			}
			if err != nil {
				return fmt.Errorf("fetching PVC %s: %w", pvcName, err)
			}

			current := pvc.Spec.Resources.Requests[corev1.ResourceStorage]
			if current.Cmp(size) >= 0 {
				continue
			}

			patch := client.MergeFrom(pvc.DeepCopy())
			pvc.Spec.Resources.Requests[corev1.ResourceStorage] = size
			if err := r.Patch(ctx, pvc, patch); err != nil {
				if r.Recorder != nil {
					r.Recorder.Eventf(ledger, corev1.EventTypeWarning, "VolumeExpansionFailed",
						"Failed to grow PVC %s to %s: %v", pvcName, size.String(), err)
				}

				return fmt.Errorf("growing PVC %s: %w", pvcName, err)
			}

			logger.Info("grew PVC", "pvc", pvcName, "size", size.String())
			if r.Recorder != nil {
				r.Recorder.Eventf(ledger, corev1.EventTypeNormal, "VolumeExpanded",
					"Grew PVC %s to %s", pvcName, size.String())
			}
		}
	}

	return nil
}
