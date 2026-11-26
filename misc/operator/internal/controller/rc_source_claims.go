package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
)

// annotationClaimSourceTag records the Ledger image tag that wrote a retained
// WAL or data claim. Its meaning is a guarantee, not a hint: every Ledger Pod
// that mounted the claim since the annotation was written ran exactly this tag.
//
// The operator keeps the guarantee with two rules:
//   - it stamps a claim only after a fresh API read shows the owned StatefulSet
//     fully rolled out on one template whose Ledger image carries the tag;
//   - before it publishes a StatefulSet template whose Ledger tag differs from
//     a claim's recorded tag, it removes that annotation, and it does not
//     publish when the removal fails.
//
// The stopped-upgrade reconciler also removes it before deleting the source
// StatefulSet. A missing StatefulSet with retained claims then proves its RC
// source only when every ordinal claim records the requested tag.
const annotationClaimSourceTag = "ledger.formance.com/data-source-tag"

// ordinalClaimPrefixes returns the claim-name prefixes produced by the WAL and
// data claim templates of the Cluster's StatefulSet.
func ordinalClaimPrefixes(stsName string) []string {
	return []string{"wal-" + stsName, "data-" + stsName}
}

func isOrdinalClaim(claimName, stsName string) bool {
	return slices.ContainsFunc(ordinalClaimPrefixes(stsName), func(prefix string) bool {
		return hasStatefulSetOrdinal(claimName, prefix)
	})
}

// listOrdinalClaims reads every WAL/data ordinal claim of the Cluster, at any
// ordinal, through the uncached API reader. A cache that lags behind a recent
// stamp must neither hide that stamp from a clear nor show a cleared stamp to
// the RC source check. The operator also has no watch RBAC on PVCs, so a
// cached List would start an informer that never syncs.
func (r *ClusterReconciler) listOrdinalClaims(ctx context.Context, ledger *ledgerv1alpha1.Cluster) ([]corev1.PersistentVolumeClaim, error) {
	claims := &corev1.PersistentVolumeClaimList{}
	if err := r.storageAPIReader().List(ctx, claims, client.InNamespace(ledger.Namespace)); err != nil {
		return nil, fmt.Errorf("listing PVCs for the RC source record: %w", err)
	}
	name := resourceName(ledger.Name)
	matching := make([]corev1.PersistentVolumeClaim, 0, len(claims.Items))
	for _, claim := range claims.Items {
		if isOrdinalClaim(claim.Name, name) {
			matching = append(matching, claim)
		}
	}
	slices.SortFunc(matching, func(a, b corev1.PersistentVolumeClaim) int {
		return strings.Compare(a.Name, b.Name)
	})

	return matching, nil
}

// patchClaimSourceTag writes (tag != nil) or removes (tag == nil) the source
// annotation. The merge patch carries the observed UID and resourceVersion,
// so a replaced or concurrently modified claim is rejected rather than
// relabelled; the next reconcile reads it again.
func (r *ClusterReconciler) patchClaimSourceTag(ctx context.Context, claim *corev1.PersistentVolumeClaim, tag *string) error {
	if claim.UID == "" || claim.ResourceVersion == "" {
		return fmt.Errorf("PVC %s has no observed identity", claim.Name)
	}
	var value any
	if tag != nil {
		value = *tag
	}
	patch, err := json.Marshal(map[string]any{"metadata": map[string]any{
		"uid":             claim.UID,
		"resourceVersion": claim.ResourceVersion,
		"annotations":     map[string]any{annotationClaimSourceTag: value},
	}})
	if err != nil {
		return fmt.Errorf("encoding RC source patch for PVC %s: %w", claim.Name, err)
	}
	target := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: claim.Name, Namespace: claim.Namespace}}
	if err := r.Patch(ctx, target, client.RawPatch(types.MergePatchType, patch)); err != nil {
		return fmt.Errorf("patching RC source on PVC %s: %w", claim.Name, err)
	}

	return nil
}

// clearClaimSourceTagsExcept removes the source annotation from every ordinal
// claim whose recorded tag differs from keep. An empty keep clears every claim.
// Callers run it before any workload change that may let a Pod with another
// image mount the claims; an error must stop that change.
func (r *ClusterReconciler) clearClaimSourceTagsExcept(ctx context.Context, ledger *ledgerv1alpha1.Cluster, keep string) error {
	claims, err := r.listOrdinalClaims(ctx, ledger)
	if err != nil {
		return err
	}
	for i := range claims {
		recorded, ok := claims[i].Annotations[annotationClaimSourceTag]
		if !ok || (keep != "" && recorded == keep) {
			continue
		}
		if err := r.patchClaimSourceTag(ctx, &claims[i], nil); err != nil {
			return fmt.Errorf("clearing RC source before a workload image change: %w", err)
		}
		log.FromContext(ctx).Info("cleared RC source record on PVC", "pvc", claims[i].Name, "recorded", recorded, "next", keep)
	}

	return nil
}

// clearClaimSourceTagsForTemplate clears every record that the template about
// to be published could falsify. A template whose Ledger tag cannot be read,
// such as a digest-only image, clears all records.
func (r *ClusterReconciler) clearClaimSourceTagsForTemplate(ctx context.Context, ledger *ledgerv1alpha1.Cluster, template corev1.PodTemplateSpec) error {
	tag, err := ledgerContainerTag(template.Spec.Containers)
	if err != nil {
		tag = ""
	}

	return r.clearClaimSourceTagsExcept(ctx, ledger, tag)
}

// ledgerImageTagChanged reports whether two templates run different Ledger
// tags. An unreadable tag on either side counts as a change.
func ledgerImageTagChanged(live, desired corev1.PodTemplateSpec) bool {
	liveTag, liveErr := ledgerContainerTag(live.Spec.Containers)
	desiredTag, desiredErr := ledgerContainerTag(desired.Spec.Containers)

	return liveErr != nil || desiredErr != nil || liveTag != desiredTag
}

// sourceTagRolloutConverged requires one revision across every replica in
// addition to the ordinary rollout convergence.
func sourceTagRolloutConverged(sts *appsv1.StatefulSet) bool {
	return rolloutConverged(sts) && sts.Status.CurrentRevision == sts.Status.UpdateRevision
}

// reconcileClaimSourceTags stamps the claims mounted by a converged owned
// StatefulSet with its Ledger image tag. Only claims created from one of its
// claim templates and mounted by the ledger container qualify. It reads the StatefulSet and claims
// through the uncached API reader: a cached StatefulSet that still shows the
// previous converged template after a new publish must not re-stamp the old
// tag. Ordinals at or above the replica count are left as they are; no Pod
// mounts them, so a record they already carry remains true.
func (r *ClusterReconciler) reconcileClaimSourceTags(ctx context.Context, ledger *ledgerv1alpha1.Cluster) error {
	sts := &appsv1.StatefulSet{}
	err := r.storageAPIReader().Get(ctx, types.NamespacedName{Name: resourceName(ledger.Name), Namespace: ledger.Namespace}, sts)
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("reading StatefulSet for the RC source record: %w", err)
	}
	if !metav1.IsControlledBy(sts, ledger) || !sts.DeletionTimestamp.IsZero() || sts.Spec.Replicas == nil || !sourceTagRolloutConverged(sts) {
		return nil
	}
	tag, err := ledgerImageTag(sts)
	if err != nil {
		return nil //nolint:nilerr // An unidentified image is never recorded.
	}
	claims, err := r.listOrdinalClaims(ctx, ledger)
	if err != nil {
		return err
	}
	var errs []error
	for i := range claims {
		claim := &claims[i]
		if claim.Annotations[annotationClaimSourceTag] == tag || !claim.DeletionTimestamp.IsZero() ||
			!claimOrdinalBelow(claim.Name, sts.Name, *sts.Spec.Replicas) || !claimMountedByTemplate(claim.Name, sts) {
			continue
		}
		if err := r.patchClaimSourceTag(ctx, claim, &tag); err != nil {
			errs = append(errs, err)

			continue
		}
		log.FromContext(ctx).Info("recorded RC source on PVC", "pvc", claim.Name, "tag", tag)
	}

	return errors.Join(errs...)
}

// claimMountedByTemplate reports whether the Pods of sts mount claimName: the
// claim must come from one of the StatefulSet's claim templates, and the
// ledger container must mount that template's volume. A retained claim whose
// volume is now hostPath-backed has no template; the converged Pods never
// mounted it, so recording their tag on it would be false.
func claimMountedByTemplate(claimName string, sts *appsv1.StatefulSet) bool {
	for _, template := range sts.Spec.VolumeClaimTemplates {
		if !hasStatefulSetOrdinal(claimName, template.Name+"-"+sts.Name) {
			continue
		}
		for _, container := range sts.Spec.Template.Spec.Containers {
			if container.Name != "ledger" {
				continue
			}
			for _, mount := range container.VolumeMounts {
				if mount.Name == template.Name {
					return true
				}
			}
		}

		return false
	}

	return false
}

func claimOrdinalBelow(claimName, stsName string, replicas int32) bool {
	for _, prefix := range ordinalClaimPrefixes(stsName) {
		if !hasStatefulSetOrdinal(claimName, prefix) {
			continue
		}
		ordinal, err := strconv.ParseUint(strings.TrimPrefix(claimName, prefix+"-"), 10, 32)

		return err == nil && replicas > 0 && ordinal < uint64(replicas)
	}

	return false
}

// rejectDeletingClaims refuses a recreation while a retained ordinal claim is
// being deleted. Such a claim is about to be replaced by an empty one, so
// neither its record nor a matching orphan Pod says anything about the data
// the recreated Pods will mount. Kubernetes keeps the claim until no Pod
// mounts it, so an orphan Pod still using it holds the deletion open.
func rejectDeletingClaims(stsName string, claims []corev1.PersistentVolumeClaim) error {
	for _, claim := range claims {
		if !claim.DeletionTimestamp.IsZero() {
			return fmt.Errorf("%w: cannot recreate StatefulSet %q while PVC %q is being deleted; wait for the deletion to finish, which requires that no Pod still mounts the claim", errRCClaimDeletionPending, stsName, claim.Name)
		}
	}

	return nil
}

// rejectMissingClaims refuses a recreation that would provision an empty claim
// for a member of the previous cluster. A retained ordinal claim or a matching
// orphan Pod proves that a previous cluster existed; every claim the recreated
// StatefulSet will mount (each PVC-backed volume at each ordinal below the
// desired replica count) must then still exist. The operator cannot tell a
// claim that was lost from one that never existed, so a recreation that also
// scales up is refused too: recreate with the previous replica count, then
// scale through the normal path. Without such evidence there is nothing to
// protect and the Cluster is created from scratch.
func rejectMissingClaims(ledger *ledgerv1alpha1.Cluster, orphanPods bool, claims []corev1.PersistentVolumeClaim) error {
	if !orphanPods && len(claims) == 0 {
		return nil
	}
	stsName := resourceName(ledger.Name)
	present := make(map[string]bool, len(claims))
	for _, claim := range claims {
		present[claim.Name] = true
	}
	replicas := int32(3)
	if ledger.Spec.Replicas != nil {
		replicas = *ledger.Spec.Replicas
	}
	var missing []string
	for _, volume := range pvcVolumeNames(&ledger.Spec.Persistence) {
		for ordinal := range replicas {
			name := fmt.Sprintf("%s-%s-%d", volume, stsName, ordinal)
			if !present[name] {
				missing = append(missing, name)
			}
		}
	}
	if len(missing) > 0 {
		return fmt.Errorf("%w: cannot recreate StatefulSet %q over the previous cluster: missing PVC(s) %s would be provisioned empty; restore them, or recreate with the replica count the retained claims cover and scale afterwards", errRCClaimMissing, stsName, strings.Join(missing, ", "))
	}

	return nil
}

// validateClaimSourceTags admits a recreation only when every retained
// ordinal claim records exactly the requested tag. Without a StatefulSet there
// is no rolling transition, so even the next adjacent RC is refused.
func validateClaimSourceTags(stsName, target string, claims []corev1.PersistentVolumeClaim) error {
	if len(claims) == 0 {
		return nil
	}
	if err := rejectDeletingClaims(stsName, claims); err != nil {
		return err
	}
	var unrecorded []string
	recorded := map[string][]string{}
	for _, claim := range claims {
		tag, ok := claim.Annotations[annotationClaimSourceTag]
		if !ok || tag == "" {
			unrecorded = append(unrecorded, claim.Name)

			continue
		}
		recorded[tag] = append(recorded[tag], claim.Name)
	}
	if len(unrecorded) > 0 {
		return fmt.Errorf("cannot verify RC source while StatefulSet %q is absent and PVC %q remains without the %s annotation; verify the tag that last ran on every retained claim and record it as documented in the upgrade guide", stsName, unrecorded[0], annotationClaimSourceTag)
	}
	if len(recorded) > 1 {
		tags := slices.Sorted(maps.Keys(recorded))

		return fmt.Errorf("cannot verify RC source while StatefulSet %q is absent: retained PVCs record different source tags %s", stsName, strings.Join(tags, ", "))
	}
	for source := range recorded {
		if source != target {
			return fmt.Errorf("cannot change RC while StatefulSet %q is absent: retained PVCs record source %s; restore spec.image.tag to %s, let the rollout converge, then upgrade to %s", stsName, source, source, target)
		}
	}

	return nil
}
