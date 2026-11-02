package controller

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/Masterminds/semver/v3"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
)

type rcVersion struct {
	version *semver.Version
	ordinal uint64
}

// parseRCVersion keeps the version syntax with semver and only extracts the
// numeric RC ordinal needed to check adjacency.
func parseRCVersion(value string) (rcVersion, error) {
	if !strings.HasPrefix(value, "v") {
		return rcVersion{}, fmt.Errorf("%q is not a canonical RC version", value)
	}
	parsed, err := semver.StrictNewVersion(strings.TrimPrefix(value, "v"))
	if err != nil || parsed.Metadata() != "" || "v"+parsed.String() != value {
		return rcVersion{}, fmt.Errorf("%q is not a canonical RC version", value)
	}
	parts := strings.Split(parsed.Prerelease(), ".")
	if len(parts) != 2 || parts[0] != "rc" || parts[1] == "" || strings.HasPrefix(parts[1], "0") {
		return rcVersion{}, fmt.Errorf("%q must use the vX.Y.Z-rc.N form with N > 0", value)
	}
	ordinal, err := strconv.ParseUint(parts[1], 10, 64)
	if err != nil || ordinal == 0 {
		return rcVersion{}, fmt.Errorf("%q must use the vX.Y.Z-rc.N form with N > 0", value)
	}

	return rcVersion{version: parsed, ordinal: ordinal}, nil
}

func rcCandidate(value string) bool {
	return strings.Contains(value, "-rc")
}

func ledgerImageTag(sts *appsv1.StatefulSet) (string, error) {
	return ledgerContainerTag(sts.Spec.Template.Spec.Containers)
}

func ledgerContainerTag(containers []corev1.Container) (string, error) {
	for _, container := range containers {
		if container.Name != "ledger" {
			continue
		}
		// A digest alone does not identify the published release. The existing
		// operator renders repository:tag, so an untagged digest is unknown.
		if strings.Contains(container.Image, "@") {
			return "", fmt.Errorf("cannot determine the running RC from digest image %q", container.Image)
		}
		colon := strings.LastIndex(container.Image, ":")
		if colon <= strings.LastIndex(container.Image, "/") || colon == len(container.Image)-1 {
			return "", fmt.Errorf("cannot determine the running RC from image %q", container.Image)
		}

		return container.Image[colon+1:], nil
	}

	return "", errors.New("cannot determine the running RC: StatefulSet has no ledger container")
}

// A missing StatefulSet can be an interrupted orphan-and-recreate operation,
// not a new cluster. The old Pods retain the source image, and the old PVCs
// remain when Pods have already gone. Refuse a new RC while that source cannot
// be verified from a complete StatefulSet rollout.
func (r *ClusterReconciler) validateRCUpgradeWithoutStatefulSet(ctx context.Context, ledger *ledgerv1alpha1.Cluster) error {
	if !rcCandidate(ledger.Spec.Image.Tag) {
		return nil
	}

	name := resourceName(ledger.Name)
	pods := &corev1.PodList{}
	if err := r.List(ctx, pods, client.InNamespace(ledger.Namespace)); err != nil {
		return fmt.Errorf("listing Pods for RC upgrade validation: %w", err)
	}
	matchingPod := false
	for _, pod := range pods.Items {
		if !hasStatefulSetOrdinal(pod.Name, name) {
			continue
		}
		matchingPod = true
		source, err := ledgerContainerTag(pod.Spec.Containers)
		if err != nil || source != ledger.Spec.Image.Tag {
			return fmt.Errorf("cannot change RC while StatefulSet %q is absent and Pod %q retains another or unknown image; restore the source image tag first", name, pod.Name)
		}
	}
	if matchingPod {
		// A matching orphan Pod is enough to prove this is a recreation of
		// the same image, not a new version transition.
		return nil
	}

	claims := &corev1.PersistentVolumeClaimList{}
	if err := r.List(ctx, claims, client.InNamespace(ledger.Namespace)); err != nil {
		return fmt.Errorf("listing PVCs for RC upgrade validation: %w", err)
	}
	for _, claim := range claims.Items {
		if hasStatefulSetOrdinal(claim.Name, "wal-"+name) || hasStatefulSetOrdinal(claim.Name, "data-"+name) {
			return fmt.Errorf("cannot verify RC source while StatefulSet %q is absent and PVC %q remains", name, claim.Name)
		}
	}

	return nil
}

func hasStatefulSetOrdinal(objectName, prefix string) bool {
	ordinal, ok := strings.CutPrefix(objectName, prefix+"-")
	if !ok || ordinal == "" {
		return false
	}
	_, err := strconv.ParseUint(ordinal, 10, 32)

	return err == nil
}

// A VolumeClaimTemplate replacement orphans the current Pods. First publish
// the adjacent RC template and let its rollout finish, so those Pods never
// span two RCs when the StatefulSet is deleted for recreation.
func deferRCVolumeClaimRecreation(target string, sts *appsv1.StatefulSet) bool {
	if !rcCandidate(target) {
		return false
	}
	source, err := ledgerImageTag(sts)

	return err != nil || source != target || !rolloutConverged(sts)
}

// validateRCUpgrade rejects a skipped RC before the controller modifies any
// resource. The current pod template, not a user-declared source, determines
// the release already accepted by the operator.
func validateRCUpgrade(target string, sts *appsv1.StatefulSet) error {
	if sts == nil {
		return nil // Initial creation is not an upgrade.
	}
	source, err := ledgerImageTag(sts)
	if err != nil {
		if rcCandidate(target) {
			return err
		}

		return nil
	}
	if source == target {
		return nil // The accepted target may still be rolling out.
	}
	if !rcCandidate(target) {
		return nil // Non-RC targets are outside this rule.
	}
	targetRC, err := parseRCVersion(target)
	if err != nil {
		return fmt.Errorf("invalid target RC: %w", err)
	}
	sourceRC, err := parseRCVersion(source)
	if err != nil {
		return fmt.Errorf("cannot verify source RC %q: %w", source, err)
	}
	if sourceRC.version.Major() != targetRC.version.Major() ||
		sourceRC.version.Minor() != targetRC.version.Minor() ||
		sourceRC.version.Patch() != targetRC.version.Patch() ||
		sourceRC.ordinal == ^uint64(0) || targetRC.ordinal != sourceRC.ordinal+1 {
		return fmt.Errorf("unsupported RC upgrade %s -> %s: only the immediately next RC in the same release line is allowed", source, target)
	}
	if !rolloutConverged(sts) {
		return fmt.Errorf("cannot upgrade %s -> %s until the current StatefulSet rollout is complete", source, target)
	}

	return nil
}
