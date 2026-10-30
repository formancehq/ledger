package controller

import (
	"context"
	"errors"
	"fmt"
	"path"
	"reflect"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
)

// Internal TLS modes propagated to the ledger via the TLS_MODE env var.
// The user-facing CR exposes only the boolean tls.enabled; the operator
// drives the StatefulSet through an intermediate "optional" mode during a
// toggle so that pods on either side of the rolling update can still talk
// to each other.
const (
	tlsModeDisabled = "disabled"
	tlsModeOptional = "optional"
	tlsModeRequired = "required"
)

// TLSMigrationPhase values surfaced on Cluster.Status.TLSMigrationPhase.
const (
	TLSPhaseDisabled                = "disabled"
	TLSPhaseTransitioningToRequired = "transitioning-to-required"
	TLSPhaseTransitioningToDisabled = "transitioning-to-disabled"
	TLSPhaseRequired                = "required"
)

// desiredTLSMode maps the user-facing bool to the strict target mode.
func desiredTLSMode(ledger *ledgerv1alpha1.Cluster) string {
	if ledger.Spec.TLS != nil && ledger.Spec.TLS.Enabled {
		return tlsModeRequired
	}

	return tlsModeDisabled
}

// errClusterRuntimeAbsent signals that the Cluster has no StatefulSet under its
// derived name, so there is no pod of this Cluster to exec into or dial. A pod
// named "<statefulset>-0" may still exist (e.g. orphaned during StatefulSet
// recreation, or left by a deleted Cluster of the same name), which is exactly
// why the exec paths must not proceed.
var errClusterRuntimeAbsent = errors.New("cluster StatefulSet not found")

// fetchClusterStatefulSet returns the Cluster's StatefulSet when the Cluster
// controls it. It is the gate of every cross-CRD exec path (Ledger, ledger
// indexes, EventSink) and of the TLS mode they use: a missing StatefulSet is
// errClusterRuntimeAbsent, so callers retry before exec (orphaned pods may
// still require TLS during StatefulSet recreation, so absence cannot safely
// select plaintext), and one not controlled by the Cluster is
// errResourceNameConflict. Exec paths then pass the returned StatefulSet to
// ownedPodExec, which checks the target pod itself, and read the TLS mode from
// it with currentTLSModeFromStatefulSet.
func fetchClusterStatefulSet(ctx context.Context, c client.Client, cluster *ledgerv1alpha1.Cluster) (*appsv1.StatefulSet, error) {
	sts := &appsv1.StatefulSet{}
	key := types.NamespacedName{Name: resourceName(cluster.Name), Namespace: cluster.Namespace}
	if err := c.Get(ctx, key, sts); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, fmt.Errorf("%w: %s: %w", errClusterRuntimeAbsent, key, err)
		}

		return nil, err
	}
	if notControlledBy(sts, cluster) {
		return nil, nameConflictError("StatefulSet", sts, cluster)
	}

	return sts, nil
}

// currentTLSModeFromStatefulSet returns the TLS_MODE env var that is
// currently configured on the StatefulSet pod template, or "" if the
// StatefulSet does not yet exist (bootstrap path) or the env var is unset.
func currentTLSModeFromStatefulSet(sts *appsv1.StatefulSet) string {
	if sts == nil {
		return ""
	}

	for _, c := range sts.Spec.Template.Spec.Containers {
		if c.Name != "ledger" {
			continue
		}

		for _, e := range c.Env {
			if e.Name == "TLS_MODE" {
				return e.Value
			}
		}
	}

	return ""
}

// rolloutConverged reports whether the StatefulSet has fully rolled out the
// latest pod template — all replicas updated and ready, status observed at
// the current generation.
func rolloutConverged(sts *appsv1.StatefulSet) bool {
	if sts == nil {
		return false
	}

	if sts.Status.ObservedGeneration < sts.Generation {
		return false
	}

	desired := int32(1)
	if sts.Spec.Replicas != nil {
		desired = *sts.Spec.Replicas
	}

	return sts.Status.UpdatedReplicas == desired && sts.Status.ReadyReplicas == desired
}

// computeTargetTLSMode is the heart of the TLS migration state machine.
//
// Inputs:
//   - desired: "disabled" or "required" (derived from spec.tls.enabled).
//   - actual:  the TLS_MODE currently configured on the StatefulSet, or ""
//     if no StatefulSet exists yet (initial bootstrap).
//   - converged: whether the StatefulSet has fully rolled out its current
//     template.
//
// Output: the TLS_MODE the operator must set on the StatefulSet right now.
//
// Transitions:
//   - Bootstrap (actual == ""): go directly to desired; no peers exist.
//   - Stable: target == desired.
//   - Mid-migration (actual == "optional"): wait for rollout to converge,
//     then advance to desired.
//   - Toggle (actual == disabled and desired == required, or vice versa):
//     step through "optional" first so both modes coexist during the
//     rolling update.
func computeTargetTLSMode(desired, actual string, converged bool) string {
	if actual == "" {
		return desired
	}

	if actual == desired {
		return desired
	}

	if actual == tlsModeOptional {
		if converged {
			return desired
		}

		return tlsModeOptional
	}

	return tlsModeOptional
}

// tlsMigrationPhase maps (desired, target) to the user-visible
// status.tlsMigrationPhase value.
func tlsMigrationPhase(desired, target string) string {
	switch {
	case target == tlsModeDisabled:
		return TLSPhaseDisabled
	case target == tlsModeRequired:
		return TLSPhaseRequired
	case target == tlsModeOptional && desired == tlsModeRequired:
		return TLSPhaseTransitioningToRequired
	case target == tlsModeOptional && desired == tlsModeDisabled:
		return TLSPhaseTransitioningToDisabled
	}

	return ""
}

// fetchExistingStatefulSet retrieves the live StatefulSet for the ledger,
// returning (nil, nil) if it doesn't exist yet (bootstrap case). A StatefulSet
// under the name that the Cluster does not control is errResourceNameConflict:
// its image, TLS mode and rollout state must never drive this Cluster's RC
// upgrade check, TLS transition or cluster Secret.
func (r *ClusterReconciler) fetchExistingStatefulSet(ctx context.Context, ledger *ledgerv1alpha1.Cluster) (*appsv1.StatefulSet, error) {
	sts := &appsv1.StatefulSet{}

	err := r.Get(ctx, types.NamespacedName{Name: resourceName(ledger.Name), Namespace: ledger.Namespace}, sts)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}

		return nil, err
	}
	if notControlledBy(sts, ledger) {
		return nil, nameConflictError("StatefulSet", sts, ledger)
	}

	return sts, nil
}

// shouldInjectClusterSecret reports whether the operator must reconcile the
// cluster-secret Secret and propagate CLUSTER_SECRET into pods.
//
// The cluster secret is a static bearer token; sending it in plaintext is
// an anti-pattern, so it is gated on TLS being at least partially active
// (mode != disabled).
func shouldInjectClusterSecret(targetTLSMode string) bool {
	return targetTLSMode != "" && targetTLSMode != tlsModeDisabled
}

// observedTLSMode falls back to durable rollout state while an orphaned
// StatefulSet is absent. Absence alone must not bootstrap plaintext among TLS peers.
func observedTLSMode(ledger *ledgerv1alpha1.Cluster, sts *appsv1.StatefulSet) string {
	if sts == nil && ledger.Status.TLSRollout != nil {
		return ledger.Status.TLSRollout.Mode
	}

	return currentTLSModeFromStatefulSet(sts)
}

// tlsConfigFromStatefulSet recovers the effective certificate and CA references
// from the workload, including when the desired TLS block has already been removed.
func tlsConfigFromStatefulSet(sts *appsv1.StatefulSet) *ledgerv1alpha1.TLSConfig {
	if sts == nil {
		return nil
	}
	var config ledgerv1alpha1.TLSConfig
	for _, volume := range sts.Spec.Template.Spec.Volumes {
		if volume.Name == "tls-certs" && volume.Secret != nil {
			config.SecretName = volume.Secret.SecretName
		}
	}
	for _, container := range sts.Spec.Template.Spec.Containers {
		if container.Name != "ledger" {
			continue
		}
		for _, env := range container.Env {
			if env.Name == "TLS_CA_CERT_FILE" && strings.HasPrefix(env.Value, "/tls/") {
				config.CASecretKey = path.Base(env.Value)
			}
		}
	}
	if config.SecretName == "" {
		return nil
	}

	return &config
}

// prepareTLSRollout persists recovery state before any workload mutation. A
// failed or conflicting status write leaves the existing StatefulSet untouched.
// Desired spec is never rewritten with the effective transitional configuration.
func prepareTLSRollout(ctx context.Context, c client.Client, ledger *ledgerv1alpha1.Cluster, sts *appsv1.StatefulSet, target string) (*ledgerv1alpha1.Cluster, error) {
	actual := currentTLSModeFromStatefulSet(sts)
	if sts != nil && (actual == tlsModeRequired || actual == tlsModeOptional) && !metav1.IsControlledBy(sts, ledger) {
		return nil, fmt.Errorf("TLS StatefulSet %q is not controlled by Cluster %q (UID %s)", sts.Name, ledger.Name, ledger.UID)
	}
	var config *ledgerv1alpha1.TLSConfig
	if desiredTLSMode(ledger) == tlsModeRequired {
		config = ledger.Spec.TLS.DeepCopy()
	} else {
		config = tlsConfigFromStatefulSet(sts)
		if config == nil && ledger.Status.TLSRollout != nil {
			config = &ledgerv1alpha1.TLSConfig{
				SecretName:  ledger.Status.TLSRollout.SecretName,
				CASecretKey: ledger.Status.TLSRollout.CASecretKey,
			}
		}
	}
	if target != tlsModeDisabled && (config == nil || config.SecretName == "") {
		return nil, fmt.Errorf("TLS mode %s requires a certificate Secret reference in the StatefulSet or retained rollout status", target)
	}

	var retained *ledgerv1alpha1.TLSRolloutStatus
	if config != nil && (target != tlsModeDisabled || currentTLSModeFromStatefulSet(sts) != tlsModeDisabled || !rolloutConverged(sts)) {
		retained = &ledgerv1alpha1.TLSRolloutStatus{Mode: target, SecretName: config.SecretName, CASecretKey: config.CASecretKey}
	}
	if !reflect.DeepEqual(ledger.Status.TLSRollout, retained) {
		before := ledger.DeepCopy()
		updated := ledger.DeepCopy()
		updated.Status.TLSRollout = retained
		if err := c.Status().Patch(ctx, updated, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{})); err != nil {
			return nil, fmt.Errorf("persisting TLS rollout recovery state: %w", err)
		}
		ledger.ResourceVersion = updated.ResourceVersion
		ledger.Status.TLSRollout = retained
	}
	effective := ledger.DeepCopy()
	effective.Spec.TLS = config

	return effective, nil
}
