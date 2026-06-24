package controller

import (
	"context"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	ledgerv1alpha1 "github.com/formance/ledger/operator/api/v1alpha1"
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

// TLSMigrationPhase values surfaced on LedgerService.Status.TLSMigrationPhase.
const (
	TLSPhaseDisabled                = "disabled"
	TLSPhaseTransitioningToRequired = "transitioning-to-required"
	TLSPhaseTransitioningToDisabled = "transitioning-to-disabled"
	TLSPhaseRequired                = "required"
)

// desiredTLSMode maps the user-facing bool to the strict target mode.
func desiredTLSMode(ledger *ledgerv1alpha1.LedgerService) string {
	if ledger.Spec.TLS != nil && ledger.Spec.TLS.Enabled {
		return tlsModeRequired
	}

	return tlsModeDisabled
}

// fetchTLSMode returns the TLS_MODE currently in effect on the running pods of
// the named LedgerService, by reading the TLS_MODE env var from the
// StatefulSet pod template. Returns an empty string (and no error) if the
// StatefulSet does not exist yet; the caller's exec is meaningless in that
// case and will fail on the pod lookup, but this keeps the bootstrap path
// from masking the original error.
func fetchTLSMode(ctx context.Context, c client.Client, namespace, name string) (string, error) {
	sts := &appsv1.StatefulSet{}
	if err := c.Get(ctx, types.NamespacedName{Name: name, Namespace: namespace}, sts); err != nil {
		if apierrors.IsNotFound(err) {
			return "", nil
		}

		return "", err
	}

	return currentTLSModeFromStatefulSet(sts), nil
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
// returning (nil, nil) if it doesn't exist yet (bootstrap case).
func (r *LedgerServiceReconciler) fetchExistingStatefulSet(ctx context.Context, ledger *ledgerv1alpha1.LedgerService) (*appsv1.StatefulSet, error) {
	sts := &appsv1.StatefulSet{}

	err := r.Get(ctx, types.NamespacedName{Name: resourceName(ledger.Name), Namespace: ledger.Namespace}, sts)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}

		return nil, err
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

// _ keeps corev1 imported in case future helpers need EnvVar/Volume.
var _ = corev1.EnvVar{}
