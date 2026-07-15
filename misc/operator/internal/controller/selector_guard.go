package controller

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"sort"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
)

// errSelectorDrift signals that spec.additionalLabels rewrote one of the
// labels that an owned Service/StatefulSet uses as selector. Selectors are
// immutable on StatefulSet (compiler-enforced by the API server) and on
// long-lived Services we treat them the same to avoid silent traffic black
// holes when a new selector stops matching the existing pods. Caller surfaces
// this via the SelectorImmutable status condition and skips the reconcile.
var errSelectorDrift = errors.New("selector drift detected")

// pruneDisabledOptionalServices deletes HeadlessService / GrpcService when
// the new spec marks them disabled. Runs BEFORE validateSelectorImmutability
// so a single edit that both disables an optional Service and changes
// additionalLabels does not leave a stale Service zombie when the drift on
// the primary Service / StatefulSet aborts the rest of the reconcile loop.
//
// Safe to call before the main reconcile pass: the canonical deletion paths
// live in reconcileHeadlessService / reconcileGrpcService and remain in place
// for the steady state — this is just a pre-pass that mirrors them under the
// drift-guard's nose.
func (r *ClusterReconciler) pruneDisabledOptionalServices(
	ctx context.Context, ledger *ledgerv1alpha1.Cluster,
) error {
	if ledger.Spec.HeadlessService.Enabled != nil && !*ledger.Spec.HeadlessService.Enabled {
		if err := r.deleteIfExists(ctx, &corev1.Service{
			ObjectMeta: metav1.ObjectMeta{
				Name:      headlessServiceName(ledger.Name),
				Namespace: ledger.Namespace,
			},
		}); err != nil {
			return fmt.Errorf("pruning disabled HeadlessService: %w", err)
		}
	}
	if ledger.Spec.IngressGrpc == nil || !ledger.Spec.IngressGrpc.Enabled {
		if err := r.deleteIfExists(ctx, &corev1.Service{
			ObjectMeta: metav1.ObjectMeta{
				Name:      grpcServiceName(ledger.Name),
				Namespace: ledger.Namespace,
			},
		}); err != nil {
			return fmt.Errorf("pruning disabled GrpcService: %w", err)
		}
		// The gRPC Ingress is not selector-bearing (the drift guard never
		// looks at it) but the steady-state delete branch lives in
		// reconcileIngressGrpc, which runs AFTER the guard. Drop it here too
		// so a drift on the primary Service / StatefulSet does not strand
		// an Ingress pointing at a deleted backend.
		if err := r.deleteIfExists(ctx, &networkingv1.Ingress{
			ObjectMeta: metav1.ObjectMeta{
				Name:      grpcIngressName(ledger.Name),
				Namespace: ledger.Namespace,
			},
		}); err != nil {
			return fmt.Errorf("pruning disabled gRPC Ingress: %w", err)
		}
	}

	return nil
}

// validateSelectorImmutability fails the reconcile when the selector computed
// from the current spec no longer matches the selector embedded in an
// already-existing owned object. The check is best-effort: a NotFound is
// treated as "this object will be created in this pass" and is fine.
//
// The returned error is wrapped errSelectorDrift; the message lists every
// drifting object so the user knows what to revert (or which clusters to
// rebuild) in one shot.
func (r *ClusterReconciler) validateSelectorImmutability(
	ctx context.Context, ledger *ledgerv1alpha1.Cluster,
) error {
	desired := selectorLabels(ledger)

	type check struct {
		kind string
		name string
		// wanted reports whether the object is still desired by the new spec.
		// When false, the reconciler will delete it in this pass: the old
		// selector is irrelevant and the check is skipped (otherwise an user
		// could be blocked from disabling an optional Service simply because
		// they also tweaked additionalLabels in the same edit).
		wanted bool
		fetch  func() (map[string]string, bool, error)
	}

	getService := func(name string) func() (map[string]string, bool, error) {
		return func() (map[string]string, bool, error) {
			svc := &corev1.Service{}
			err := r.Get(ctx, types.NamespacedName{Name: name, Namespace: ledger.Namespace}, svc)
			if apierrors.IsNotFound(err) {
				return nil, false, nil
			}
			if err != nil {
				return nil, false, err
			}

			return svc.Spec.Selector, true, nil
		}
	}

	headlessWanted := ledger.Spec.HeadlessService.Enabled == nil || *ledger.Spec.HeadlessService.Enabled
	grpcWanted := ledger.Spec.IngressGrpc != nil && ledger.Spec.IngressGrpc.Enabled

	checks := []check{
		{"Service", resourceName(ledger.Name), true, getService(resourceName(ledger.Name))},
		{"HeadlessService", headlessServiceName(ledger.Name), headlessWanted, getService(headlessServiceName(ledger.Name))},
		{"GrpcService", grpcServiceName(ledger.Name), grpcWanted, getService(grpcServiceName(ledger.Name))},
		{"StatefulSet", resourceName(ledger.Name), true, func() (map[string]string, bool, error) {
			sts := &appsv1.StatefulSet{}
			err := r.Get(ctx, types.NamespacedName{Name: resourceName(ledger.Name), Namespace: ledger.Namespace}, sts)
			if apierrors.IsNotFound(err) {
				return nil, false, nil
			}
			if err != nil {
				return nil, false, err
			}
			if sts.Spec.Selector == nil {
				return nil, true, nil
			}

			return sts.Spec.Selector.MatchLabels, true, nil
		}},
	}

	var drifts []string
	for _, c := range checks {
		if !c.wanted {
			continue
		}
		current, exists, err := c.fetch()
		if err != nil {
			return fmt.Errorf("fetching %s/%s: %w", c.kind, c.name, err)
		}
		if !exists {
			continue
		}
		if maps.Equal(current, desired) {
			continue
		}
		drifts = append(drifts, fmt.Sprintf("%s/%s: current=%s desired=%s",
			c.kind, c.name, formatLabels(current), formatLabels(desired)))
	}

	if len(drifts) == 0 {
		return nil
	}

	return fmt.Errorf(
		"%w: spec.additionalLabels would change immutable selector(s); "+
			"delete the affected object(s) to recreate with the new selector: %s",
		errSelectorDrift, strings.Join(drifts, "; "),
	)
}

// formatLabels returns a deterministic string rendering of a label map.
// Used in error messages so the diff stays stable across reconciles.
func formatLabels(labels map[string]string) string {
	if len(labels) == 0 {
		return "{}"
	}
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf("%s=%s", k, labels[k]))
	}

	return "{" + strings.Join(parts, ",") + "}"
}
