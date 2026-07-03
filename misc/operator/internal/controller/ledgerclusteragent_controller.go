package controller

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"

	ledgerv1alpha1 "github.com/formance/ledger/operator/api/v1alpha1"
)

const (
	agentFinalizer = "ledger.formance.com/agent-keys"
	agentNameLabel = "ledger.formance.com/agent-name"
)

// LedgerClusterAgentReconciler reconciles a LedgerClusterAgent object.
type LedgerClusterAgentReconciler struct {
	client.Client

	Scheme *runtime.Scheme

	// OperatorNamespace is where the canonical seed Secret of every
	// LedgerClusterAgent is stored. Injected at construction (from
	// DiscoverOperatorNamespace in production, from a fixed namespace in
	// envtest) so the reconciler does not depend on process-global state.
	OperatorNamespace string

	// APIReader is an uncached reader used exclusively for canonical Secret
	// reads in the operator's own namespace. Going through the manager's
	// cached client would either force us to widen --watch-namespace scope
	// (surprising for multi-tenant deployments) or hit a cache-miss. Writes
	// always bypass the cache and use the regular Client.
	APIReader client.Reader
}

// +kubebuilder:rbac:groups=ledger.formance.com,resources=ledgerclusteragents,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=ledger.formance.com,resources=ledgerclusteragents/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=ledger.formance.com,resources=ledgerclusteragents/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch;create;update;patch;delete

// Reconcile handles the reconciliation loop for LedgerClusterAgent resources.
func (r *LedgerClusterAgentReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	agent := &ledgerv1alpha1.LedgerClusterAgent{}
	if err := r.Get(ctx, req.NamespacedName, agent); err != nil {
		return ctrl.Result{}, ignoreNotFound(err)
	}

	// Handle deletion with finalizer.
	if !agent.DeletionTimestamp.IsZero() {
		return r.handleDeletion(ctx, agent)
	}

	// Ensure finalizer is present.
	if !controllerutil.ContainsFinalizer(agent, agentFinalizer) {
		controllerutil.AddFinalizer(agent, agentFinalizer)
		if err := r.Update(ctx, agent); err != nil {
			return ctrl.Result{}, fmt.Errorf("adding finalizer: %w", err)
		}
	}

	// Resolve matched services.
	matchedServices, err := r.resolveMatchedServices(ctx, agent)
	if err != nil {
		logger.Error(err, "failed to resolve matched services")
		meta.SetStatusCondition(&agent.Status.Conditions, metav1.Condition{
			Type:               "SelectorResolved",
			Status:             metav1.ConditionFalse,
			Reason:             "SelectorFailed",
			Message:            err.Error(),
			ObservedGeneration: agent.Generation,
		})
		agent.Status.Phase = "Error"
		_ = r.Status().Update(ctx, agent)

		return ctrl.Result{}, fmt.Errorf("resolving matched services: %w", err)
	}

	meta.SetStatusCondition(&agent.Status.Conditions, metav1.Condition{
		Type:               "SelectorResolved",
		Status:             metav1.ConditionTrue,
		Reason:             "Resolved",
		Message:            fmt.Sprintf("matched %d service(s)", len(matchedServices)),
		ObservedGeneration: agent.Generation,
	})

	// Compute the desired target namespaces (matched services + additional).
	desiredNamespaces := computeDesiredNamespaces(matchedServices, agent.Spec.AdditionalNamespaces)

	// List existing replicas across all namespaces (canonical excluded via label filter).
	existingReplicas, err := r.listAgentReplicaSecrets(ctx, agent.Name)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("listing agent replica secrets: %w", err)
	}

	// Resolve canonical seed material. The canonical Secret lives in the
	// operator's namespace and is the sole source of truth for the seed;
	// per-target replicas are pure projections of its content.
	//
	// Bootstrap conditions: at least one desired target OR at least one
	// existing replica. The second case covers the no-target upgrade path:
	// legacy replicas from a pre-canonical operator would otherwise be
	// deleted by the aggressive GC below before their seed could be
	// adopted, permanently losing the identity.
	var (
		canonicalData map[string][]byte
		refs          = make([]ledgerv1alpha1.SecretReference, 0, len(desiredNamespaces))
	)
	if len(desiredNamespaces) > 0 || len(existingReplicas) > 0 {
		canonicalData, err = r.ensureCanonicalSecret(ctx, agent, existingReplicas)
		if err != nil {
			meta.SetStatusCondition(&agent.Status.Conditions, metav1.Condition{
				Type:               "Ready",
				Status:             metav1.ConditionFalse,
				Reason:             "CanonicalFailed",
				Message:            err.Error(),
				ObservedGeneration: agent.Generation,
			})
			agent.Status.Phase = "Error"
			_ = r.Status().Update(ctx, agent)

			return ctrl.Result{}, fmt.Errorf("ensuring canonical secret: %w", err)
		}
	}

	if len(desiredNamespaces) > 0 {
		for _, ns := range desiredNamespaces {
			if err := r.ensureReplica(ctx, agent, ns, canonicalData); err != nil {
				meta.SetStatusCondition(&agent.Status.Conditions, metav1.Condition{
					Type:               "Ready",
					Status:             metav1.ConditionFalse,
					Reason:             "SecretFailed",
					Message:            err.Error(),
					ObservedGeneration: agent.Generation,
				})
				agent.Status.Phase = "Error"
				_ = r.Status().Update(ctx, agent)

				return ctrl.Result{}, fmt.Errorf("ensuring secret in %q: %w", ns, err)
			}
			refs = append(refs, ledgerv1alpha1.SecretReference{
				Namespace: ns,
				Name:      agentSecretName(agent),
			})
		}
	}

	// Aggressively garbage-collect replica Secrets in namespaces no longer
	// in scope. Because the seed lives on the canonical Secret in the
	// operator namespace, deleting stale replicas can never destroy the
	// canonical key material — the "seed vanishes when the last target
	// disappears" hazard the previous design had is gone.
	desiredSet := make(map[string]struct{}, len(desiredNamespaces))
	for _, ns := range desiredNamespaces {
		desiredSet[ns] = struct{}{}
	}
	for i := range existingReplicas {
		secret := &existingReplicas[i]
		if _, keep := desiredSet[secret.Namespace]; keep {
			continue
		}
		if err := r.Delete(ctx, secret); err != nil && !apierrors.IsNotFound(err) {
			return ctrl.Result{}, fmt.Errorf("deleting orphan secret in %q: %w", secret.Namespace, err)
		}
		logger.Info("deleted orphan agent replica secret", "namespace", secret.Namespace, "name", secret.Name)
	}

	agent.Status.MatchedServices = matchedServices
	agent.Status.DistributedSecretRefs = refs
	agent.Status.ObservedGeneration = agent.Generation

	if len(refs) == 0 {
		agent.Status.KeyID = ""
		agent.Status.Phase = "Pending"
		meta.SetStatusCondition(&agent.Status.Conditions, metav1.Condition{
			Type:               "Ready",
			Status:             metav1.ConditionFalse,
			Reason:             "NoTargets",
			Message:            "no matched Clusters or additional namespaces; canonical seed (if any) is preserved for later reuse",
			ObservedGeneration: agent.Generation,
		})
	} else {
		agent.Status.KeyID = string(canonicalData["key-id"])
		agent.Status.Phase = "Ready"
		meta.SetStatusCondition(&agent.Status.Conditions, metav1.Condition{
			Type:               "Ready",
			Status:             metav1.ConditionTrue,
			Reason:             "KeyPairReady",
			Message:            fmt.Sprintf("Ed25519 keypair distributed to %d namespace(s)", len(refs)),
			ObservedGeneration: agent.Generation,
		})
	}

	if err := r.Status().Update(ctx, agent); err != nil {
		return ctrl.Result{}, fmt.Errorf("updating status: %w", err)
	}

	return ctrl.Result{}, nil
}

// handleDeletion removes the canonical Secret and every replica of the
// agent's Secret, then drops the finalizer.
func (r *LedgerClusterAgentReconciler) handleDeletion(ctx context.Context, agent *ledgerv1alpha1.LedgerClusterAgent) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	if controllerutil.ContainsFinalizer(agent, agentFinalizer) {
		replicas, err := r.listAgentReplicaSecrets(ctx, agent.Name)
		if err != nil {
			return ctrl.Result{}, fmt.Errorf("listing agent secrets for deletion: %w", err)
		}
		for i := range replicas {
			secret := &replicas[i]
			if err := r.Delete(ctx, secret); err != nil && !apierrors.IsNotFound(err) {
				return ctrl.Result{}, fmt.Errorf("deleting agent replica secret in %q: %w", secret.Namespace, err)
			}
			logger.Info("deleted agent replica secret", "namespace", secret.Namespace, "name", secret.Name)
		}

		canonical := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:      agentCanonicalSecretName(agent.Name),
				Namespace: r.OperatorNamespace,
			},
		}
		if err := r.Delete(ctx, canonical); err != nil && !apierrors.IsNotFound(err) {
			return ctrl.Result{}, fmt.Errorf("deleting canonical secret: %w", err)
		}
		logger.Info("deleted agent canonical secret", "namespace", r.OperatorNamespace, "name", canonical.Name)

		controllerutil.RemoveFinalizer(agent, agentFinalizer)
		if err := r.Update(ctx, agent); err != nil {
			return ctrl.Result{}, fmt.Errorf("removing finalizer: %w", err)
		}
	}

	return ctrl.Result{}, nil
}

// resolveMatchedServices lists Clusters (and, for the migration window,
// legacy LedgerServices) across all namespaces and returns those matching
// the agent's label selector. LedgerServices are included so that during
// an in-place upgrade the agent does not see matched=∅ and treat every
// existing replica Secret as an orphan to garbage-collect: that path
// rotates keys mid-upgrade and breaks Cluster auth before the migrator
// has had a chance to run. Once a legacy LS has been migrated it still
// exists as a deprecated shadow but shares the Cluster's namespace+name,
// so the resulting MatchedService list is de-duplicated.
func (r *LedgerClusterAgentReconciler) resolveMatchedServices(ctx context.Context, agent *ledgerv1alpha1.LedgerClusterAgent) ([]ledgerv1alpha1.MatchedService, error) {
	selector, err := metav1.LabelSelectorAsSelector(&agent.Spec.Selector)
	if err != nil {
		return nil, fmt.Errorf("parsing label selector: %w", err)
	}

	var clusters ledgerv1alpha1.ClusterList
	if err := r.List(ctx, &clusters, &client.ListOptions{LabelSelector: selector}); err != nil {
		return nil, fmt.Errorf("listing Clusters: %w", err)
	}

	seen := make(map[string]struct{}, len(clusters.Items))
	matched := make([]ledgerv1alpha1.MatchedService, 0, len(clusters.Items))
	for i := range clusters.Items {
		if !selector.Matches(labels.Set(clusters.Items[i].Labels)) {
			continue
		}
		key := clusters.Items[i].Namespace + "/" + clusters.Items[i].Name
		if _, dup := seen[key]; dup {
			continue
		}
		seen[key] = struct{}{}
		matched = append(matched, ledgerv1alpha1.MatchedService{
			Namespace: clusters.Items[i].Namespace,
			Name:      clusters.Items[i].Name,
		})
	}

	var legacy ledgerv1alpha1.LedgerServiceList
	if err := r.List(ctx, &legacy, &client.ListOptions{LabelSelector: selector}); err != nil {
		return nil, fmt.Errorf("listing LedgerServices: %w", err)
	}
	for i := range legacy.Items {
		if !selector.Matches(labels.Set(legacy.Items[i].Labels)) {
			continue
		}
		key := legacy.Items[i].Namespace + "/" + legacy.Items[i].Name
		if _, dup := seen[key]; dup {
			continue
		}
		seen[key] = struct{}{}
		matched = append(matched, ledgerv1alpha1.MatchedService{
			Namespace: legacy.Items[i].Namespace,
			Name:      legacy.Items[i].Name,
		})
	}

	return matched, nil
}

// listAgentReplicaSecrets returns every replica Secret belonging to the given
// agent, across all namespaces. The canonical Secret is identified by
// name + namespace and excluded from the result — filtering by name (rather
// than by an additional label) means Secrets created by pre-canonical
// versions of the operator are still discovered, so upgrade adopts them
// instead of orphaning them.
func (r *LedgerClusterAgentReconciler) listAgentReplicaSecrets(ctx context.Context, agentName string) ([]corev1.Secret, error) {
	var secrets corev1.SecretList
	if err := r.List(ctx, &secrets, client.MatchingLabels{
		agentNameLabel: agentName,
	}); err != nil {
		return nil, err
	}

	canonicalName := agentCanonicalSecretName(agentName)

	filtered := secrets.Items[:0]
	for _, s := range secrets.Items {
		if s.Name == canonicalName && s.Namespace == r.OperatorNamespace {
			continue
		}
		filtered = append(filtered, s)
	}

	return filtered, nil
}

// ensureCanonicalSecret creates the canonical seed Secret in the operator's
// namespace on first call for the agent, and returns its data on every
// subsequent call. The seed is generated exactly once and the Secret is
// never updated after creation — the canonical value is stable across the
// agent's lifetime, independent of the manager cache configuration.
//
// Reads go through r.APIReader (uncached) to avoid forcing the caller to
// widen --watch-namespace scope for every controller in the manager just so
// the operator namespace is covered. Writes hit the API server directly
// regardless of cache.
//
// Upgrade path: existingReplicas is the set of Secrets carrying seed material
// under the older (canonical-less) layout. When bootstrapping the canonical
// for the first time, we adopt the seed from one of them instead of
// generating fresh material — otherwise upgrading the operator would
// silently invalidate every bundle already handed out.
func (r *LedgerClusterAgentReconciler) ensureCanonicalSecret(ctx context.Context, agent *ledgerv1alpha1.LedgerClusterAgent, existingReplicas []corev1.Secret) (map[string][]byte, error) {
	if r.OperatorNamespace == "" {
		return nil, errors.New("operator namespace not configured")
	}
	if r.APIReader == nil {
		return nil, errors.New("APIReader not configured")
	}

	key := types.NamespacedName{
		Name:      agentCanonicalSecretName(agent.Name),
		Namespace: r.OperatorNamespace,
	}

	existing := &corev1.Secret{}
	switch err := r.APIReader.Get(ctx, key, existing); {
	case err == nil:
		if len(existing.Data[keySeedHex]) == 0 {
			return nil, fmt.Errorf("canonical secret %s/%s exists but has no seed", key.Namespace, key.Name)
		}

		return existing.Data, nil
	case !apierrors.IsNotFound(err):
		return nil, fmt.Errorf("reading canonical secret: %w", err)
	}

	// Not found — mint a new canonical, adopting a legacy replica's seed
	// when available to keep bundles valid across upgrades.
	fresh := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      key.Name,
			Namespace: key.Namespace,
			Labels: map[string]string{
				agentNameLabel: agent.Name,
			},
		},
	}

	if adopted := adoptSeedFromReplica(existingReplicas); adopted != nil {
		fresh.Data = adopted
	} else {
		seed, pubKey, keyID, err := generateEd25519KeyPair()
		if err != nil {
			return nil, fmt.Errorf("generating Ed25519 keypair: %w", err)
		}
		fresh.Data = map[string][]byte{
			keySeedHex:   []byte(hex.EncodeToString(seed)),
			keyPubKeyHex: []byte(hex.EncodeToString(pubKey)),
			keyKeyID:     []byte(keyID),
		}
	}

	if err := r.Create(ctx, fresh); err != nil {
		if apierrors.IsAlreadyExists(err) {
			// A concurrent reconcile beat us to it. Re-read via APIReader
			// (the write may not be visible on the cached client yet).
			if err := r.APIReader.Get(ctx, key, existing); err != nil {
				return nil, fmt.Errorf("re-reading canonical secret after AlreadyExists: %w", err)
			}

			return existing.Data, nil
		}

		return nil, fmt.Errorf("creating canonical secret: %w", err)
	}

	return fresh.Data, nil
}

// adoptSeedFromReplica returns a copy of the seed material found on any
// existing replica, or nil when no candidate carries a seed. The candidate
// pool is sorted deterministically by namespace/name so the choice is stable
// across reconciles when multiple replicas exist (all replicas should carry
// the same content, but we do not rely on that invariant here).
func adoptSeedFromReplica(replicas []corev1.Secret) map[string][]byte {
	candidates := make([]*corev1.Secret, 0, len(replicas))
	for i := range replicas {
		if len(replicas[i].Data[keySeedHex]) > 0 {
			candidates = append(candidates, &replicas[i])
		}
	}
	if len(candidates) == 0 {
		return nil
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].Namespace != candidates[j].Namespace {
			return candidates[i].Namespace < candidates[j].Namespace
		}

		return candidates[i].Name < candidates[j].Name
	})

	src := candidates[0].Data

	return map[string][]byte{
		keySeedHex:   append([]byte(nil), src[keySeedHex]...),
		keyPubKeyHex: append([]byte(nil), src[keyPubKeyHex]...),
		keyKeyID:     append([]byte(nil), src[keyKeyID]...),
	}
}

// ensureReplica creates or updates the agent's replica Secret in the given
// namespace with a projection of the canonical data.
func (r *LedgerClusterAgentReconciler) ensureReplica(ctx context.Context, agent *ledgerv1alpha1.LedgerClusterAgent, namespace string, data map[string][]byte) error {
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      agentSecretName(agent),
			Namespace: namespace,
		},
	}

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, secret, func() error {
		if secret.Labels == nil {
			secret.Labels = make(map[string]string, 1)
		}
		secret.Labels[agentNameLabel] = agent.Name
		secret.Data = data

		return nil
	})

	return err
}

// computeDesiredNamespaces returns the sorted, deduplicated list of namespaces
// that must hold a replica of the agent's Secret.
func computeDesiredNamespaces(matched []ledgerv1alpha1.MatchedService, additional []string) []string {
	set := make(map[string]struct{}, len(matched)+len(additional))
	for _, m := range matched {
		if m.Namespace != "" {
			set[m.Namespace] = struct{}{}
		}
	}
	for _, ns := range additional {
		if ns != "" {
			set[ns] = struct{}{}
		}
	}

	out := make([]string, 0, len(set))
	for ns := range set {
		out = append(out, ns)
	}
	sort.Strings(out)

	return out
}

// agentSecretName returns the name of the replica Secret managed by the agent.
func agentSecretName(agent *ledgerv1alpha1.LedgerClusterAgent) string {
	return prefixedName(agent.Name) + "-agent-keys"
}

// SetupWithManager sets up the controller with the Manager.
func (r *LedgerClusterAgentReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&ledgerv1alpha1.LedgerClusterAgent{}).
		Watches(&ledgerv1alpha1.Cluster{},
			handler.EnqueueRequestsFromMapFunc(r.clusterToAgents)).
		Complete(r)
}

// clusterToAgents maps a Cluster change to all LedgerClusterAgents
// whose selector matches the service, so replica state is kept in sync with
// service membership and namespace placement.
func (r *LedgerClusterAgentReconciler) clusterToAgents(ctx context.Context, obj client.Object) []ctrl.Request {
	logger := log.FromContext(ctx)

	service, ok := obj.(*ledgerv1alpha1.Cluster)
	if !ok {
		return nil
	}

	var agents ledgerv1alpha1.LedgerClusterAgentList
	if err := r.List(ctx, &agents); err != nil {
		logger.Error(err, "listing LedgerClusterAgents for service mapping")

		return nil
	}

	requests := make([]ctrl.Request, 0)
	for i := range agents.Items {
		agent := &agents.Items[i]
		selector, err := metav1.LabelSelectorAsSelector(&agent.Spec.Selector)
		if err != nil {
			continue
		}
		if selector.Matches(labels.Set(service.Labels)) {
			requests = append(requests, ctrl.Request{
				NamespacedName: types.NamespacedName{Name: agent.Name},
			})
		}
	}

	return requests
}

const (
	keySeedHex   = "seed.hex"
	keyPubKeyHex = "pubkey.hex"
	keyKeyID     = "key-id"
)

// generateEd25519KeyPair generates a new Ed25519 keypair and returns the seed,
// public key, and a key ID (SHA-256 fingerprint prefix, 16 hex chars).
func generateEd25519KeyPair() (seed, pubKey []byte, keyID string, err error) {
	seed = make([]byte, ed25519.SeedSize)
	if _, err = rand.Read(seed); err != nil {
		return nil, nil, "", fmt.Errorf("reading random bytes: %w", err)
	}

	privKey := ed25519.NewKeyFromSeed(seed)
	pubKey = privKey.Public().(ed25519.PublicKey)

	hash := sha256.Sum256(pubKey)
	keyID = hex.EncodeToString(hash[:8])

	return seed, pubKey, keyID, nil
}
