package controller

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
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

	// List existing replicas across all namespaces.
	existingReplicas, err := r.listAgentSecrets(ctx, agent.Name)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("listing agent secrets: %w", err)
	}

	// Replicate (and resolve canonical key material) only when at least one target exists.
	// With no targets, the keypair is not persisted so it is not generated either.
	var (
		canonicalData map[string][]byte
		refs          = make([]ledgerv1alpha1.SecretReference, 0, len(desiredNamespaces))
	)
	if len(desiredNamespaces) > 0 {
		canonicalData, err = canonicalKeyData(existingReplicas)
		if err != nil {
			return ctrl.Result{}, fmt.Errorf("resolving canonical key data: %w", err)
		}

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

	// Garbage-collect orphan replicas in namespaces no longer in scope (also covers
	// the no-targets case: any leftover replica from a previous reconcile is removed).
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
		logger.Info("deleted orphan agent secret", "namespace", secret.Namespace, "name", secret.Name)
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
			Message:            "no matched Clusters or additional namespaces; agent keypair is not persisted",
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

// handleDeletion removes every replica of the agent's Secret and then drops the finalizer.
func (r *LedgerClusterAgentReconciler) handleDeletion(ctx context.Context, agent *ledgerv1alpha1.LedgerClusterAgent) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	if controllerutil.ContainsFinalizer(agent, agentFinalizer) {
		replicas, err := r.listAgentSecrets(ctx, agent.Name)
		if err != nil {
			return ctrl.Result{}, fmt.Errorf("listing agent secrets for deletion: %w", err)
		}
		for i := range replicas {
			secret := &replicas[i]
			if err := r.Delete(ctx, secret); err != nil && !apierrors.IsNotFound(err) {
				return ctrl.Result{}, fmt.Errorf("deleting agent secret in %q: %w", secret.Namespace, err)
			}
			logger.Info("deleted agent secret", "namespace", secret.Namespace, "name", secret.Name)
		}

		controllerutil.RemoveFinalizer(agent, agentFinalizer)
		if err := r.Update(ctx, agent); err != nil {
			return ctrl.Result{}, fmt.Errorf("removing finalizer: %w", err)
		}
	}

	return ctrl.Result{}, nil
}

// resolveMatchedServices lists Clusters across all namespaces and returns
// those matching the agent's label selector.
func (r *LedgerClusterAgentReconciler) resolveMatchedServices(ctx context.Context, agent *ledgerv1alpha1.LedgerClusterAgent) ([]ledgerv1alpha1.MatchedService, error) {
	selector, err := metav1.LabelSelectorAsSelector(&agent.Spec.Selector)
	if err != nil {
		return nil, fmt.Errorf("parsing label selector: %w", err)
	}

	var services ledgerv1alpha1.ClusterList
	if err := r.List(ctx, &services, &client.ListOptions{LabelSelector: selector}); err != nil {
		return nil, fmt.Errorf("listing Clusters: %w", err)
	}

	matched := make([]ledgerv1alpha1.MatchedService, 0, len(services.Items))
	for i := range services.Items {
		if selector.Matches(labels.Set(services.Items[i].Labels)) {
			matched = append(matched, ledgerv1alpha1.MatchedService{
				Namespace: services.Items[i].Namespace,
				Name:      services.Items[i].Name,
			})
		}
	}

	return matched, nil
}

// listAgentSecrets returns every Secret labeled as belonging to the given agent,
// across all namespaces.
func (r *LedgerClusterAgentReconciler) listAgentSecrets(ctx context.Context, agentName string) ([]corev1.Secret, error) {
	var secrets corev1.SecretList
	if err := r.List(ctx, &secrets, client.MatchingLabels{agentNameLabel: agentName}); err != nil {
		return nil, err
	}

	return secrets.Items, nil
}

// canonicalKeyData returns the canonical Secret payload for the agent. If any
// replica already exists, its data is used (all replicas carry identical content).
// Otherwise, a fresh Ed25519 keypair is generated.
func canonicalKeyData(existing []corev1.Secret) (map[string][]byte, error) {
	for i := range existing {
		if len(existing[i].Data["seed.hex"]) > 0 {
			return existing[i].Data, nil
		}
	}

	seed, pubKey, keyID, err := generateEd25519KeyPair()
	if err != nil {
		return nil, fmt.Errorf("generating Ed25519 keypair: %w", err)
	}

	return map[string][]byte{
		"seed.hex":   []byte(hex.EncodeToString(seed)),
		"pubkey.hex": []byte(hex.EncodeToString(pubKey)),
		"key-id":     []byte(keyID),
	}, nil
}

// ensureReplica creates or updates the agent's Secret in the given namespace
// with the canonical data.
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

// agentSecretName returns the name of the Secret managed by the agent.
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
