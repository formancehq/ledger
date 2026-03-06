package controller

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"

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
	"sigs.k8s.io/controller-runtime/pkg/log"

	ledgerv1alpha1 "github.com/formancehq/ledger-v3-poc/operator/api/v1alpha1"
)

const (
	agentFinalizer = "ledger.formance.com/agent-keys"
)

// LedgerClusterAgentReconciler reconciles a LedgerClusterAgent object.
type LedgerClusterAgentReconciler struct {
	client.Client

	Scheme           *runtime.Scheme
	SecretsNamespace string
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

	// Ensure Secret with Ed25519 keypair.
	secretName := agent.Name + "-agent-keys"
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      secretName,
			Namespace: r.SecretsNamespace,
		},
	}

	result, err := controllerutil.CreateOrUpdate(ctx, r.Client, secret, func() error {
		// Only generate keys on creation (when Data is empty).
		if secret.Data == nil || len(secret.Data["seed.hex"]) == 0 {
			seed, pubKey, keyID, genErr := generateEd25519KeyPair()
			if genErr != nil {
				return fmt.Errorf("generating Ed25519 keypair: %w", genErr)
			}
			secret.Data = map[string][]byte{
				"seed.hex":   []byte(hex.EncodeToString(seed)),
				"pubkey.hex": []byte(hex.EncodeToString(pubKey)),
				"key-id":     []byte(keyID),
			}
		}

		// Track the owning agent via annotations (no ownerReference for cross-namespace).
		if secret.Annotations == nil {
			secret.Annotations = make(map[string]string)
		}
		secret.Annotations["ledger.formance.com/agent-name"] = agent.Name

		return nil
	})
	if err != nil {
		logger.Error(err, "failed to reconcile agent secret")
		meta.SetStatusCondition(&agent.Status.Conditions, metav1.Condition{
			Type:               "Ready",
			Status:             metav1.ConditionFalse,
			Reason:             "SecretFailed",
			Message:            err.Error(),
			ObservedGeneration: agent.Generation,
		})
		agent.Status.Phase = "Error"
		_ = r.Status().Update(ctx, agent)

		return ctrl.Result{}, fmt.Errorf("reconciling secret: %w", err)
	}
	if result != controllerutil.OperationResultNone {
		logger.Info("reconciled agent secret", "operation", result)
	}

	// Read key-id from the secret.
	keyID := string(secret.Data["key-id"])

	// Resolve matched LedgerServices.
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

	// Update status.
	agent.Status.Phase = "Ready"
	agent.Status.KeyID = keyID
	agent.Status.SecretRef = ledgerv1alpha1.SecretReference{
		Namespace: r.SecretsNamespace,
		Name:      secretName,
	}
	agent.Status.MatchedServices = matchedServices
	agent.Status.ObservedGeneration = agent.Generation

	meta.SetStatusCondition(&agent.Status.Conditions, metav1.Condition{
		Type:               "Ready",
		Status:             metav1.ConditionTrue,
		Reason:             "KeyPairReady",
		Message:            "Ed25519 keypair is ready",
		ObservedGeneration: agent.Generation,
	})

	if err := r.Status().Update(ctx, agent); err != nil {
		return ctrl.Result{}, fmt.Errorf("updating status: %w", err)
	}

	return ctrl.Result{}, nil
}

// handleDeletion removes the agent's Secret and then removes the finalizer.
func (r *LedgerClusterAgentReconciler) handleDeletion(ctx context.Context, agent *ledgerv1alpha1.LedgerClusterAgent) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	if controllerutil.ContainsFinalizer(agent, agentFinalizer) {
		// Delete the agent's Secret.
		secret := &corev1.Secret{}
		secretKey := types.NamespacedName{
			Namespace: r.SecretsNamespace,
			Name:      agent.Name + "-agent-keys",
		}
		if err := r.Get(ctx, secretKey, secret); err != nil {
			if !apierrors.IsNotFound(err) {
				return ctrl.Result{}, fmt.Errorf("fetching agent secret for deletion: %w", err)
			}
		} else {
			if err := r.Delete(ctx, secret); err != nil && !apierrors.IsNotFound(err) {
				return ctrl.Result{}, fmt.Errorf("deleting agent secret: %w", err)
			}
			logger.Info("deleted agent secret", "secret", secretKey)
		}

		// Remove finalizer.
		controllerutil.RemoveFinalizer(agent, agentFinalizer)
		if err := r.Update(ctx, agent); err != nil {
			return ctrl.Result{}, fmt.Errorf("removing finalizer: %w", err)
		}
	}

	return ctrl.Result{}, nil
}

// resolveMatchedServices lists LedgerServices across all namespaces and returns
// those matching the agent's label selector.
func (r *LedgerClusterAgentReconciler) resolveMatchedServices(ctx context.Context, agent *ledgerv1alpha1.LedgerClusterAgent) ([]ledgerv1alpha1.MatchedService, error) {
	selector, err := metav1.LabelSelectorAsSelector(&agent.Spec.Selector)
	if err != nil {
		return nil, fmt.Errorf("parsing label selector: %w", err)
	}

	var ledgers ledgerv1alpha1.LedgerServiceList
	if err := r.List(ctx, &ledgers, &client.ListOptions{
		LabelSelector: selector,
	}); err != nil {
		return nil, fmt.Errorf("listing LedgerServices: %w", err)
	}

	matched := make([]ledgerv1alpha1.MatchedService, 0, len(ledgers.Items))
	for i := range ledgers.Items {
		// Double-check label match (belt and suspenders).
		if selector.Matches(labels.Set(ledgers.Items[i].Labels)) {
			matched = append(matched, ledgerv1alpha1.MatchedService{
				Namespace: ledgers.Items[i].Namespace,
				Name:      ledgers.Items[i].Name,
			})
		}
	}

	return matched, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *LedgerClusterAgentReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&ledgerv1alpha1.LedgerClusterAgent{}).
		Complete(r)
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
