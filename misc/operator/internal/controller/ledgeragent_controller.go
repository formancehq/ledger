package controller

import (
	"context"
	"encoding/hex"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	ledgerv1alpha1 "github.com/formancehq/ledger-v3-poc/operator/api/v1alpha1"
)

// LedgerAgentReconciler reconciles a LedgerAgent object.
type LedgerAgentReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=ledger.formance.com,resources=ledgeragents,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=ledger.formance.com,resources=ledgeragents/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=ledger.formance.com,resources=ledgeragents/finalizers,verbs=update

// Reconcile handles the reconciliation loop for LedgerAgent resources.
func (r *LedgerAgentReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	agent := &ledgerv1alpha1.LedgerAgent{}
	if err := r.Get(ctx, req.NamespacedName, agent); err != nil {
		return ctrl.Result{}, ignoreNotFound(err)
	}

	// Ensure Secret with Ed25519 keypair (same namespace as agent).
	secretName := agent.Name + "-agent-keys"
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      secretName,
			Namespace: agent.Namespace,
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

		// Set owner reference for garbage collection on agent deletion.
		return controllerutil.SetControllerReference(agent, secret, r.Scheme)
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
		_ = r.Status().Update(ctx, agent) //nolint:errcheck // best-effort status update
		return ctrl.Result{}, fmt.Errorf("reconciling secret: %w", err)
	}
	if result != controllerutil.OperationResultNone {
		logger.Info("reconciled agent secret", "operation", result)
	}

	// Read key-id from the secret.
	keyID := string(secret.Data["key-id"])

	// Resolve matched LedgerServices (same namespace only).
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
		_ = r.Status().Update(ctx, agent) //nolint:errcheck // best-effort status update
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
		Namespace: agent.Namespace,
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

// resolveMatchedServices lists LedgerServices in the same namespace and returns
// those matching the agent's label selector.
func (r *LedgerAgentReconciler) resolveMatchedServices(ctx context.Context, agent *ledgerv1alpha1.LedgerAgent) ([]ledgerv1alpha1.MatchedService, error) {
	selector, err := metav1.LabelSelectorAsSelector(&agent.Spec.Selector)
	if err != nil {
		return nil, fmt.Errorf("parsing label selector: %w", err)
	}

	var ledgers ledgerv1alpha1.LedgerServiceList
	if err := r.List(ctx, &ledgers, &client.ListOptions{
		Namespace:     agent.Namespace,
		LabelSelector: selector,
	}); err != nil {
		return nil, fmt.Errorf("listing LedgerServices: %w", err)
	}

	matched := make([]ledgerv1alpha1.MatchedService, 0, len(ledgers.Items))
	for i := range ledgers.Items {
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
func (r *LedgerAgentReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&ledgerv1alpha1.LedgerAgent{}).
		Owns(&corev1.Secret{}).
		Complete(r)
}

