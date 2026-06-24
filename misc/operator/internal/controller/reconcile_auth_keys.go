package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"sort"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	ledgerv1alpha1 "github.com/formance/ledger/operator/api/v1alpha1"
)

// agentKeyInfo holds the resolved key information for an agent matching a LedgerService.
type agentKeyInfo struct {
	// ConfigMapPrefix differentiates agent types in the ConfigMap keys.
	ConfigMapPrefix string
	AgentName       string
	KeyID           string
	PublicKey       string // hex-encoded
	Scopes          []string
	God             bool
}

// authKeysJSON is the top-level structure for the auth-keys.json file.
type authKeysJSON struct {
	Keys []authKeyEntry `json:"keys"`
}

// authKeyEntry is a single key entry in auth-keys.json.
type authKeyEntry struct {
	KeyID         string   `json:"keyId"`
	PublicKeyFile string   `json:"publicKeyFile"`
	Scopes        []string `json:"scopes"`
	God           bool     `json:"god,omitempty"`
}

// reconcileAuthKeys resolves all LedgerClusterAgents matching the given LedgerService,
// creates/updates (or deletes) a ConfigMap with aggregated auth keys, and returns the
// list of agent key info for use by the StatefulSet reconciler.
func (r *LedgerServiceReconciler) reconcileAuthKeys(ctx context.Context, ledger *ledgerv1alpha1.LedgerService) ([]agentKeyInfo, error) {
	logger := log.FromContext(ctx)

	// Collect keys from cluster-scoped LedgerClusterAgents.
	agents, err := r.collectClusterAgentKeys(ctx, ledger)
	if err != nil {
		return nil, err
	}

	logger.V(1).Info("resolved agent keys", "clusterAgents", len(agents))

	// Sort by prefix+name for deterministic output.
	sort.Slice(agents, func(i, j int) bool {
		ki := agents[i].ConfigMapPrefix + "/" + agents[i].AgentName
		kj := agents[j].ConfigMapPrefix + "/" + agents[j].AgentName

		return ki < kj
	})

	cmName := authKeysConfigMapName(ledger.Name)

	if len(agents) == 0 {
		// No agents match: delete the ConfigMap if it exists.
		cm := &corev1.ConfigMap{}
		if err := r.Get(ctx, types.NamespacedName{Namespace: ledger.Namespace, Name: cmName}, cm); err != nil {
			if !apierrors.IsNotFound(err) {
				return nil, fmt.Errorf("checking auth-keys ConfigMap: %w", err)
			}
		} else {
			if err := r.Delete(ctx, cm); err != nil && !apierrors.IsNotFound(err) {
				return nil, fmt.Errorf("deleting auth-keys ConfigMap: %w", err)
			}
			logger.Info("deleted auth-keys ConfigMap (no matching agents)")
		}

		return nil, nil
	}

	// Build the auth-keys.json content.
	authKeys := authKeysJSON{
		Keys: make([]authKeyEntry, 0, len(agents)),
	}
	pubKeyData := make(map[string]string, len(agents))

	for _, a := range agents {
		pubKeyFileName := fmt.Sprintf("%s-%s-pubkey.hex", a.ConfigMapPrefix, a.AgentName)
		authKeys.Keys = append(authKeys.Keys, authKeyEntry{
			KeyID:         a.KeyID,
			PublicKeyFile: "/auth-keys/" + pubKeyFileName,
			Scopes:        a.Scopes,
			God:           a.God,
		})
		pubKeyData[pubKeyFileName] = a.PublicKey
	}

	authKeysBytes, err := json.MarshalIndent(authKeys, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("marshaling auth-keys.json: %w", err)
	}

	// Create or update the ConfigMap.
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cmName,
			Namespace: ledger.Namespace,
		},
	}

	_, err = controllerutil.CreateOrUpdate(ctx, r.Client, cm, func() error {
		cm.Labels = commonLabels(ledger)
		cm.Data = map[string]string{
			"auth-keys.json": string(authKeysBytes),
		}
		maps.Copy(cm.Data, pubKeyData)

		return controllerutil.SetControllerReference(ledger, cm, r.Scheme)
	})
	if err != nil {
		return nil, fmt.Errorf("reconciling auth-keys ConfigMap: %w", err)
	}

	return agents, nil
}

// collectClusterAgentKeys lists all LedgerClusterAgents and returns keys for those
// whose selector matches the given LedgerService.
func (r *LedgerServiceReconciler) collectClusterAgentKeys(ctx context.Context, ledger *ledgerv1alpha1.LedgerService) ([]agentKeyInfo, error) {
	logger := log.FromContext(ctx)

	var agentList ledgerv1alpha1.LedgerClusterAgentList
	if err := r.List(ctx, &agentList); err != nil {
		return nil, fmt.Errorf("listing LedgerClusterAgents: %w", err)
	}

	var agents []agentKeyInfo
	for i := range agentList.Items {
		agent := &agentList.Items[i]

		selector, err := metav1.LabelSelectorAsSelector(&agent.Spec.Selector)
		if err != nil {
			logger.Error(err, "invalid label selector on cluster agent", "agent", agent.Name)

			continue
		}
		if !selector.Matches(labels.Set(ledger.Labels)) {
			continue
		}

		if len(agent.Status.DistributedSecretRefs) == 0 {
			logger.Info("agent has no distributed secret yet, skipping", "agent", agent.Name)

			continue
		}

		info, ok, err := r.readAgentKeyFromSecret(ctx, agent.Name, agent.Status.DistributedSecretRefs[0], agent.Spec.Scopes, agent.Spec.God, "agent")
		if err != nil {
			return nil, err
		}
		if ok {
			agents = append(agents, info)
		}
	}

	return agents, nil
}

// readAgentKeyFromSecret reads the public key from an agent's secret and returns
// an agentKeyInfo. Returns (info, false, nil) if the secret is not yet ready.
func (r *LedgerServiceReconciler) readAgentKeyFromSecret(
	ctx context.Context,
	agentName string,
	secretRef ledgerv1alpha1.SecretReference,
	scopes []string,
	god bool,
	configMapPrefix string,
) (agentKeyInfo, bool, error) {
	logger := log.FromContext(ctx)

	if secretRef.Name == "" {
		logger.Info("agent secret not yet ready, skipping", "agent", agentName)

		return agentKeyInfo{}, false, nil
	}

	secret := &corev1.Secret{}
	secretKey := types.NamespacedName{
		Namespace: secretRef.Namespace,
		Name:      secretRef.Name,
	}
	if err := r.Get(ctx, secretKey, secret); err != nil {
		if apierrors.IsNotFound(err) {
			logger.Info("agent secret not found, skipping", "agent", agentName)

			return agentKeyInfo{}, false, nil
		}

		return agentKeyInfo{}, false, fmt.Errorf("fetching secret for agent %q: %w", agentName, err)
	}

	pubKeyHex := string(secret.Data["pubkey.hex"])
	keyID := string(secret.Data["key-id"])
	if pubKeyHex == "" || keyID == "" {
		logger.Info("agent secret missing pubkey.hex or key-id, skipping", "agent", agentName)

		return agentKeyInfo{}, false, nil
	}

	return agentKeyInfo{
		ConfigMapPrefix: configMapPrefix,
		AgentName:       agentName,
		KeyID:           keyID,
		PublicKey:       pubKeyHex,
		Scopes:          scopes,
		God:             god,
	}, true, nil
}

// ledgerClusterAgentToLedgerServices maps a LedgerClusterAgent change to all
// LedgerServices matched by its selector, triggering their re-reconciliation.
func (r *LedgerServiceReconciler) ledgerClusterAgentToLedgerServices(ctx context.Context, obj client.Object) []ctrl.Request {
	agent, ok := obj.(*ledgerv1alpha1.LedgerClusterAgent)
	if !ok {
		return nil
	}

	return r.ledgerServicesMatchingSelector(ctx, &agent.Spec.Selector, "")
}

// ledgerServicesMatchingSelector lists LedgerServices matching the given label selector.
// If namespace is non-empty, the search is restricted to that namespace.
func (r *LedgerServiceReconciler) ledgerServicesMatchingSelector(ctx context.Context, ls *metav1.LabelSelector, namespace string) []ctrl.Request {
	selector, err := metav1.LabelSelectorAsSelector(ls)
	if err != nil {
		log.FromContext(ctx).Error(err, "invalid label selector on agent")

		return nil
	}

	opts := &client.ListOptions{LabelSelector: selector}
	if namespace != "" {
		opts.Namespace = namespace
	}

	var ledgers ledgerv1alpha1.LedgerServiceList
	if err := r.List(ctx, &ledgers, opts); err != nil {
		log.FromContext(ctx).Error(err, "failed to list LedgerServices for agent mapping")

		return nil
	}

	var requests []ctrl.Request
	for i := range ledgers.Items {
		if selector.Matches(labels.Set(ledgers.Items[i].Labels)) {
			requests = append(requests, ctrl.Request{
				NamespacedName: types.NamespacedName{
					Name:      ledgers.Items[i].Name,
					Namespace: ledgers.Items[i].Namespace,
				},
			})
		}
	}

	return requests
}
