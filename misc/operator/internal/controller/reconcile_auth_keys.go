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

// credentialsKeyInfo holds the resolved key information for an credentials matching a Cluster.
type credentialsKeyInfo struct {
	// ConfigMapPrefix differentiates credentials types in the ConfigMap keys.
	ConfigMapPrefix string
	CredentialsName string
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

// reconcileAuthKeys resolves all Credentials matching the given Cluster,
// creates/updates (or deletes) a ConfigMap with aggregated auth keys, and returns the
// list of credentials key info for use by the StatefulSet reconciler.
func (r *ClusterReconciler) reconcileAuthKeys(ctx context.Context, ledger *ledgerv1alpha1.Cluster) ([]credentialsKeyInfo, error) {
	logger := log.FromContext(ctx)

	// Collect keys from cluster-scoped Credentials.
	credentials, err := r.collectClusterCredentialsKeys(ctx, ledger)
	if err != nil {
		return nil, err
	}

	logger.V(1).Info("resolved credentials keys", "clusterAgents", len(credentials))

	// Sort by prefix+name for deterministic output.
	sort.Slice(credentials, func(i, j int) bool {
		ki := credentials[i].ConfigMapPrefix + "/" + credentials[i].CredentialsName
		kj := credentials[j].ConfigMapPrefix + "/" + credentials[j].CredentialsName

		return ki < kj
	})

	cmName := authKeysConfigMapName(ledger.Name)

	if len(credentials) == 0 {
		// No credentials match: delete the ConfigMap if it exists.
		cm := &corev1.ConfigMap{}
		if err := r.Get(ctx, types.NamespacedName{Namespace: ledger.Namespace, Name: cmName}, cm); err != nil {
			if !apierrors.IsNotFound(err) {
				return nil, fmt.Errorf("checking auth-keys ConfigMap: %w", err)
			}
		} else {
			if err := r.Delete(ctx, cm); err != nil && !apierrors.IsNotFound(err) {
				return nil, fmt.Errorf("deleting auth-keys ConfigMap: %w", err)
			}
			logger.Info("deleted auth-keys ConfigMap (no matching credentials)")
		}

		return nil, nil
	}

	// Build the auth-keys.json content.
	authKeys := authKeysJSON{
		Keys: make([]authKeyEntry, 0, len(credentials)),
	}
	pubKeyData := make(map[string]string, len(credentials))

	for _, a := range credentials {
		pubKeyFileName := fmt.Sprintf("%s-%s-pubkey.hex", a.ConfigMapPrefix, a.CredentialsName)
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

	return credentials, nil
}

// collectClusterCredentialsKeys lists all Credentials and returns keys for
// those whose selector matches the given Cluster.
func (r *ClusterReconciler) collectClusterCredentialsKeys(ctx context.Context, ledger *ledgerv1alpha1.Cluster) ([]credentialsKeyInfo, error) {
	logger := log.FromContext(ctx)

	var list ledgerv1alpha1.CredentialsList
	if err := r.List(ctx, &list); err != nil {
		return nil, fmt.Errorf("listing Credentials: %w", err)
	}

	var keys []credentialsKeyInfo
	for i := range list.Items {
		cred := &list.Items[i]

		selector, err := metav1.LabelSelectorAsSelector(&cred.Spec.Selector)
		if err != nil {
			logger.Error(err, "invalid label selector on cluster credentials", "credentials", cred.Name)

			continue
		}
		if !selector.Matches(labels.Set(ledger.Labels)) {
			continue
		}

		if len(cred.Status.DistributedSecretRefs) == 0 {
			logger.Info("credentials has no distributed secret yet, skipping", "credentials", cred.Name)

			continue
		}

		info, ok, err := r.readCredentialsKeyFromSecret(ctx, cred.Name, cred.Status.DistributedSecretRefs[0], cred.Spec.Scopes, cred.Spec.God, "credentials")
		if err != nil {
			return nil, err
		}
		if ok {
			keys = append(keys, info)
		}
	}

	return keys, nil
}

// readCredentialsKeyFromSecret reads the public key from an credentials's secret and returns
// an credentialsKeyInfo. Returns (info, false, nil) if the secret is not yet ready.
func (r *ClusterReconciler) readCredentialsKeyFromSecret(
	ctx context.Context,
	credentialsName string,
	secretRef ledgerv1alpha1.SecretReference,
	scopes []string,
	god bool,
	configMapPrefix string,
) (credentialsKeyInfo, bool, error) {
	logger := log.FromContext(ctx)

	if secretRef.Name == "" {
		logger.Info("credentials secret not yet ready, skipping", "credentials", credentialsName)

		return credentialsKeyInfo{}, false, nil
	}

	secret := &corev1.Secret{}
	secretKey := types.NamespacedName{
		Namespace: secretRef.Namespace,
		Name:      secretRef.Name,
	}
	if err := r.Get(ctx, secretKey, secret); err != nil {
		if apierrors.IsNotFound(err) {
			logger.Info("credentials secret not found, skipping", "credentials", credentialsName)

			return credentialsKeyInfo{}, false, nil
		}

		return credentialsKeyInfo{}, false, fmt.Errorf("fetching secret for credentials %q: %w", credentialsName, err)
	}

	pubKeyHex := string(secret.Data["pubkey.hex"])
	keyID := string(secret.Data["key-id"])
	if pubKeyHex == "" || keyID == "" {
		logger.Info("credentials secret missing pubkey.hex or key-id, skipping", "credentials", credentialsName)

		return credentialsKeyInfo{}, false, nil
	}

	return credentialsKeyInfo{
		ConfigMapPrefix: configMapPrefix,
		CredentialsName: credentialsName,
		KeyID:           keyID,
		PublicKey:       pubKeyHex,
		Scopes:          scopes,
		God:             god,
	}, true, nil
}

// credentialsToClusters maps a Credentials change to all
// Clusters matched by its selector, triggering their re-reconciliation.
func (r *ClusterReconciler) credentialsToClusters(ctx context.Context, obj client.Object) []ctrl.Request {
	credentials, ok := obj.(*ledgerv1alpha1.Credentials)
	if !ok {
		return nil
	}

	return r.clustersMatchingSelector(ctx, &credentials.Spec.Selector, "")
}

// clustersMatchingSelector lists Clusters matching the given label selector.
// If namespace is non-empty, the search is restricted to that namespace.
func (r *ClusterReconciler) clustersMatchingSelector(ctx context.Context, ls *metav1.LabelSelector, namespace string) []ctrl.Request {
	selector, err := metav1.LabelSelectorAsSelector(ls)
	if err != nil {
		log.FromContext(ctx).Error(err, "invalid label selector on credentials")

		return nil
	}

	opts := &client.ListOptions{LabelSelector: selector}
	if namespace != "" {
		opts.Namespace = namespace
	}

	var ledgers ledgerv1alpha1.ClusterList
	if err := r.List(ctx, &ledgers, opts); err != nil {
		log.FromContext(ctx).Error(err, "failed to list Clusters for credentials mapping")

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
