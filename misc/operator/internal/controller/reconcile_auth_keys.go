package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"sort"
	"time"

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

// authKeysPendingRequeueInterval is how soon to requeue a Cluster whose matching
// Credentials are not distributed yet. The Credentials watch already re-enqueues
// on distribution, so this poll is a safety net bounding convergence when the
// watch event is missed, not the primary trigger.
const authKeysPendingRequeueInterval = 10 * time.Second

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
//
// It distinguishes two zero-key situations that must NOT be conflated (EN-1487):
//
//   - No Credentials match the selector at all — a legitimate removal. The
//     ConfigMap is deleted and the caller strips the auth wiring from the
//     StatefulSet.
//   - One or more Credentials match but none is distributed yet (their
//     status.DistributedSecretRefs is transiently empty, e.g. during
//     operator/Credentials churn). Deleting the ConfigMap and stripping the
//     StatefulSet here would produce AUTH_ENABLED=true without any key and
//     crash-loop an otherwise healthy cluster. This is a transient state, so we
//     fail safe: preserve the existing ConfigMap and StatefulSet wiring, and
//     report pending=true so the caller sets the AuthKeysPending condition and
//     requeues. The Credentials watch reconverges once distribution completes.
//
// The returned pending flag is true only in the second case.
func (r *ClusterReconciler) reconcileAuthKeys(ctx context.Context, ledger *ledgerv1alpha1.Cluster) ([]credentialsKeyInfo, bool, error) {
	logger := log.FromContext(ctx)

	// Collect keys from cluster-scoped Credentials. matched counts Credentials
	// whose selector matches this Cluster, regardless of distribution state;
	// credentials holds only those already distributed and readable.
	credentials, matched, err := r.collectClusterCredentialsKeys(ctx, ledger)
	if err != nil {
		return nil, false, err
	}

	logger.V(1).Info("resolved credentials keys", "matched", matched, "resolved", len(credentials))

	// Sort by prefix+name for deterministic output.
	sort.Slice(credentials, func(i, j int) bool {
		ki := credentials[i].ConfigMapPrefix + "/" + credentials[i].CredentialsName
		kj := credentials[j].ConfigMapPrefix + "/" + credentials[j].CredentialsName

		return ki < kj
	})

	cmName := authKeysConfigMapName(ledger.Name)

	// Fail-safe (EN-1487): if ANY selector-matching Credentials is not yet
	// distributed (matched > len(credentials)), one or more configured keys are
	// transiently unresolved. Rewriting the ConfigMap from the resolved subset
	// (or deleting it when the subset is empty) would drop those keys and roll
	// the StatefulSet with an auth-keys hash that omits them, dropping
	// previously configured Ed25519 keys from a healthy cluster during
	// Credentials churn. This covers both the zero-resolved case and the
	// partially-resolved case — the latter is just as dangerous.
	//
	// The guard only matters when a missing key would crash-loop the cluster,
	// i.e. when buildEnvVars would emit AUTH_ED25519_KEYS. For a cluster with
	// auth explicitly disabled that env var is never set (see envvars.go), so
	// preserving stale wiring buys no safety and halting the StatefulSet pass
	// would needlessly stall bootstrap/updates during Credentials churn. Mirror
	// buildEnvVars' authExplicitlyDisabled gate.
	authExplicitlyDisabled := ledger.Spec.Auth != nil && ledger.Spec.Auth.Enabled != nil && !*ledger.Spec.Auth.Enabled
	if matched > len(credentials) && !authExplicitlyDisabled {
		// Transient: some matching Credentials are not distributed yet. Fail
		// safe — do not touch the ConfigMap or the StatefulSet auth wiring.
		// The caller sets the AuthKeysPending condition and requeues.
		logger.Info("some matching credentials are not distributed yet, preserving existing auth-key wiring",
			"matched", matched, "resolved", len(credentials))

		return nil, true, nil
	}

	if len(credentials) == 0 {
		// No credentials match (matched == 0), or all matched are resolved but
		// there are none — delete the ConfigMap if it exists.
		cm := &corev1.ConfigMap{}
		if err := r.Get(ctx, types.NamespacedName{Namespace: ledger.Namespace, Name: cmName}, cm); err != nil {
			if !apierrors.IsNotFound(err) {
				return nil, false, fmt.Errorf("checking auth-keys ConfigMap: %w", err)
			}
		} else {
			if err := r.Delete(ctx, cm); err != nil && !apierrors.IsNotFound(err) {
				return nil, false, fmt.Errorf("deleting auth-keys ConfigMap: %w", err)
			}
			logger.Info("deleted auth-keys ConfigMap (no matching credentials)")
		}

		return nil, false, nil
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
		return nil, false, fmt.Errorf("marshaling auth-keys.json: %w", err)
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
		return nil, false, fmt.Errorf("reconciling auth-keys ConfigMap: %w", err)
	}

	return credentials, false, nil
}

// collectClusterCredentialsKeys lists all Credentials and returns keys for
// those whose selector matches the given Cluster. It also returns matched: the
// number of Credentials whose selector matches this Cluster, counted before any
// distribution filtering. matched >= len(keys) always; the gap is the set of
// matching-but-not-yet-distributed Credentials that the caller uses to tell a
// legitimate removal (matched == 0) apart from a transient non-distribution
// (matched > 0, len(keys) == 0) — see reconcileAuthKeys.
func (r *ClusterReconciler) collectClusterCredentialsKeys(ctx context.Context, ledger *ledgerv1alpha1.Cluster) ([]credentialsKeyInfo, int, error) {
	logger := log.FromContext(ctx)

	var list ledgerv1alpha1.CredentialsList
	if err := r.List(ctx, &list); err != nil {
		return nil, 0, fmt.Errorf("listing Credentials: %w", err)
	}

	var keys []credentialsKeyInfo
	matched := 0
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

		matched++

		if len(cred.Status.DistributedSecretRefs) == 0 {
			logger.Info("credentials has no distributed secret yet, skipping", "credentials", cred.Name)

			continue
		}

		info, ok, err := r.readCredentialsKeyFromSecret(ctx, cred.Name, cred.Status.DistributedSecretRefs[0], cred.Spec.Scopes, cred.Spec.God, "credentials")
		if err != nil {
			return nil, 0, err
		}
		if ok {
			keys = append(keys, info)
		}
	}

	return keys, matched, nil
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
