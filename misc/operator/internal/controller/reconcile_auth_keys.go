package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"path"
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

// errMalformedAuthKeys marks a failure that comes from the CONTENT of an
// existing auth-keys ConfigMap being corrupt (unparseable auth-keys.json), as
// opposed to the ConfigMap read (the API Get) itself failing. The caller treats
// the two very differently: malformed content is self-healable — the reconcile
// proceeds treating the corrupt prior state as absent and rebuilds from the
// freshly-resolved set — whereas an API read error is transient and must NOT be
// swallowed, because assuming "no prior key" on a transient blip would silently
// drop a still-authorized key during partial resolution.
var errMalformedAuthKeys = errors.New("malformed existing auth-keys ConfigMap")

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

// unresolvedCredential identifies a selector-matching Credentials whose key is
// transiently not distributed yet. It carries only the stable identity needed to
// look the credential's prior key up in the existing ConfigMap for carry-forward
// (EN-1491): the ConfigMap prefix + credential name, which together form the
// pubkey filename produced by pubKeyFileName. No secret content is available
// (that is precisely what is missing), so no KeyID/PublicKey/Scopes are carried.
type unresolvedCredential struct {
	ConfigMapPrefix string
	CredentialsName string
}

// pubKeyFileName returns the ConfigMap key under which a credential's hex public
// key is stored. It is the stable per-credential identity used both to write a
// resolved key and to carry a still-pending key forward from the existing
// ConfigMap. The auth-keys.json entry references it via PublicKeyFile
// ("/auth-keys/" + pubKeyFileName).
func pubKeyFileName(configMapPrefix, credentialsName string) string {
	return fmt.Sprintf("%s-%s-pubkey.hex", configMapPrefix, credentialsName)
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
// It distinguishes THREE situations that must NOT be conflated (EN-1487 / EN-1491):
//
//   - matched == 0: no Credentials match the selector at all — a legitimate
//     removal. The ConfigMap is deleted and the caller strips the auth wiring
//     from the StatefulSet.
//   - matched >= 1 and ZERO resolved: one or more Credentials match but none is
//     distributed yet (their status.DistributedSecretRefs is transiently empty,
//     e.g. during operator/Credentials churn). Deleting the ConfigMap and
//     stripping the StatefulSet here would produce AUTH_ENABLED=true without any
//     key and crash-loop an otherwise healthy cluster. This is a transient
//     state, so we fail safe: preserve the existing ConfigMap and StatefulSet
//     wiring, and report pending=true so the caller sets the AuthKeysPending
//     condition and requeues. The Credentials watch reconverges once
//     distribution completes.
//   - matched >= 1, SOME resolved and SOME transiently non-distributed
//     (partial): do NOT freeze the whole cluster (freezing means a single
//     permanently-broken Credentials would block key rotation/propagation for
//     ALL keys indefinitely). Instead build the ConfigMap from the UNION of the
//     freshly-resolved keys and each still-unresolved matched credential's
//     previously-stored key, carried forward from the existing ConfigMap. This
//     propagates newly-resolved/rotated keys while preserving each individual
//     pending key; a permanently-broken Credentials only ever keeps its own
//     last-known key. Proceed with the StatefulSet pass normally (pending=false).
//
// The returned pending flag is true only in the second case.
func (r *ClusterReconciler) reconcileAuthKeys(ctx context.Context, ledger *ledgerv1alpha1.Cluster) ([]credentialsKeyInfo, bool, error) {
	logger := log.FromContext(ctx)

	// Collect keys from cluster-scoped Credentials. matched counts Credentials
	// whose selector matches this Cluster, regardless of distribution state;
	// credentials holds only those already distributed and readable; unresolved
	// identifies the matched-but-not-distributed ones for carry-forward.
	credentials, matched, unresolved, err := r.collectClusterCredentialsKeys(ctx, ledger)
	if err != nil {
		return nil, false, err
	}

	logger.V(1).Info("resolved credentials keys", "matched", matched, "resolved", len(credentials), "unresolved", len(unresolved))

	cmName := authKeysConfigMapName(ledger.Name)

	// Case 1: no Credentials match (matched == 0) — legitimate removal. Delete
	// the ConfigMap if it exists.
	if matched == 0 {
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

	// The pending fail-safe (case 2) only matters when a missing key would
	// crash-loop the cluster, i.e. when buildEnvVars would emit
	// AUTH_ED25519_KEYS. For a cluster with auth explicitly disabled that env var
	// is never set (see envvars.go), so preserving stale wiring buys no safety and
	// halting the StatefulSet pass would needlessly stall bootstrap/updates during
	// Credentials churn. Mirror buildEnvVars' authExplicitlyDisabled gate.
	authExplicitlyDisabled := ledger.Spec.Auth != nil && ledger.Spec.Auth.Enabled != nil && !*ledger.Spec.Auth.Enabled

	// Case 2 (EN-1487): matched >= 1 but ZERO resolved (full transient
	// non-distribution). Rewriting the ConfigMap from the empty subset (or
	// deleting it) would drop every key and roll the StatefulSet without any
	// Ed25519 key, crash-looping a healthy auth-enabled cluster. Fail safe: leave
	// the ConfigMap and StatefulSet wiring untouched; the caller sets
	// AuthKeysPending and requeues. Skipped when auth is explicitly disabled.
	if len(credentials) == 0 && !authExplicitlyDisabled {
		logger.Info("no matching credentials are distributed yet, preserving existing auth-key wiring",
			"matched", matched)

		return nil, true, nil
	}

	// Read the existing ConfigMap ONLY when there is a still-unresolved matched
	// credential to carry forward. The fully-resolved path (unresolved empty) must
	// stay self-healing: it rebuilds the ConfigMap unconditionally from the freshly
	// resolved set below, so it must never read — and therefore never fail on — a
	// malformed existing auth-keys.json. Reading there would wedge normal
	// reconciliation on a corrupt ConfigMap the rebuild is about to repair.
	//
	// existingEntries is also the carry-forward source for case 3 and lets case 3
	// detect a regression (a previously-present key vanishing). A MISSING ConfigMap
	// degrades to empty maps (nothing to carry forward); MALFORMED content likewise
	// degrades and self-heals; but a TRANSIENT API read error must requeue, never
	// degrade — see the switch below — so a still-authorized key is never dropped on
	// a blip.
	existingEntries := map[string]authKeyEntry{}
	existingPubKeys := map[string]string{}
	if len(unresolved) > 0 && !authExplicitlyDisabled {
		entries, pubKeys, err := r.readExistingAuthKeys(ctx, ledger.Namespace, cmName)
		switch {
		case errors.Is(err, errMalformedAuthKeys):
			// The ConfigMap read SUCCEEDED but its auth-keys.json is corrupt. This is
			// self-healable: carry-forward degrades to "no prior key" for every
			// unresolved credential (handled below) and the ConfigMap is rebuilt from
			// the resolved set; the corrupt prior key is genuinely unrecoverable. The
			// crash-loop guard still holds the line if this would leave an
			// auth-enabled cluster keyless.
			logger.Info("existing auth-keys ConfigMap is malformed, treating prior keys as absent (will self-heal)",
				"error", err.Error())
		case err != nil:
			// The API Get itself failed (transient/transport). Do NOT proceed with
			// empty prior keys: assuming "no prior key" here would drop a
			// still-authorized credential's key during partial resolution (the merged
			// set is non-empty because another credential resolved, so the crash-loop
			// guard would not catch it). Fail safe: propagate so the reconcile
			// requeues and retries once the API recovers.
			return nil, false, fmt.Errorf("reading existing auth-keys for carry-forward: %w", err)
		default:
			existingEntries = entries
			existingPubKeys = pubKeys
		}
	}

	// Case 3 (EN-1491): partial — some resolved, some transiently non-distributed.
	// Carry each still-unresolved credential's prior key forward from the existing
	// ConfigMap so its individual key is preserved, while the freshly-resolved keys
	// are propagated. A credential that was never distributed (no prior entry)
	// simply contributes no key. Auth-disabled clusters skip carry-forward: no key
	// is ever emitted, so there is nothing to preserve.
	carried := make([]credentialsKeyInfo, 0, len(unresolved))
	if !authExplicitlyDisabled {
		for _, u := range unresolved {
			fileName := pubKeyFileName(u.ConfigMapPrefix, u.CredentialsName)
			entry, hasEntry := existingEntries[fileName]
			pubKey, hasPubKey := existingPubKeys[fileName]
			if !hasEntry || !hasPubKey {
				// Never distributed (or its prior data is incomplete): nothing to
				// preserve. It contributes no key until distribution completes.
				logger.Info("no prior key to carry forward for undistributed credential",
					"credentials", u.CredentialsName)

				continue
			}
			carried = append(carried, credentialsKeyInfo{
				ConfigMapPrefix: u.ConfigMapPrefix,
				CredentialsName: u.CredentialsName,
				KeyID:           entry.KeyID,
				PublicKey:       pubKey,
				Scopes:          entry.Scopes,
				God:             entry.God,
			})
		}
	}

	// Merge freshly-resolved keys with the carried-forward pending keys.
	merged := make([]credentialsKeyInfo, 0, len(credentials)+len(carried))
	merged = append(merged, credentials...)
	merged = append(merged, carried...)

	// Sort by prefix+name for deterministic output.
	sort.Slice(merged, func(i, j int) bool {
		ki := merged[i].ConfigMapPrefix + "/" + merged[i].CredentialsName
		kj := merged[j].ConfigMapPrefix + "/" + merged[j].CredentialsName

		return ki < kj
	})

	// Regression guard: never ship an auth-enabled cluster with a lost or empty
	// key set. If the merge dropped a previously-present key for a credential that
	// still matches this Cluster (should not happen with carry-forward), or auth
	// is enabled and the merged set is empty, fall back to the case-2 pending
	// behavior rather than roll a key-losing template.
	//
	// The guard is scoped to currently-matched credentials only: an existing entry
	// for a credential that no longer matches the selector is a legitimate removal
	// (case 1 for that individual key), and its lingering entry must NOT freeze the
	// cluster forever.
	if !authExplicitlyDisabled {
		if len(merged) == 0 {
			logger.Info("merged auth-key set is empty for an auth-enabled cluster, preserving existing wiring",
				"matched", matched, "resolved", len(credentials))

			return nil, true, nil
		}
		mergedFiles := make(map[string]struct{}, len(merged))
		for _, m := range merged {
			mergedFiles[pubKeyFileName(m.ConfigMapPrefix, m.CredentialsName)] = struct{}{}
		}
		matchedFiles := make(map[string]struct{}, len(credentials)+len(unresolved))
		for _, c := range credentials {
			matchedFiles[pubKeyFileName(c.ConfigMapPrefix, c.CredentialsName)] = struct{}{}
		}
		for _, u := range unresolved {
			matchedFiles[pubKeyFileName(u.ConfigMapPrefix, u.CredentialsName)] = struct{}{}
		}
		for fileName := range existingEntries {
			if _, stillMatched := matchedFiles[fileName]; !stillMatched {
				continue // legitimate removal — no longer matched
			}
			if _, ok := mergedFiles[fileName]; !ok {
				logger.Info("merged auth-key set would drop a still-matched previously-present key, preserving existing wiring",
					"missing", fileName, "matched", matched, "resolved", len(credentials))

				return nil, true, nil
			}
		}
	}

	// No effective keys and not pending — only reachable for an auth-disabled
	// cluster (the auth-enabled empty case returned pending above). There is no
	// wiring to preserve, so delete the ConfigMap like any other no-effective-keys
	// removal rather than ship an empty one.
	if len(merged) == 0 {
		cm := &corev1.ConfigMap{}
		if err := r.Get(ctx, types.NamespacedName{Namespace: ledger.Namespace, Name: cmName}, cm); err != nil {
			if !apierrors.IsNotFound(err) {
				return nil, false, fmt.Errorf("checking auth-keys ConfigMap: %w", err)
			}
		} else {
			if err := r.Delete(ctx, cm); err != nil && !apierrors.IsNotFound(err) {
				return nil, false, fmt.Errorf("deleting auth-keys ConfigMap: %w", err)
			}
			logger.Info("deleted auth-keys ConfigMap (no effective auth keys)")
		}

		return nil, false, nil
	}

	// Build the auth-keys.json content from the merged set.
	authKeys := authKeysJSON{
		Keys: make([]authKeyEntry, 0, len(merged)),
	}
	pubKeyData := make(map[string]string, len(merged))

	for _, a := range merged {
		fileName := pubKeyFileName(a.ConfigMapPrefix, a.CredentialsName)
		authKeys.Keys = append(authKeys.Keys, authKeyEntry{
			KeyID:         a.KeyID,
			PublicKeyFile: "/auth-keys/" + fileName,
			Scopes:        a.Scopes,
			God:           a.God,
		})
		pubKeyData[fileName] = a.PublicKey
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

	return merged, false, nil
}

// readExistingAuthKeys reads the current auth-keys ConfigMap and indexes it by
// the per-credential pubkey filename (the stable identity — see pubKeyFileName).
// It returns the auth-keys.json entries keyed by their PublicKeyFile basename and
// the hex public-key blobs keyed by the same filename. A missing ConfigMap
// yields empty maps and no error: there is simply nothing to carry forward.
func (r *ClusterReconciler) readExistingAuthKeys(ctx context.Context, namespace, cmName string) (map[string]authKeyEntry, map[string]string, error) {
	cm := &corev1.ConfigMap{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: namespace, Name: cmName}, cm); err != nil {
		if apierrors.IsNotFound(err) {
			return map[string]authKeyEntry{}, map[string]string{}, nil
		}

		return nil, nil, fmt.Errorf("reading existing auth-keys ConfigMap: %w", err)
	}

	entries := map[string]authKeyEntry{}
	if rawJSON, ok := cm.Data["auth-keys.json"]; ok {
		var parsed authKeysJSON
		if err := json.Unmarshal([]byte(rawJSON), &parsed); err != nil {
			// Wrap with errMalformedAuthKeys so the caller can distinguish corrupt
			// content (self-heal) from a transient API read failure (requeue).
			return nil, nil, fmt.Errorf("%w: parsing existing auth-keys.json: %w", errMalformedAuthKeys, err)
		}
		for _, e := range parsed.Keys {
			entries[path.Base(e.PublicKeyFile)] = e
		}
	}

	pubKeys := map[string]string{}
	for k, v := range cm.Data {
		if k == "auth-keys.json" {
			continue
		}
		pubKeys[k] = v
	}

	return entries, pubKeys, nil
}

// collectClusterCredentialsKeys lists all Credentials and returns keys for
// those whose selector matches the given Cluster. It also returns matched: the
// number of Credentials whose selector matches this Cluster, counted before any
// distribution filtering, and unresolved: the identities of the
// matching-but-not-yet-distributed (or not-yet-readable) Credentials.
//
// matched >= len(keys)+len(unresolved) always; the three counts let the caller
// distinguish a legitimate removal (matched == 0) from a full transient
// non-distribution (len(keys) == 0, unresolved non-empty) and a partial one
// (both non-empty) — see reconcileAuthKeys. unresolved carries the per-credential
// identity needed to carry each pending key forward from the existing ConfigMap.
func (r *ClusterReconciler) collectClusterCredentialsKeys(ctx context.Context, ledger *ledgerv1alpha1.Cluster) ([]credentialsKeyInfo, int, []unresolvedCredential, error) {
	logger := log.FromContext(ctx)

	const configMapPrefix = "credentials"

	var list ledgerv1alpha1.CredentialsList
	if err := r.List(ctx, &list); err != nil {
		return nil, 0, nil, fmt.Errorf("listing Credentials: %w", err)
	}

	var keys []credentialsKeyInfo
	var unresolved []unresolvedCredential
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
			unresolved = append(unresolved, unresolvedCredential{ConfigMapPrefix: configMapPrefix, CredentialsName: cred.Name})

			continue
		}

		info, ok, err := r.readCredentialsKeyFromSecret(ctx, cred.Name, cred.Status.DistributedSecretRefs[0], cred.Spec.Scopes, cred.Spec.God, configMapPrefix)
		if err != nil {
			return nil, 0, nil, err
		}
		if !ok {
			// Distributed but the secret is not yet readable (missing/empty
			// fields): treat it as transiently unresolved so its prior key can be
			// carried forward rather than dropped.
			unresolved = append(unresolved, unresolvedCredential{ConfigMapPrefix: configMapPrefix, CredentialsName: cred.Name})

			continue
		}
		keys = append(keys, info)
	}

	return keys, matched, unresolved, nil
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
