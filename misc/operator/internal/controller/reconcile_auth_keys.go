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

// authKeysPendingReason is the structured reason reconcileAuthKeys returns when it
// holds the StatefulSet pass (pending). It lets the caller set an accurate status
// condition / event for each distinct fail-safe path instead of a single
// one-size-fits-all "none distributed" message. The empty value means NOT pending.
type authKeysPendingReason string

const (
	// authKeysNotPending signals the StatefulSet pass may proceed.
	authKeysNotPending authKeysPendingReason = ""
	// authKeysPendingNoneDistributed: an Ed25519-dependent cluster matches one or
	// more Credentials but NONE is distributed and nothing can be carried forward,
	// so the merged key set is empty. Rolling would boot the cluster keyless.
	authKeysPendingNoneDistributed authKeysPendingReason = "CredentialsNotDistributed"
	// authKeysPendingIncompleteKeySet: some key material exists (resolved and/or
	// carried forward) but a still-matched, previously-present key could not be
	// carried (e.g. its stored pubkey blob is missing), so rolling would drop a
	// still-authorized key. Distinct from NoneDistributed: here the set is
	// non-empty but incomplete.
	authKeysPendingIncompleteKeySet authKeysPendingReason = "AuthKeysIncomplete"
)

// pending reports whether the reason holds the StatefulSet pass.
func (r authKeysPendingReason) pending() bool { return r != authKeysNotPending }

// conditionMessage returns the human-readable status-condition / event message for
// a pending reason.
func (r authKeysPendingReason) conditionMessage() string {
	switch r {
	case authKeysPendingNoneDistributed:
		return "matching Credentials exist but none is distributed yet; preserving existing auth-key wiring and waiting for distribution"
	case authKeysPendingIncompleteKeySet:
		return "auth keys cannot be safely reconciled yet: a previously-configured key for a still-matching Credentials cannot be carried forward; preserving existing auth-key wiring and waiting for distribution"
	default:
		return ""
	}
}

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
// transiently not distributed yet. It carries the stable identity needed to look
// the credential's prior key up in the existing ConfigMap for carry-forward
// (EN-1491) — the ConfigMap prefix + credential name, which together form the
// pubkey filename produced by pubKeyFileName — plus the LIVE authorization
// metadata (Scopes, God) read from the current Credentials spec. Only the key
// material (KeyID/PublicKey) is unavailable (that is precisely what is missing)
// and must be carried forward from the prior ConfigMap; authorization metadata
// must always reflect the current spec, never the stale stored entry.
type unresolvedCredential struct {
	ConfigMapPrefix string
	CredentialsName string
	// Scopes and God come from the current Credentials spec so a narrowed spec
	// (scopes removed, god cleared) takes effect even while the secret is
	// transiently unresolved and the key material is carried forward.
	Scopes []string
	God    bool
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
//     e.g. during operator/Credentials churn). For a cluster that DEPENDS on
//     Ed25519 keys (auth EFFECTIVELY enabled and NO OIDC issuer configured — see
//     requiresEd25519Keys), deleting the ConfigMap and stripping the StatefulSet
//     here would produce AUTH_ENABLED=true without any key and crash-loop an
//     otherwise healthy cluster. This is a transient state, so we keep the
//     StatefulSet fail-safe: return a non-empty pending reason so the caller sets
//     the AuthKeysPending condition, holds the StatefulSet pass, and requeues until
//     distribution completes. A cluster that does NOT depend on Ed25519 keys —
//     auth effectively disabled (nil auth / nil-or-false enabled), or backed by an
//     OIDC issuer — boots and authenticates fine with no Ed25519 key set at all, so
//     it is NEVER frozen even when every matching Credentials is unresolved;
//     freezing it would needlessly block image/replica/TLS updates (EN-1487, P1).
//     In every non-disabled case we still REBUILD the ConfigMap via the same
//     carry-forward path as the partial case — each entry keeps its last-known key
//     material while its authorization metadata (scopes / god) is refreshed from
//     the live Credentials spec, so a narrowed or god-cleared Credentials does not
//     preserve stale privileges indefinitely while its Secret stays undistributed.
//     The freeze is further narrowed to an INCOMPLETE carried set: if every
//     previously-distributed still-matched credential's key is carried forward
//     (a COMPLETE set), rolling is crash-loop-safe (the mounted key set is
//     non-empty and complete) and is in fact required so the refreshed
//     authorization reaches the running pods (which load AUTH_ED25519_KEYS once at
//     boot) — so pending=false there. Only an empty or key-dropping merge (no
//     prior key material to carry) keeps the wiring untouched and pending=true, so
//     an Ed25519-dependent cluster is never rolled keyless.
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
// The returned pending reason is non-empty only when the cluster depends on Ed25519
// keys AND the merged key set is empty (authKeysPendingNoneDistributed) or would
// drop a still-matched previously-present key (authKeysPendingIncompleteKeySet) —
// the ConfigMap may still have been refreshed. A complete carried set, an
// issuer-backed cluster, and any partial resolution all return authKeysNotPending.
func (r *ClusterReconciler) reconcileAuthKeys(ctx context.Context, ledger *ledgerv1alpha1.Cluster) ([]credentialsKeyInfo, authKeysPendingReason, error) {
	logger := log.FromContext(ctx)

	// Collect keys from cluster-scoped Credentials. matched counts Credentials
	// whose selector matches this Cluster, regardless of distribution state;
	// credentials holds only those already distributed and readable; unresolved
	// identifies the matched-but-not-distributed ones for carry-forward.
	credentials, matched, unresolved, err := r.collectClusterCredentialsKeys(ctx, ledger)
	if err != nil {
		return nil, authKeysNotPending, err
	}

	logger.V(1).Info("resolved credentials keys", "matched", matched, "resolved", len(credentials), "unresolved", len(unresolved))

	cmName := authKeysConfigMapName(ledger.Name)

	// Case 1: no Credentials match (matched == 0) — legitimate removal. Delete
	// the ConfigMap if it exists.
	if matched == 0 {
		cm := &corev1.ConfigMap{}
		if err := r.Get(ctx, types.NamespacedName{Namespace: ledger.Namespace, Name: cmName}, cm); err != nil {
			if !apierrors.IsNotFound(err) {
				return nil, authKeysNotPending, fmt.Errorf("checking auth-keys ConfigMap: %w", err)
			}
		} else {
			if err := r.Delete(ctx, cm); err != nil && !apierrors.IsNotFound(err) {
				return nil, authKeysNotPending, fmt.Errorf("deleting auth-keys ConfigMap: %w", err)
			}
			logger.Info("deleted auth-keys ConfigMap (no matching credentials)")
		}

		return nil, authKeysNotPending, nil
	}

	// authExplicitlyDisabled mirrors buildEnvVars' gate: for an auth-disabled
	// cluster AUTH_ED25519_KEYS is never emitted (see envvars.go), so the ConfigMap
	// is never mounted. It gates whether we ever write/carry-forward key material at
	// all — the ConfigMap build and carry-forward read below are skipped in that
	// case, exactly as envvars.go skips the env var.
	authExplicitlyDisabled := ledger.Spec.Auth != nil && ledger.Spec.Auth.Enabled != nil && !*ledger.Spec.Auth.Enabled

	// requiresEd25519Keys reports whether an empty/missing Ed25519 key set would
	// actually break this cluster — and therefore whether the pending fail-safe
	// must hold the StatefulSet pass. It is gated on the EFFECTIVE enabled value,
	// mirroring buildEnvVars + the server default: auth is on only when
	// spec.auth.enabled is explicitly true. A nil spec.auth or a nil
	// spec.auth.enabled emits no AUTH_ENABLED (appendIfBool skips nil), and the
	// server defaults --auth-enabled to false (cmd/server/server.go), so such a
	// cluster runs with auth disabled and needs no Ed25519 keys — it must NOT be
	// frozen (EN-1487, flemzord follow-up). This is a stricter condition than
	// !authExplicitlyDisabled (which is also true for the nil cases); the freeze
	// must use the effective value while the ConfigMap-write gates below keep
	// mirroring envvars.go's own !authExplicitlyDisabled emission condition.
	//
	// The server's validateAuthConfig further accepts AUTH_ENABLED=true backed by
	// an OIDC issuer ALONE (no Ed25519 keys); such a cluster authenticates on the
	// issuer and boots with no AUTH_ED25519_KEYS. So freezing an issuer-backed
	// cluster whose matching Credentials are all unresolved buys no crash-loop
	// safety and needlessly blocks image/replica/TLS updates (EN-1487, P1). The
	// fail-safe is therefore scoped to clusters that depend on Ed25519 keys: auth
	// EFFECTIVELY enabled AND no OIDC issuer configured. Issuer-backed and
	// effectively-disabled clusters reconcile normally (not pending) — their
	// existing key wiring, if any, is still carried forward and preserved by the
	// paths below; only the StatefulSet freeze is lifted.
	authEffectivelyEnabled := ledger.Spec.Auth != nil && ledger.Spec.Auth.Enabled != nil && *ledger.Spec.Auth.Enabled
	hasIssuer := ledger.Spec.Auth != nil && (ledger.Spec.Auth.Issuer != "" || len(ledger.Spec.Auth.Issuers) > 0)
	requiresEd25519Keys := authEffectivelyEnabled && !hasIssuer

	// Case 2 (EN-1487): matched >= 1 but ZERO resolved (full transient
	// non-distribution). For an Ed25519-dependent cluster we must never roll the
	// StatefulSet with an empty key set (crash-loop), so a pending reason is
	// returned below (gated on requiresEd25519Keys) and the caller holds the
	// StatefulSet pass; an effectively-disabled or issuer-backed cluster reconciles
	// normally. Either way, rather than early-return with the ConfigMap untouched —
	// which would preserve stale authorization metadata indefinitely while the
	// Secret is undistributed — we fall through to the shared carry-forward path
	// (case 3): each entry keeps its last-known key material and gets its scopes/god
	// refreshed from the live spec. The regression guard further down still returns
	// a pending reason with the wiring untouched when there is nothing to carry
	// forward (no prior key material), so an Ed25519-dependent cluster is never
	// rolled keyless.

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
			return nil, authKeysNotPending, fmt.Errorf("reading existing auth-keys for carry-forward: %w", err)
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
				// Key material (KeyID/PublicKey) is carried forward from the prior
				// ConfigMap — it is unrecoverable while the secret is unresolved. But
				// authorization metadata comes from the LIVE spec so a narrowed
				// Credentials (scopes removed / god cleared) takes effect immediately
				// rather than preserving stale privileges indefinitely.
				KeyID:     entry.KeyID,
				PublicKey: pubKey,
				Scopes:    u.Scopes,
				God:       u.God,
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

	// Regression guard: never ship an Ed25519-dependent cluster with a lost or
	// empty key set. If the merge dropped a previously-present key for a credential
	// that still matches this Cluster (should not happen with carry-forward), or
	// the merged set is empty, fall back to the case-2 pending behavior rather than
	// roll a key-losing template. It is scoped to requiresEd25519Keys because that
	// is the only configuration a missing key crash-loops; an issuer-backed cluster
	// tolerates an empty/reduced key set (auth still succeeds on the issuer), so it
	// must NOT be frozen here (EN-1487, P1).
	//
	// The guard is scoped to currently-matched credentials only: an existing entry
	// for a credential that no longer matches the selector is a legitimate removal
	// (case 1 for that individual key), and its lingering entry must NOT freeze the
	// cluster forever.
	//
	// This block decides, for an Ed25519-dependent cluster, whether the carried set
	// is COMPLETE — every previously-distributed still-matched credential is
	// represented in merged. An incomplete set returns a pending reason here — an
	// empty merge is authKeysPendingNoneDistributed, a merge that would drop a
	// still-matched prior key is authKeysPendingIncompleteKeySet; a complete set
	// falls through and rolls.
	// A complete carried set is safe to roll even while every live Secret is
	// unresolved: the ConfigMap references only key material that is present, so
	// envvars resolves a non-empty key file and no keyless crash-loop can occur (the
	// server verifies tokens with public keys alone). Freezing a complete set would
	// strand a legitimate authorization change — narrowed scopes / cleared god on
	// the carried entries — because pods load AUTH_ED25519_KEYS once at boot and
	// would never restart (EN-1487, QHX5a rollout half).
	if requiresEd25519Keys {
		if len(merged) == 0 {
			// Nothing to carry forward and nothing resolved: rolling would boot the
			// cluster keyless (crash-loop). Preserve the existing wiring and stay
			// pending. This is the genuine transient-undistribution hazard.
			logger.Info("merged auth-key set is empty for an Ed25519-dependent cluster, preserving existing wiring",
				"matched", matched, "resolved", len(credentials))

			return nil, authKeysPendingNoneDistributed, nil
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
		// existingEntries is the set of previously-DISTRIBUTED credentials (an entry
		// is written only once a credential resolves). Treating it as the expected
		// carry-forward set, the carried set is complete iff every still-matched
		// prior entry made it into merged. A prior entry that is still matched but
		// absent from merged (e.g. its pubkey blob was missing so carry-forward
		// skipped it) means we would drop a still-authorized key: that is incomplete,
		// so freeze rather than roll a key-losing template.
		for fileName := range existingEntries {
			if _, stillMatched := matchedFiles[fileName]; !stillMatched {
				continue // legitimate removal — no longer matched
			}
			if _, ok := mergedFiles[fileName]; !ok {
				logger.Info("merged auth-key set would drop a still-matched previously-present key, preserving existing wiring",
					"missing", fileName, "matched", matched, "resolved", len(credentials))

				return nil, authKeysPendingIncompleteKeySet, nil
			}
		}
	}

	// No effective keys and not pending — reachable for any cluster that does NOT
	// depend on Ed25519 keys (auth-disabled, or issuer-backed); the
	// Ed25519-dependent empty case returned pending above. There is no wiring to
	// preserve (the server needs no key set here), so delete the ConfigMap like any
	// other no-effective-keys removal rather than ship an empty one.
	if len(merged) == 0 {
		cm := &corev1.ConfigMap{}
		if err := r.Get(ctx, types.NamespacedName{Namespace: ledger.Namespace, Name: cmName}, cm); err != nil {
			if !apierrors.IsNotFound(err) {
				return nil, authKeysNotPending, fmt.Errorf("checking auth-keys ConfigMap: %w", err)
			}
		} else {
			if err := r.Delete(ctx, cm); err != nil && !apierrors.IsNotFound(err) {
				return nil, authKeysNotPending, fmt.Errorf("deleting auth-keys ConfigMap: %w", err)
			}
			logger.Info("deleted auth-keys ConfigMap (no effective auth keys)")
		}

		return nil, authKeysNotPending, nil
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
		return nil, authKeysNotPending, fmt.Errorf("marshaling auth-keys.json: %w", err)
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
		return nil, authKeysNotPending, fmt.Errorf("reconciling auth-keys ConfigMap: %w", err)
	}

	// Reaching this point means the merged set is non-empty and, for an
	// Ed25519-dependent cluster, COMPLETE — the regression guard above already
	// returned pending=true for the empty and incomplete cases. So every remaining
	// path can safely roll the StatefulSet:
	//
	//   - Partial resolution (>=1 resolved) — always rolled, as before.
	//   - All-unresolved but with a complete carried key set (EN-1487, QHX5a
	//     rollout half): the ConfigMap was rebuilt with the live spec's
	//     authorization metadata over the carried key material, and rolling is
	//     crash-loop-safe because the referenced key set is non-empty and complete.
	//     Rolling is in fact REQUIRED here: pods load AUTH_ED25519_KEYS once at
	//     boot, so a narrowed-scopes / cleared-god change only takes effect on
	//     restart. Freezing would strand that authorization change indefinitely.
	//   - Issuer-backed / auth-disabled clusters (requiresEd25519Keys == false) —
	//     never crash-loop on a missing key set, so they reconcile normally too.
	//
	// pending is therefore not raised: the only cases that must hold the
	// StatefulSet pass (empty or incomplete Ed25519 key set) have already returned
	// above.
	return merged, authKeysNotPending, nil
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
			unresolved = append(unresolved, unresolvedCredential{ConfigMapPrefix: configMapPrefix, CredentialsName: cred.Name, Scopes: cred.Spec.Scopes, God: cred.Spec.God})

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
			unresolved = append(unresolved, unresolvedCredential{ConfigMapPrefix: configMapPrefix, CredentialsName: cred.Name, Scopes: cred.Spec.Scopes, God: cred.Spec.God})

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
