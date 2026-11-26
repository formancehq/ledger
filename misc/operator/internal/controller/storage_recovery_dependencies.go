package controller

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"strconv"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
)

// reconcileStorageRecoveryDependencies refreshes resources used by the trusted
// saved workload without changing its PodTemplate or advancing TLS rollout state.
// The recovery caller must join this error after attempting restoration: an
// independent dependency failure must not strand an absent StatefulSet.
func (r *ClusterReconciler) reconcileStorageRecoveryDependencies(ctx context.Context, ledger *ledgerv1alpha1.Cluster, sts *appsv1.StatefulSet, claims []retainedClaimIdentity) error {
	if sts == nil || sts.Spec.Replicas == nil || *sts.Spec.Replicas < 1 {
		return errors.New("storage recovery dependencies require a saved StatefulSet with replicas")
	}
	latest := &ledgerv1alpha1.Cluster{}
	if err := r.storageAPIReader().Get(ctx, client.ObjectKeyFromObject(ledger), latest); err != nil {
		return fmt.Errorf("reading Cluster for storage recovery dependencies: %w", err)
	}
	if latest.UID != ledger.UID || !latest.DeletionTimestamp.IsZero() {
		return errors.New("cluster identity changed or is deleting during storage recovery dependencies")
	}
	// Live metadata selects Credentials and current additional labels apply to
	// dependency resources. Later desired configuration must not disable the
	// saved workload's auth, TLS, replica or storage dependencies.
	projection := &ledgerv1alpha1.Cluster{
		ObjectMeta: *latest.ObjectMeta.DeepCopy(),
		Spec: ledgerv1alpha1.ClusterSpec{
			AdditionalLabels: maps.Clone(latest.Spec.AdditionalLabels),
		},
	}
	var dependencyErrors []error
	usesAuthKeys := false
	for _, volume := range sts.Spec.Template.Spec.Volumes {
		if volume.ConfigMap != nil && volume.ConfigMap.Name == authKeysConfigMapName(ledger.Name) {
			usesAuthKeys = true
		}
	}
	if usesAuthKeys {
		auth, err := storageRecoveryAuthConfig(sts)
		if err == nil {
			projection.Spec.Auth = auth
			var pending authKeysPendingReason
			var keys []credentialsKeyInfo
			keys, pending, err = r.reconcileAuthKeysProjection(ctx, projection, true)
			if err == nil {
				if pending.pending() {
					meta.SetStatusCondition(&ledger.Status.Conditions, metav1.Condition{
						Type: "AuthKeysPending", Status: metav1.ConditionTrue,
						Reason: string(pending), Message: pending.conditionMessage(), ObservedGeneration: latest.Generation,
					})
				} else {
					meta.RemoveStatusCondition(&ledger.Status.Conditions, "AuthKeysPending")
				}
				message := "Storage recovery preserves the saved PodTemplate; auth-key changes affect running Pods only after a later restart or rollout"
				if len(keys) == 0 && !pending.pending() {
					message += "; explicit key removal retains an empty mounted ConfigMap, which the current key loader rejects on new starts"
				}
				meta.SetStatusCondition(&ledger.Status.Conditions, metav1.Condition{
					Type: "AuthKeysRolloutDeferred", Status: metav1.ConditionTrue,
					Reason: "StorageRecovery", Message: message, ObservedGeneration: latest.Generation,
				})
			}
		}
		if err != nil {
			dependencyErrors = append(dependencyErrors, fmt.Errorf("reconciling storage recovery auth keys: %w", err))
		}
	}

	// Equivalent to mayDelete=false: maintain only an actually referenced token,
	// preserve existing bytes, and never run the desired TLS transition machine.
	usesClusterSecret := false
	for _, container := range sts.Spec.Template.Spec.Containers {
		if container.Name != "ledger" {
			continue
		}
		for _, env := range container.Env {
			if env.Name == "CLUSTER_SECRET" && env.ValueFrom != nil && env.ValueFrom.SecretKeyRef != nil &&
				env.ValueFrom.SecretKeyRef.Name == clusterSecretName(ledger.Name) && env.ValueFrom.SecretKeyRef.Key == clusterSecretKey {
				usesClusterSecret = true
			}
		}
	}
	if usesClusterSecret {
		if !shouldInjectClusterSecret(currentTLSModeFromStatefulSet(sts)) {
			dependencyErrors = append(dependencyErrors, errors.New("saved workload references cluster-secret without TLS"))
		} else if err := r.reconcileClusterSecret(ctx, projection); err != nil {
			dependencyErrors = append(dependencyErrors, fmt.Errorf("reconciling storage recovery cluster-secret: %w", err))
		}
	}
	if r.Clientset != nil {
		volumeNames := make([]string, 0, len(sts.Spec.VolumeClaimTemplates))
		for _, claim := range sts.Spec.VolumeClaimTemplates {
			volumeNames = append(volumeNames, claim.Name)
		}
		retainedClaims := make(map[string]retainedClaimIdentity, len(claims))
		for _, claim := range claims {
			retainedClaims[claim.Name] = claim
		}
		// The explicit live protection toggle is independent of the workload. The
		// inventory and saved identities must remain frozen even if desired scale
		// or hostPath changes.
		if _, err := reconcileVolumeProtectionWithIdentities(ctx, r.Clientset, sts.Namespace, sts.Name, *sts.Spec.Replicas,
			volumeNames, latest.Spec.Persistence.DeletionProtectionEnabled(), retainedClaims); err != nil {
			dependencyErrors = append(dependencyErrors, fmt.Errorf("reconciling storage recovery volume protection: %w", err))
		}
	}

	return errors.Join(dependencyErrors...)
}

// storageRecoveryAuthConfig interprets the saved ledger container's auth inputs
// for the existing projection fail-safe. In particular, a key-file input enables
// Ed25519 auth even when AUTH_ENABLED was not explicitly emitted by the Operator.
func storageRecoveryAuthConfig(sts *appsv1.StatefulSet) (*ledgerv1alpha1.AuthorizationConfig, error) {
	auth := &ledgerv1alpha1.AuthorizationConfig{Enabled: new(false)}
	hasKeyFile := false
	for _, container := range sts.Spec.Template.Spec.Containers {
		if container.Name != "ledger" {
			continue
		}
		for _, env := range container.Env {
			switch env.Name {
			case "AUTH_ENABLED", "AUTH_ED25519_KEYS", "AUTH_ISSUER", "AUTH_ISSUERS":
				if env.ValueFrom != nil {
					return nil, fmt.Errorf("cannot resolve saved %s from valueFrom for auth projection", env.Name)
				}
			}
			switch env.Name {
			case "AUTH_ENABLED":
				enabled, err := strconv.ParseBool(env.Value)
				if err != nil {
					return nil, fmt.Errorf("parsing saved AUTH_ENABLED: %w", err)
				}
				auth.Enabled = &enabled
			case "AUTH_ED25519_KEYS":
				hasKeyFile = env.Value != ""
			case "AUTH_ISSUER":
				auth.Issuer = env.Value
			case "AUTH_ISSUERS":
				if env.Value != "" {
					auth.Issuers = strings.Split(env.Value, ",")
				}
			}
		}
	}
	if hasKeyFile {
		auth.Enabled = new(true)
	}

	return auth, nil
}
