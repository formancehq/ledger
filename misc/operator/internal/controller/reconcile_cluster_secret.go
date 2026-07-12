package controller

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	ledgerv1alpha1 "github.com/formance/ledger/operator/api/v1alpha1"
)

const clusterSecretKey = "cluster-secret"

// reconcileClusterSecret ensures a Secret exists with a random cluster
// secret for inter-node authentication. The caller is expected to invoke
// this only when TLS is at least partially active (the secret is a static
// bearer token and must never be sent in plaintext); see
// shouldInjectClusterSecret.
//
// During a TLS toggle the operator orders things so that the secret
// appears at the same time the StatefulSet moves into the "optional" mode,
// and is symmetrically removed (via deleteClusterSecret) when TLS is
// turned off again.
func (r *ClusterReconciler) reconcileClusterSecret(ctx context.Context, ledger *ledgerv1alpha1.Cluster) error {
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      clusterSecretName(ledger.Name),
			Namespace: ledger.Namespace,
		},
	}

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, secret, func() error {
		secret.Labels = commonLabels(ledger)

		// Only generate the secret value on creation; preserve it on updates.
		if secret.Data == nil || len(secret.Data[clusterSecretKey]) == 0 {
			token, err := generateRandomToken(32)
			if err != nil {
				return fmt.Errorf("generating cluster secret: %w", err)
			}

			secret.Data = map[string][]byte{
				clusterSecretKey: []byte(token),
			}
		}

		return controllerutil.SetControllerReference(ledger, secret, r.Scheme)
	})

	return err
}

// deleteClusterSecret removes the cluster-secret Secret if it exists. Used
// when TLS is turned off (mode=disabled): the secret is no longer needed
// and must not be left around for someone to harvest it from a plaintext
// cluster.
func (r *ClusterReconciler) deleteClusterSecret(ctx context.Context, ledger *ledgerv1alpha1.Cluster) error {
	secret := &corev1.Secret{}

	err := r.Get(ctx, types.NamespacedName{Name: clusterSecretName(ledger.Name), Namespace: ledger.Namespace}, secret)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}

		return err
	}

	if err := r.Delete(ctx, secret, &client.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
		return err
	}

	return nil
}

// reconcileClusterSecretForTLSState creates or deletes the cluster-secret to
// match the TLS mode the running StatefulSet is (or is converging) toward. It
// reads the EXISTING StatefulSet for TLS state and never mutates the pod template
// or triggers a rollout, so it is safe to run even while the auth-keys fail-safe
// (EN-1487) is holding the StatefulSet pass: a restarted pod referencing
// CLUSTER_SECRET via SecretKeyRef must still find the Secret present, and a
// TLS-off toggle must still remove it, regardless of whether Credentials are
// distributed yet. It is therefore invoked BEFORE the AuthKeysPending gate as well
// as on the normal path, so a long Credentials-non-distribution window can never
// strand the cluster-secret (which the deferred reconcileStatefulSet would
// otherwise be the only thing to reconcile).
func (r *ClusterReconciler) reconcileClusterSecretForTLSState(ctx context.Context, ledger *ledgerv1alpha1.Cluster) error {
	existingSTS, err := r.fetchExistingStatefulSet(ctx, ledger)
	if err != nil {
		return fmt.Errorf("fetching StatefulSet for TLS state: %w", err)
	}
	targetTLS := computeTargetTLSMode(
		desiredTLSMode(ledger),
		currentTLSModeFromStatefulSet(existingSTS),
		rolloutConverged(existingSTS),
	)

	// The secret is a static bearer token; it must never travel in plaintext, so it
	// exists only while TLS is at least partially active. During a TLS toggle the
	// operator orders things so the secret appears as the StatefulSet moves to
	// "optional" and is removed symmetrically when TLS is turned off.
	if shouldInjectClusterSecret(targetTLS) {
		if err := r.reconcileClusterSecret(ctx, ledger); err != nil {
			return fmt.Errorf("reconciling ClusterSecret: %w", err)
		}

		return nil
	}

	if err := r.deleteClusterSecret(ctx, ledger); err != nil {
		return fmt.Errorf("deleting ClusterSecret: %w", err)
	}

	return nil
}

// generateRandomToken returns a hex-encoded random token of the given byte length.
func generateRandomToken(n int) (string, error) {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}

	return hex.EncodeToString(b), nil
}
