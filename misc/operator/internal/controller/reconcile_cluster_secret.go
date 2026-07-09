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

// generateRandomToken returns a hex-encoded random token of the given byte length.
func generateRandomToken(n int) (string, error) {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}

	return hex.EncodeToString(b), nil
}
