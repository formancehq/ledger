package controller

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	ledgerv1alpha1 "github.com/formance/ledger/operator/api/v1alpha1"
)

const clusterSecretKey = "cluster-secret"

// clusterSecretName returns the Secret name for the cluster inter-node auth secret.
func clusterSecretName(ledger *ledgerv1alpha1.LedgerService) string {
	return ledger.Name + "-cluster-secret"
}

// reconcileClusterSecret ensures a Secret always exists with a random cluster secret
// for inter-node authentication. The secret is created unconditionally so that all
// pods always send the bearer token on outgoing calls. This prevents a rolling-update
// deadlock when agents are added for the first time: without this, only updated pods
// would send the token, and those same pods (with auth enabled) would reject calls
// from not-yet-updated pods that don't send it.
func (r *LedgerServiceReconciler) reconcileClusterSecret(ctx context.Context, ledger *ledgerv1alpha1.LedgerService) error {
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      clusterSecretName(ledger),
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

// generateRandomToken returns a hex-encoded random token of the given byte length.
func generateRandomToken(n int) (string, error) {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}

	return hex.EncodeToString(b), nil
}
