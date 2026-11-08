package controller

import (
	"context"
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
)

var operatorCredentialScopes = []string{
	"ledger:ReadOps",
	"ledger:WriteOps",
	"ledger:ReadCluster",
	"ledger:WriteCluster",
}

func ownedOperatorCredentials(credentials *ledgerv1alpha1.Credentials, ledger *ledgerv1alpha1.Cluster) bool {
	return credentials.Labels[labelManagedBy] == "ledger-operator" &&
		credentials.Annotations[annotationOperatorCluster] == ledger.Namespace+"/"+ledger.Name
}

func (r *ClusterReconciler) deleteOperatorCredentials(ctx context.Context, ledger *ledgerv1alpha1.Cluster) error {
	credentials := &ledgerv1alpha1.Credentials{}
	if err := r.Get(ctx, types.NamespacedName{Name: operatorCredentialsName(ledger)}, credentials); err != nil {
		return client.IgnoreNotFound(err)
	}
	if !ownedOperatorCredentials(credentials, ledger) {
		return nil
	}
	if err := r.Delete(ctx, credentials, client.Preconditions{UID: &credentials.UID}); err != nil {
		return client.IgnoreNotFound(err)
	}
	return nil
}

// reconcileOperatorCredentials creates the scoped Credentials resource used by
// operator maintenance commands. It is deliberately not exposed in the Cluster
// API: the operator owns the identity, scopes, and lifecycle.
func (r *ClusterReconciler) reconcileOperatorCredentials(ctx context.Context, ledger *ledgerv1alpha1.Cluster) (bool, error) {
	credentialsName := operatorCredentialsName(ledger)
	authEnabled := ledger.Spec.Auth != nil && ledger.Spec.Auth.Enabled != nil && *ledger.Spec.Auth.Enabled
	if !authEnabled {
		if err := r.deleteOperatorCredentials(ctx, ledger); err != nil {
			return false, err
		}

		return true, nil
	}

	selectorValue := operatorCredentialsSelectorValue(ledger)
	if ledger.Labels == nil {
		ledger.Labels = make(map[string]string)
	}
	if ledger.Labels[labelOperatorCredential] != selectorValue {
		ledger.Labels[labelOperatorCredential] = selectorValue
		if err := r.Update(ctx, ledger); err != nil {
			return false, fmt.Errorf("persisting operator Credentials selector label: %w", err)
		}
	}

	credentials := &ledgerv1alpha1.Credentials{ObjectMeta: metav1.ObjectMeta{Name: credentialsName}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, credentials, func() error {
		if credentials.ResourceVersion != "" && !ownedOperatorCredentials(credentials, ledger) {
			return fmt.Errorf("Credentials %q already exists and is not owned by Cluster %s/%s", credentials.Name, ledger.Namespace, ledger.Name)
		}
		if credentials.Labels == nil {
			credentials.Labels = make(map[string]string)
		}
		credentials.Labels[labelManagedBy] = "ledger-operator"
		credentials.Labels[labelOperatorCredential] = selectorValue
		if credentials.Annotations == nil {
			credentials.Annotations = make(map[string]string)
		}
		credentials.Annotations[annotationOperatorCluster] = ledger.Namespace + "/" + ledger.Name
		credentials.Spec.Scopes = append([]string(nil), operatorCredentialScopes...)
		credentials.Spec.Superuser = false
		credentials.Spec.Selector = metav1.LabelSelector{MatchLabels: map[string]string{
			labelOperatorCredential: selectorValue,
		}}

		return nil
	})
	if err != nil {
		return false, fmt.Errorf("ensuring %s: %w", credentialsName, err)
	}

	for _, ref := range credentials.Status.DistributedSecretRefs {
		if ref.Namespace == ledger.Namespace && credentials.Status.Phase == "Ready" {
			return true, nil
		}
	}

	return false, nil
}
