package controller

import (
	"context"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"

	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
)

func credentialsOwnerKey(ledger *ledgerv1alpha1.Cluster, annotation string) string {
	return annotation + "=" + ledger.Namespace + "/" + ledger.Name
}

func (r *ClusterReconciler) managedCredentials(ctx context.Context, ledger *ledgerv1alpha1.Cluster, name, ownerAnnotation string) (*ledgerv1alpha1.Credentials, bool, error) {
	credentials := &ledgerv1alpha1.Credentials{}
	if err := r.Get(ctx, types.NamespacedName{Name: name}, credentials); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, false, nil
		}
		return nil, false, err
	}

	if credentials.Labels[labelManagedBy] != "ledger-operator" ||
		credentials.Annotations[ownerAnnotation] != ledger.Namespace+"/"+ledger.Name {
		return credentials, false, fmt.Errorf("Credentials %s is not owned by Cluster %s/%s (%s)", name, ledger.Namespace, ledger.Name, credentialsOwnerKey(ledger, ownerAnnotation))
	}

	return credentials, true, nil
}

func (r *ClusterReconciler) deleteManagedCredentials(ctx context.Context, ledger *ledgerv1alpha1.Cluster, name, ownerAnnotation string) error {
	credentials, managed, err := r.managedCredentials(ctx, ledger, name, ownerAnnotation)
	if err != nil {
		return err
	}
	if !managed {
		return nil
	}

	if err := r.Delete(ctx, credentials); err != nil && !apierrors.IsNotFound(err) {
		return err
	}

	return nil
}
