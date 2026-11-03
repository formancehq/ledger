package controller

import (
	"context"

	"sigs.k8s.io/controller-runtime/pkg/client"

	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
)

// listBackupRuns uses the admitted parent reference, including manual runs
// without labels or owner references. The reader preserves each caller's
// consistency contract; the parent must bypass the informer cache.
func listBackupRuns(ctx context.Context, reader client.Reader, backup client.ObjectKey) ([]ledgerv1alpha1.BackupRun, error) {
	var list ledgerv1alpha1.BackupRunList
	if err := reader.List(ctx, &list, client.InNamespace(backup.Namespace)); err != nil {
		return nil, err
	}

	runs := list.Items[:0]
	for i := range list.Items {
		if list.Items[i].Spec.BackupRef == backup.Name {
			runs = append(runs, list.Items[i])
		}
	}

	return runs, nil
}
