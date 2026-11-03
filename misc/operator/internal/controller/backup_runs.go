package controller

import (
	"context"

	"sigs.k8s.io/controller-runtime/pkg/client"

	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
)

const backupRunParentField = "spec.backupRef"

// indexBackupRunParent enables the same parent selector on the informer cache
// that the CRD's selectableFields enables for direct API-server reads.
func indexBackupRunParent(ctx context.Context, indexer client.FieldIndexer) error {
	return indexer.IndexField(ctx, &ledgerv1alpha1.BackupRun{}, backupRunParentField, backupRunParentIndex)
}

func backupRunParentIndex(obj client.Object) []string {
	return []string{obj.(*ledgerv1alpha1.BackupRun).Spec.BackupRef}
}

// listBackupRuns uses the admitted parent reference, including manual runs
// without labels or owner references. The reader preserves each caller's
// consistency contract; the parent must bypass the informer cache.
func listBackupRuns(ctx context.Context, reader client.Reader, backup client.ObjectKey) ([]ledgerv1alpha1.BackupRun, error) {
	var list ledgerv1alpha1.BackupRunList
	if err := reader.List(ctx, &list, client.InNamespace(backup.Namespace), client.MatchingFields{backupRunParentField: backup.Name}); err != nil {
		return nil, err
	}

	return list.Items, nil
}
