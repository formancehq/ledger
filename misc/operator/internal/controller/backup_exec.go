package controller

import (
	"context"
	"encoding/json"
	"fmt"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	ledgerv1alpha1 "github.com/formance/ledger/operator/api/v1alpha1"
)

// fullBackupResult holds the parsed JSON output from `ledgerctl store backup`.
type fullBackupResult struct {
	FilesUploaded     uint32 `json:"filesUploaded"`
	FilesDeleted      uint32 `json:"filesDeleted"`
	TotalFiles        uint32 `json:"totalFiles"`
	DurationMs        int64  `json:"durationMs"`
	LastLogSequence   uint64 `json:"lastLogSequence"`
	LastAuditSequence uint64 `json:"lastAuditSequence"`
	LastAppliedIndex  uint64 `json:"lastAppliedIndex"`
}

// incrementalBackupResult holds the parsed JSON output from `ledgerctl store incremental-backup`.
type incrementalBackupResult struct {
	LogEntriesExported   uint64 `json:"logEntriesExported"`
	AuditEntriesExported uint64 `json:"auditEntriesExported"`
	SegmentsUploaded     uint32 `json:"segmentsUploaded"`
	DurationMs           int64  `json:"durationMs"`
	LastLogSequence      uint64 `json:"lastLogSequence"`
	LastAuditSequence    uint64 `json:"lastAuditSequence"`
}

// backupFlags builds the common ledgerctl flags for backup commands.
func backupFlags(dest *ledgerv1alpha1.BackupDestination) []string {
	args := []string{"--driver", dest.Driver}
	if dest.BucketID != "" {
		args = append(args, "--bucket-id", dest.BucketID)
	}
	if dest.S3 != nil {
		if dest.S3.Bucket != "" {
			args = append(args, "--s3-bucket", dest.S3.Bucket)
		}
		if dest.S3.Region != "" {
			args = append(args, "--s3-region", dest.S3.Region)
		}
		if dest.S3.Endpoint != "" {
			args = append(args, "--s3-endpoint", dest.S3.Endpoint)
		}
	}
	if dest.S3AccessKeyID != "" {
		args = append(args, "--s3-access-key-id", dest.S3AccessKeyID)
	}
	if dest.S3SecretAccessKey != "" {
		args = append(args, "--s3-secret-access-key", dest.S3SecretAccessKey)
	}

	return args
}

// execFullBackup runs `ledgerctl store backup` on pod-0 of the LedgerService
// and parses the JSON output. The leader proxies the call internally so any
// pod can be targeted.
func execFullBackup(
	ctx context.Context,
	cfg *rest.Config,
	clientset kubernetes.Interface,
	backup *ledgerv1alpha1.LedgerBackup,
	ledgerService *ledgerv1alpha1.LedgerService,
	grpcPort int32,
) (*fullBackupResult, error) {
	pod := ledgerService.Name + "-0"
	container := "ledger"

	args := []string{"store", "backup"}
	args = append(args, backupFlags(&backup.Spec.Destination)...)
	args = append(args, "--json")

	result, err := podExec(ctx, cfg, clientset, ledgerService.Namespace, pod, container,
		ledgerctlCommand(grpcPort, args...))
	if err != nil {
		return nil, fmt.Errorf("ledgerctl store backup: %w", err)
	}

	var resp fullBackupResult
	if err := json.Unmarshal([]byte(result.Stdout), &resp); err != nil {
		return nil, fmt.Errorf("parsing backup output: %w (stdout: %s)", err, result.Stdout)
	}

	return &resp, nil
}

// execIncrementalBackup runs `ledgerctl store incremental-backup` on pod-0 of
// the LedgerService and parses the JSON output.
func execIncrementalBackup(
	ctx context.Context,
	cfg *rest.Config,
	clientset kubernetes.Interface,
	backup *ledgerv1alpha1.LedgerBackup,
	ledgerService *ledgerv1alpha1.LedgerService,
	grpcPort int32,
) (*incrementalBackupResult, error) {
	pod := ledgerService.Name + "-0"
	container := "ledger"

	args := []string{"store", "incremental-backup"}
	args = append(args, backupFlags(&backup.Spec.Destination)...)
	args = append(args, "--json")

	result, err := podExec(ctx, cfg, clientset, ledgerService.Namespace, pod, container,
		ledgerctlCommand(grpcPort, args...))
	if err != nil {
		return nil, fmt.Errorf("ledgerctl store incremental-backup: %w", err)
	}

	var resp incrementalBackupResult
	if err := json.Unmarshal([]byte(result.Stdout), &resp); err != nil {
		return nil, fmt.Errorf("parsing incremental backup output: %w (stdout: %s)", err, result.Stdout)
	}

	return &resp, nil
}
