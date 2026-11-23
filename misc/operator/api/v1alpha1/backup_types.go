package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// BackupPhase describes the current lifecycle phase of a Backup.
// +kubebuilder:validation:Enum=Pending;Ready;Running;Failed
type BackupPhase string

const (
	BackupPhasePending BackupPhase = "Pending"
	BackupPhaseReady   BackupPhase = "Ready"
	BackupPhaseRunning BackupPhase = "Running"
	BackupPhaseFailed  BackupPhase = "Failed"
)

// BackupDestination defines where backup data is stored.
type BackupDestination struct {
	// Driver is the backup storage driver: "s3".
	// +kubebuilder:validation:Enum=s3
	// +kubebuilder:default=s3
	Driver string `json:"driver"`

	// BucketID is the namespace prefix for backup files.
	// Defaults to the cluster-id if not set.
	// +optional
	BucketID string `json:"bucketId,omitempty"`

	// S3 configuration (required when driver is "s3").
	// +optional
	S3 *S3Config `json:"s3,omitempty"`

	// S3AccessKeyIDFrom references a static AWS access key ID in a Secret in
	// the Backup namespace. Kubernetes injects it into the backup Job at runtime.
	// If not set, the default AWS credential chain is used on the Ledger server.
	// +optional
	S3AccessKeyIDFrom *SecretKeyRef `json:"s3AccessKeyIdFrom,omitempty"`

	// S3SecretAccessKeyFrom references a static AWS secret access key in a Secret
	// in the Backup namespace. The value never appears in the Backup or Job spec.
	// If not set, the default AWS credential chain is used on the Ledger server.
	// +optional
	S3SecretAccessKeyFrom *SecretKeyRef `json:"s3SecretAccessKeyFrom,omitempty"`
}

// BackupSpec defines the desired state of a Backup.
type BackupSpec struct {
	// ClusterRef is the name of the Cluster in the same namespace.
	// +kubebuilder:validation:Required
	ClusterRef string `json:"clusterRef"`

	// Destination defines where backups are stored.
	// +kubebuilder:validation:Required
	Destination BackupDestination `json:"destination"`

	// Schedule is the cron expression for checkpoint backups.
	// It is optional; a Backup without a schedule is valid
	// and acts as a backup configuration template for manual BackupRun resources.
	// +optional
	Schedule string `json:"schedule,omitempty"`

	// SuccessfulRunsHistoryLimit is the maximum number of successful BackupRun
	// resources to keep. Older runs are garbage-collected.
	// Defaults to 3.
	// +kubebuilder:default=3
	// +optional
	SuccessfulRunsHistoryLimit *int32 `json:"successfulRunsHistoryLimit,omitempty"`

	// FailedRunsHistoryLimit is the maximum number of failed BackupRun
	// resources to keep. Older runs are garbage-collected.
	// Defaults to 1.
	// +kubebuilder:default=1
	// +optional
	FailedRunsHistoryLimit *int32 `json:"failedRunsHistoryLimit,omitempty"`

	// Timeout caps how long the `ledgerctl store backup` RPC may run before the client
	// aborts. The operator forwards it as `--timeout` to the Job's
	// ledgerctl invocation; tune it for the size of the ledger and the
	// throughput of the destination bucket. Defaults to 1h, which is
	// enough for tens of GB on S3 while still bounding stuck runs.
	// +kubebuilder:default="1h"
	// +optional
	Timeout *metav1.Duration `json:"timeout,omitempty"`
}

// BackupResult holds the result of a completed checkpoint backup.
type BackupResult struct {
	// Time is when the last checkpoint completed.
	// +optional
	Time *metav1.Time `json:"time,omitempty"`

	// FilesUploaded is the number of files uploaded in the last checkpoint.
	// +optional
	FilesUploaded uint32 `json:"filesUploaded,omitempty"`

	// FilesDeleted is the number of stale files deleted in the last checkpoint
	// (the diff between the previous manifest and the new checkpoint).
	// +optional
	FilesDeleted uint32 `json:"filesDeleted,omitempty"`

	// OrphansDeleted is the number of orphan files pruned from storage in the
	// last checkpoint. Orphans are leftovers from previous failed runs that
	// crashed before writing a manifest.
	// +optional
	OrphansDeleted uint32 `json:"orphansDeleted,omitempty"`

	// TotalFiles is the total number of files in the backup after the last run.
	// +optional
	TotalFiles uint32 `json:"totalFiles,omitempty"`

	// DurationMs is the duration of the last checkpoint in milliseconds.
	// +optional
	DurationMs int64 `json:"durationMs,omitempty"`

	// LastLogSequence is the last log sequence included in the backup.
	// +optional
	LastLogSequence uint64 `json:"lastLogSequence,omitempty"`

	// LastAuditSequence is the last audit sequence included in the backup.
	// +optional
	LastAuditSequence uint64 `json:"lastAuditSequence,omitempty"`

	// LastAppliedIndex is the last Raft applied index in the backup.
	// +optional
	LastAppliedIndex uint64 `json:"lastAppliedIndex,omitempty"`
}

// BackupStatus defines the observed state of a Backup.
type BackupStatus struct {
	// Phase is the current lifecycle phase.
	// +optional
	Phase BackupPhase `json:"phase,omitempty"`

	// Message contains human-readable status information.
	// +optional
	Message string `json:"message,omitempty"`

	// LastBackup holds the result of the last successful checkpoint.
	// +optional
	LastBackup *BackupResult `json:"lastBackup,omitempty"`

	// LastRunCompletionTime is the latest observed terminal run completion,
	// successful or failed. It is preserved independently of run history retention.
	// +optional
	LastRunCompletionTime *metav1.Time `json:"lastRunCompletionTime,omitempty"`

	// NextBackupTime is the next scheduled checkpoint time.
	// +optional
	NextBackupTime *metav1.Time `json:"nextBackupTime,omitempty"`

	// Conditions represent the latest available observations.
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=bk
// +kubebuilder:printcolumn:name="Cluster",type=string,JSONPath=`.spec.clusterRef`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Last Backup",type=date,JSONPath=`.status.lastBackup.time`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// Backup manages scheduled backups of a Cluster to S3 via ledgerctl.
type Backup struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   BackupSpec   `json:"spec,omitempty"`
	Status BackupStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// BackupList contains a list of Backup.
type BackupList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`

	Items []Backup `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Backup{}, &BackupList{})
}
