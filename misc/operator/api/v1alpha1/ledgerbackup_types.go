package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// BackupPhase describes the current lifecycle phase of a LedgerBackup.
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

	// S3AccessKeyID is a static AWS access key ID for S3 authentication.
	// If not set, the default AWS credential chain is used (env vars, IRSA, etc.).
	// +optional
	S3AccessKeyID string `json:"s3AccessKeyId,omitempty"`

	// S3SecretAccessKey is a static AWS secret access key for S3 authentication.
	// If not set, the default AWS credential chain is used.
	// +optional
	S3SecretAccessKey string `json:"s3SecretAccessKey,omitempty"`
}

// BackupSchedule defines cron schedules for full and incremental backups.
type BackupSchedule struct {
	// Full is the cron expression for full backups (e.g. "0 2 * * 0" for weekly Sunday 2am).
	// +optional
	Full string `json:"full,omitempty"`

	// Incremental is the cron expression for incremental backups (e.g. "0 * * * *" for hourly).
	// +optional
	Incremental string `json:"incremental,omitempty"`
}

// LedgerBackupSpec defines the desired state of a LedgerBackup.
type LedgerBackupSpec struct {
	// ServiceRef is the name of the LedgerService in the same namespace.
	// +kubebuilder:validation:Required
	ServiceRef string `json:"serviceRef"`

	// Destination defines where backups are stored.
	// +kubebuilder:validation:Required
	Destination BackupDestination `json:"destination"`

	// Schedule defines cron schedules for full and incremental backups.
	// Both entries are optional; a LedgerBackup without any schedule is valid
	// and acts as a backup configuration template for manual LedgerBackupRun resources.
	// +optional
	Schedule BackupSchedule `json:"schedule,omitempty"`

	// SuccessfulRunsHistoryLimit is the maximum number of successful LedgerBackupRun
	// resources to keep per type (full/incremental). Older runs are garbage-collected.
	// Defaults to 3.
	// +kubebuilder:default=3
	// +optional
	SuccessfulRunsHistoryLimit *int32 `json:"successfulRunsHistoryLimit,omitempty"`

	// FailedRunsHistoryLimit is the maximum number of failed LedgerBackupRun
	// resources to keep per type (full/incremental). Older runs are garbage-collected.
	// Defaults to 1.
	// +kubebuilder:default=1
	// +optional
	FailedRunsHistoryLimit *int32 `json:"failedRunsHistoryLimit,omitempty"`
}

// FullBackupStatus holds the result of the last full backup.
type FullBackupStatus struct {
	// Time is when the last full backup completed.
	// +optional
	Time *metav1.Time `json:"time,omitempty"`

	// FilesUploaded is the number of files uploaded in the last full backup.
	// +optional
	FilesUploaded uint32 `json:"filesUploaded,omitempty"`

	// FilesDeleted is the number of stale files deleted in the last full backup.
	// +optional
	FilesDeleted uint32 `json:"filesDeleted,omitempty"`

	// TotalFiles is the total number of files in the backup after the last run.
	// +optional
	TotalFiles uint32 `json:"totalFiles,omitempty"`

	// DurationMs is the duration of the last full backup in milliseconds.
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

// IncrementalBackupStatus holds the result of the last incremental backup.
type IncrementalBackupStatus struct {
	// Time is when the last incremental backup completed.
	// +optional
	Time *metav1.Time `json:"time,omitempty"`

	// LogEntriesExported is the number of log entries exported.
	// +optional
	LogEntriesExported uint64 `json:"logEntriesExported,omitempty"`

	// AuditEntriesExported is the number of audit entries exported.
	// +optional
	AuditEntriesExported uint64 `json:"auditEntriesExported,omitempty"`

	// SegmentsUploaded is the number of segments uploaded.
	// +optional
	SegmentsUploaded uint32 `json:"segmentsUploaded,omitempty"`

	// DurationMs is the duration of the last incremental backup in milliseconds.
	// +optional
	DurationMs int64 `json:"durationMs,omitempty"`

	// LastLogSequence is the last log sequence in the incremental backup.
	// +optional
	LastLogSequence uint64 `json:"lastLogSequence,omitempty"`

	// LastAuditSequence is the last audit sequence in the incremental backup.
	// +optional
	LastAuditSequence uint64 `json:"lastAuditSequence,omitempty"`
}

// LedgerBackupStatus defines the observed state of a LedgerBackup.
type LedgerBackupStatus struct {
	// Phase is the current lifecycle phase.
	// +optional
	Phase BackupPhase `json:"phase,omitempty"`

	// Message contains human-readable status information.
	// +optional
	Message string `json:"message,omitempty"`

	// LastFullBackup holds the result of the last full backup.
	// +optional
	LastFullBackup *FullBackupStatus `json:"lastFullBackup,omitempty"`

	// LastIncrementalBackup holds the result of the last incremental backup.
	// +optional
	LastIncrementalBackup *IncrementalBackupStatus `json:"lastIncrementalBackup,omitempty"`

	// NextFullBackupTime is the next scheduled time for a full backup.
	// +optional
	NextFullBackupTime *metav1.Time `json:"nextFullBackupTime,omitempty"`

	// NextIncrementalBackupTime is the next scheduled time for an incremental backup.
	// +optional
	NextIncrementalBackupTime *metav1.Time `json:"nextIncrementalBackupTime,omitempty"`

	// Conditions represent the latest available observations.
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=lbk
// +kubebuilder:printcolumn:name="Service",type=string,JSONPath=`.spec.serviceRef`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Last Full",type=date,JSONPath=`.status.lastFullBackup.time`
// +kubebuilder:printcolumn:name="Last Incremental",type=date,JSONPath=`.status.lastIncrementalBackup.time`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// LedgerBackup manages scheduled backups of a LedgerService to S3 via ledgerctl.
type LedgerBackup struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   LedgerBackupSpec   `json:"spec,omitempty"`
	Status LedgerBackupStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// LedgerBackupList contains a list of LedgerBackup.
type LedgerBackupList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`

	Items []LedgerBackup `json:"items"`
}

func init() {
	SchemeBuilder.Register(&LedgerBackup{}, &LedgerBackupList{})
}
