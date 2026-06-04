package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// BackupRunPhase describes the current lifecycle phase of a LedgerBackupRun.
// +kubebuilder:validation:Enum=Pending;Running;Succeeded;Failed
type BackupRunPhase string

const (
	BackupRunPhasePending   BackupRunPhase = "Pending"
	BackupRunPhaseRunning   BackupRunPhase = "Running"
	BackupRunPhaseSucceeded BackupRunPhase = "Succeeded"
	BackupRunPhaseFailed    BackupRunPhase = "Failed"
)

// BackupRunType is the type of backup to execute.
// +kubebuilder:validation:Enum=Full;Incremental
type BackupRunType string

const (
	BackupRunTypeFull        BackupRunType = "Full"
	BackupRunTypeIncremental BackupRunType = "Incremental"
)

// Labels and annotations used to associate LedgerBackupRun resources with their parent.
const (
	// LabelLedgerBackup carries the name of the parent LedgerBackup.
	LabelLedgerBackup = "ledger.formance.com/backup"
	// LabelLedgerBackupRunType carries the BackupRunType of the run.
	LabelLedgerBackupRunType = "ledger.formance.com/backup-type"
)

// LedgerBackupRunSpec defines the desired state of a LedgerBackupRun.
type LedgerBackupRunSpec struct {
	// BackupRef is the name of the parent LedgerBackup in the same namespace.
	// Destination and serviceRef are inherited from the parent.
	// +kubebuilder:validation:Required
	BackupRef string `json:"backupRef"`

	// Type is the backup type. Defaults to Full.
	// +kubebuilder:default=Full
	// +optional
	Type BackupRunType `json:"type,omitempty"`
}

// LedgerBackupRunStatus defines the observed state of a LedgerBackupRun.
type LedgerBackupRunStatus struct {
	// Phase is the current lifecycle phase.
	// +optional
	Phase BackupRunPhase `json:"phase,omitempty"`

	// Message contains human-readable status information (typically an error message).
	// +optional
	Message string `json:"message,omitempty"`

	// StartTime is when the run transitioned to Running.
	// +optional
	StartTime *metav1.Time `json:"startTime,omitempty"`

	// CompletionTime is when the run reached a terminal phase (Succeeded or Failed).
	// +optional
	CompletionTime *metav1.Time `json:"completionTime,omitempty"`

	// Full holds the result of a Full backup run. Set only when spec.type is Full and the run succeeded.
	// +optional
	Full *FullBackupStatus `json:"full,omitempty"`

	// Incremental holds the result of an Incremental backup run. Set only when spec.type is Incremental and the run succeeded.
	// +optional
	Incremental *IncrementalBackupStatus `json:"incremental,omitempty"`

	// Conditions represent the latest available observations.
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=lbkr
// +kubebuilder:printcolumn:name="Backup",type=string,JSONPath=`.spec.backupRef`
// +kubebuilder:printcolumn:name="Type",type=string,JSONPath=`.spec.type`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Started",type=date,JSONPath=`.status.startTime`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// LedgerBackupRun represents a single backup execution triggered either by a LedgerBackup schedule
// or manually (e.g. via `kubectl ledger backup trigger`).
type LedgerBackupRun struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   LedgerBackupRunSpec   `json:"spec,omitempty"`
	Status LedgerBackupRunStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// LedgerBackupRunList contains a list of LedgerBackupRun.
type LedgerBackupRunList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`

	Items []LedgerBackupRun `json:"items"`
}

// IsTerminal reports whether the run is in a terminal phase (Succeeded or Failed).
func (r *LedgerBackupRun) IsTerminal() bool {
	return r.Status.Phase == BackupRunPhaseSucceeded || r.Status.Phase == BackupRunPhaseFailed
}

func init() {
	SchemeBuilder.Register(&LedgerBackupRun{}, &LedgerBackupRunList{})
}
