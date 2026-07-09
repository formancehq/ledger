package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// BackupRunPhase describes the current lifecycle phase of a BackupRun.
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

// Labels and annotations used to associate BackupRun resources with their parent.
const (
	// LabelBackup carries the name of the parent Backup.
	LabelBackup = "ledger.formance.com/backup"
	// LabelBackupRunType carries the BackupRunType of the run.
	LabelBackupRunType = "ledger.formance.com/backup-type"
)

// BackupRunSpec defines the desired state of a BackupRun.
type BackupRunSpec struct {
	// BackupRef is the name of the parent Backup in the same namespace.
	// Destination and serviceRef are inherited from the parent.
	// +kubebuilder:validation:Required
	BackupRef string `json:"backupRef"`

	// Type is the backup type. Defaults to Full.
	// +kubebuilder:default=Full
	// +optional
	Type BackupRunType `json:"type,omitempty"`
}

// BackupRunStatus defines the observed state of a BackupRun.
type BackupRunStatus struct {
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
// +kubebuilder:resource:shortName=bkr
// +kubebuilder:printcolumn:name="Backup",type=string,JSONPath=`.spec.backupRef`
// +kubebuilder:printcolumn:name="Type",type=string,JSONPath=`.spec.type`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Started",type=date,JSONPath=`.status.startTime`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// BackupRun represents a single backup execution triggered either by a Backup schedule
// or manually (e.g. via `kubectl ledger backup trigger`).
type BackupRun struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   BackupRunSpec   `json:"spec,omitempty"`
	Status BackupRunStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// BackupRunList contains a list of BackupRun.
type BackupRunList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`

	Items []BackupRun `json:"items"`
}

// IsTerminal reports whether the run is in a terminal phase (Succeeded or Failed).
func (r *BackupRun) IsTerminal() bool {
	return r.Status.Phase == BackupRunPhaseSucceeded || r.Status.Phase == BackupRunPhaseFailed
}

func init() {
	SchemeBuilder.Register(&BackupRun{}, &BackupRunList{})
}
