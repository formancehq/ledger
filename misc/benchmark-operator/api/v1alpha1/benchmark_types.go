package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// BenchmarkPhase describes the current lifecycle phase of a Benchmark.
// +kubebuilder:validation:Enum=Pending;CreatingCluster;WaitingForCluster;CreatingLedger;Running;Snapshotting;Completed;Failed
type BenchmarkPhase string

const (
	BenchmarkPhasePending         BenchmarkPhase = "Pending"
	BenchmarkPhaseCreatingCluster BenchmarkPhase = "CreatingCluster"
	BenchmarkPhaseWaitingCluster  BenchmarkPhase = "WaitingForCluster"
	BenchmarkPhaseCreatingLedger  BenchmarkPhase = "CreatingLedger"
	BenchmarkPhaseRunning         BenchmarkPhase = "Running"
	BenchmarkPhaseSnapshotting    BenchmarkPhase = "Snapshotting"
	BenchmarkPhaseCompleted       BenchmarkPhase = "Completed"
	BenchmarkPhaseFailed          BenchmarkPhase = "Failed"
)

// BenchmarkSpec defines the desired state of a Benchmark.
type BenchmarkSpec struct {
	// LedgerService is the inline spec for the LedgerService CR.
	// Passed through without validation (the ledger operator validates).
	// +kubebuilder:validation:Required
	// +kubebuilder:pruning:PreserveUnknownFields
	LedgerService runtime.RawExtension `json:"ledgerService"`

	// TestRun is the inline spec for the k6 TestRun CR.
	// Passed through without validation (the k6 operator validates).
	// +kubebuilder:validation:Required
	// +kubebuilder:pruning:PreserveUnknownFields
	TestRun runtime.RawExtension `json:"testRun"`

	// LedgerName is the name of the ledger to create via gRPC before the test
	// and delete after the test. If empty, no ledger lifecycle management is done.
	// +optional
	LedgerName string `json:"ledgerName,omitempty"`
}

// BenchmarkStatus defines the observed state of a Benchmark.
type BenchmarkStatus struct {
	// Phase is the current lifecycle phase.
	Phase BenchmarkPhase `json:"phase,omitempty"`

	// LedgerServiceName is the name of the managed LedgerService CR.
	LedgerServiceName string `json:"ledgerServiceName,omitempty"`

	// TestRunName is the name of the managed k6 TestRun CR.
	TestRunName string `json:"testRunName,omitempty"`

	// Message contains human-readable status information (e.g. failure reason).
	Message string `json:"message,omitempty"`

	// Report is the JSON Grafana snapshot report.
	Report string `json:"report,omitempty"`

	// StartTime is when the benchmark TestRun was created.
	// +optional
	StartTime *metav1.Time `json:"startTime,omitempty"`

	// CompletionTime is when the benchmark finished (completed or failed).
	// +optional
	CompletionTime *metav1.Time `json:"completionTime,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=bm
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
// +kubebuilder:printcolumn:name="Message",type=string,JSONPath=`.status.message`,priority=1

// Benchmark orchestrates a LedgerService deployment and k6 TestRun,
// then captures Grafana snapshots.
type Benchmark struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   BenchmarkSpec   `json:"spec,omitempty"`
	Status BenchmarkStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// BenchmarkList contains a list of Benchmark.
type BenchmarkList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Benchmark `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Benchmark{}, &BenchmarkList{})
}
