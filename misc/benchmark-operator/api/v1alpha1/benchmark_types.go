package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// BenchmarkPhase describes the current lifecycle phase of a Benchmark.
// +kubebuilder:validation:Enum=Pending;CreatingResources;WaitingForResources;Running;Snapshotting;Completed;Failed
type BenchmarkPhase string

const (
	BenchmarkPhasePending             BenchmarkPhase = "Pending"
	BenchmarkPhaseCreatingResources   BenchmarkPhase = "CreatingResources"
	BenchmarkPhaseWaitingForResources BenchmarkPhase = "WaitingForResources"
	BenchmarkPhaseRunning             BenchmarkPhase = "Running"
	BenchmarkPhaseSnapshotting        BenchmarkPhase = "Snapshotting"
	BenchmarkPhaseCompleted           BenchmarkPhase = "Completed"
	BenchmarkPhaseFailed              BenchmarkPhase = "Failed"
)

// ResourceEntry describes a Kubernetes resource to create before the test
// and its readiness condition.
type ResourceEntry struct {
	// Manifest is the full resource manifest to create (apiVersion, kind, spec, etc.).
	// +kubebuilder:validation:Required
	// +kubebuilder:pruning:PreserveUnknownFields
	Manifest runtime.RawExtension `json:"manifest"`

	// ReadyCondition defines how to determine if the resource is ready.
	ReadyCondition ReadyCondition `json:"readyCondition"`
}

// ReadyCondition specifies a field path and expected value to check readiness.
type ReadyCondition struct {
	// FieldPath is the dot-separated path to the field (e.g. "status.phase").
	FieldPath string `json:"fieldPath"`

	// Value is the expected value at the field path.
	Value string `json:"value"`
}

// BenchmarkSpec defines the desired state of a Benchmark.
type BenchmarkSpec struct {
	// Resources is an ordered list of Kubernetes resources to create before the test.
	// Each resource is created in order and must reach its readyCondition before the next is created.
	// +optional
	Resources []ResourceEntry `json:"resources,omitempty"`

	// TestRun is the inline spec for the k6 TestRun CR.
	// Passed through without validation (the k6 operator validates).
	// +kubebuilder:validation:Required
	// +kubebuilder:pruning:PreserveUnknownFields
	TestRun runtime.RawExtension `json:"testRun"`
}

// BenchmarkStatus defines the observed state of a Benchmark.
type BenchmarkStatus struct {
	// Phase is the current lifecycle phase.
	Phase BenchmarkPhase `json:"phase,omitempty"`

	// ResourceNames is the list of created resource names (in creation order).
	ResourceNames []string `json:"resourceNames,omitempty"`

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

// Benchmark orchestrates resource creation and k6 TestRun execution,
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

	Items []Benchmark `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Benchmark{}, &BenchmarkList{})
}
