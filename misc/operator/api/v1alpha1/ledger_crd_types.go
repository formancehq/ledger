package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// LedgerPhase describes the current lifecycle phase of a Ledger.
// +kubebuilder:validation:Enum=Pending;Ready;Failed
type LedgerPhase string

const (
	LedgerPhasePending LedgerPhase = "Pending"
	LedgerPhaseReady   LedgerPhase = "Ready"
	LedgerPhaseFailed  LedgerPhase = "Failed"
)

// LedgerCRDSpec defines the desired state of a Ledger.
type LedgerCRDSpec struct {
	// Name is the ledger name to create/delete via gRPC.
	// +kubebuilder:validation:Required
	Name string `json:"name"`

	// ServiceRef is the name of the LedgerService in the same namespace
	// that provides the gRPC endpoint.
	// +kubebuilder:validation:Required
	ServiceRef string `json:"serviceRef"`
}

// LedgerCRDStatus defines the observed state of a Ledger.
type LedgerCRDStatus struct {
	// Phase is the current lifecycle phase.
	Phase LedgerPhase `json:"phase,omitempty"`

	// Message contains human-readable status information (e.g. failure reason).
	Message string `json:"message,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=ldg
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
// +kubebuilder:printcolumn:name="Message",type=string,JSONPath=`.status.message`,priority=1

// Ledger manages the lifecycle of a ledger on a LedgerService via gRPC.
type Ledger struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   LedgerCRDSpec   `json:"spec,omitempty"`
	Status LedgerCRDStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// LedgerList contains a list of Ledger.
type LedgerList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`

	Items []Ledger `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Ledger{}, &LedgerList{})
}
