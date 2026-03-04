package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="KeyID",type=string,JSONPath=`.status.keyID`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// LedgerAgent is a namespace-scoped resource that describes an agent with
// Ed25519 authentication keys. Like LedgerClusterAgent but scoped to a single
// namespace — it only matches LedgerService resources in the same namespace.
// The operator generates the keypair, stores the seed in a Secret (same namespace),
// and injects the public key into matching LedgerService clusters.
type LedgerAgent struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   LedgerAgentSpec   `json:"spec,omitempty"`
	Status LedgerAgentStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// LedgerAgentList contains a list of LedgerAgent.
type LedgerAgentList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []LedgerAgent `json:"items"`
}

// LedgerAgentSpec defines the desired state of a LedgerAgent.
type LedgerAgentSpec struct {
	// Scopes is the list of authorization scopes this agent is allowed (e.g. "read", "write").
	Scopes []string `json:"scopes"`

	// Selector selects which LedgerService resources (in the same namespace) this agent applies to.
	Selector metav1.LabelSelector `json:"selector"`
}

// LedgerAgentStatus defines the observed state of a LedgerAgent.
type LedgerAgentStatus struct {
	// Phase of the LedgerAgent: Pending, Ready, Error.
	// +optional
	Phase string `json:"phase,omitempty"`

	// KeyID is the SHA-256 fingerprint prefix (16 hex chars) of the public key.
	// +optional
	KeyID string `json:"keyID,omitempty"`

	// SecretRef references the Secret containing the generated Ed25519 keypair.
	// +optional
	SecretRef SecretReference `json:"secretRef,omitempty"`

	// MatchedServices lists the LedgerService resources matched by the selector.
	// +optional
	MatchedServices []MatchedService `json:"matchedServices,omitempty"`

	// ObservedGeneration is the generation last observed by the controller.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Conditions represent the latest available observations.
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

func init() {
	SchemeBuilder.Register(&LedgerAgent{}, &LedgerAgentList{})
}
