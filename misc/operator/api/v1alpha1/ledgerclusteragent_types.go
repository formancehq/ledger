package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Cluster
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="KeyID",type=string,JSONPath=`.status.keyID`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// LedgerClusterAgent is a cluster-scoped resource that describes an agent with
// Ed25519 authentication keys. The operator generates the keypair, stores the seed
// in a Secret, and injects the public key into matching LedgerService clusters.
type LedgerClusterAgent struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   LedgerClusterAgentSpec   `json:"spec,omitempty"`
	Status LedgerClusterAgentStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// LedgerClusterAgentList contains a list of LedgerClusterAgent.
type LedgerClusterAgentList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []LedgerClusterAgent `json:"items"`
}

// LedgerClusterAgentSpec defines the desired state of a LedgerClusterAgent.
type LedgerClusterAgentSpec struct {
	// Scopes is the list of authorization scopes this agent is allowed (e.g. "read", "write").
	Scopes []string `json:"scopes"`

	// Selector selects which LedgerService resources this agent applies to.
	Selector metav1.LabelSelector `json:"selector"`
}

// LedgerClusterAgentStatus defines the observed state of a LedgerClusterAgent.
type LedgerClusterAgentStatus struct {
	// Phase of the LedgerClusterAgent: Pending, Ready, Error.
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

// SecretReference identifies a namespaced Secret.
type SecretReference struct {
	// Namespace of the Secret.
	Namespace string `json:"namespace"`

	// Name of the Secret.
	Name string `json:"name"`
}

// MatchedService identifies a namespaced LedgerService matched by the agent selector.
type MatchedService struct {
	// Namespace of the LedgerService.
	Namespace string `json:"namespace"`

	// Name of the LedgerService.
	Name string `json:"name"`
}

func init() {
	SchemeBuilder.Register(&LedgerClusterAgent{}, &LedgerClusterAgentList{})
}
