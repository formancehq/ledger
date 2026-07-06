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

// Credentials is a cluster-scoped resource that describes an agent with
// Ed25519 authentication keys. The operator generates the keypair, stores the seed
// in a Secret, and injects the public key into matching Cluster clusters.
type Credentials struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   CredentialsSpec   `json:"spec,omitempty"`
	Status CredentialsStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// CredentialsList contains a list of Credentials.
type CredentialsList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`

	Items []Credentials `json:"items"`
}

// CredentialsSpec defines the desired state of a Credentials.
type CredentialsSpec struct {
	// Scopes is the list of authorization scopes this agent is allowed (e.g. "read", "write").
	// +optional
	Scopes []string `json:"scopes,omitempty"`

	// God enables god mode for this agent. God-mode agents receive all scopes
	// regardless of the Scopes field and can bypass scope checks entirely.
	// +optional
	God bool `json:"god,omitempty"`

	// Selector selects which Cluster resources this agent applies to.
	Selector metav1.LabelSelector `json:"selector"`

	// AdditionalNamespaces is an optional list of extra namespaces where the
	// agent's Secret must also be created, in addition to the namespaces of
	// the Clusters matched by Selector.
	// +optional
	AdditionalNamespaces []string `json:"additionalNamespaces,omitempty"`
}

// CredentialsStatus defines the observed state of a Credentials.
type CredentialsStatus struct {
	// Phase of the Credentials: Pending, Ready, Error.
	// +optional
	Phase string `json:"phase,omitempty"`

	// KeyID is the SHA-256 fingerprint prefix (16 hex chars) of the public key.
	// +optional
	KeyID string `json:"keyID,omitempty"`

	// DistributedSecretRefs lists every namespace where the agent's Secret has
	// been written. All replicas carry identical data; consumers can read any
	// entry — typically the first.
	// +optional
	DistributedSecretRefs []SecretReference `json:"distributedSecretRefs,omitempty"`

	// MatchedServices lists the Cluster resources matched by the selector.
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

// MatchedService identifies a namespaced Cluster matched by the agent selector.
type MatchedService struct {
	// Namespace of the Cluster.
	Namespace string `json:"namespace"`

	// Name of the Cluster.
	Name string `json:"name"`
}

func init() {
	SchemeBuilder.Register(&Credentials{}, &CredentialsList{})
}
