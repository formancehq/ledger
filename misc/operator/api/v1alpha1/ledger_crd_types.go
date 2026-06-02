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

	// Mode is the ledger mode: "normal" or "mirror".
	// A mirror ledger replicates data from a source system in read-only mode.
	// +kubebuilder:default="normal"
	// +kubebuilder:validation:Enum=normal;mirror
	// +optional
	Mode string `json:"mode,omitempty"`

	// MirrorSource configures the replication source when mode is "mirror".
	// Required when mode is "mirror".
	// +optional
	MirrorSource *MirrorSourceSpec `json:"mirrorSource,omitempty"`
}

// MirrorSourceSpec configures the source for mirror replication.
type MirrorSourceSpec struct {
	// LedgerName is the name of the ledger on the source system.
	// Defaults to the Ledger's spec.name if not set.
	// +optional
	LedgerName string `json:"ledgerName,omitempty"`

	// HTTP configures replication via the Ledger v2 HTTP API.
	// +optional
	HTTP *HTTPMirrorSource `json:"http,omitempty"`

	// Postgres configures replication via direct PostgreSQL access.
	// +optional
	Postgres *PostgresMirrorSource `json:"postgres,omitempty"`

	// BatchSize is the max number of log entries per replication batch (0 = default 100).
	// +optional
	BatchSize *int32 `json:"batchSize,omitempty"`
}

// HTTPMirrorSource configures HTTP-based mirror replication.
type HTTPMirrorSource struct {
	// BaseURL is the URL of the source Ledger v2 API.
	// +kubebuilder:validation:Required
	BaseURL string `json:"baseUrl"`

	// OAuth2 configures OAuth2 client credentials for authentication.
	// +optional
	OAuth2 *OAuth2ClientCredentials `json:"oauth2,omitempty"`
}

// OAuth2ClientCredentials configures OAuth2 client credentials flow.
type OAuth2ClientCredentials struct {
	// ClientID is the OAuth2 client ID.
	// +kubebuilder:validation:Required
	ClientID string `json:"clientId"`

	// ClientSecretFrom references a Kubernetes Secret containing the OAuth2 client secret.
	// +kubebuilder:validation:Required
	ClientSecretFrom SecretKeyRef `json:"clientSecretFrom"`

	// TokenEndpoint is the OAuth2 token endpoint URL.
	// +kubebuilder:validation:Required
	TokenEndpoint string `json:"tokenEndpoint"`

	// Scopes is the list of OAuth2 scopes to request.
	// +optional
	Scopes []string `json:"scopes,omitempty"`
}

// SecretKeyRef references a key within a Kubernetes Secret.
type SecretKeyRef struct {
	// Name is the Secret name.
	Name string `json:"name"`

	// Key is the key within the Secret.
	Key string `json:"key"`
}

// PostgresMirrorSource configures PostgreSQL-based mirror replication.
type PostgresMirrorSource struct {
	// DSNFrom references a Kubernetes Secret containing the PostgreSQL connection string.
	// +kubebuilder:validation:Required
	DSNFrom SecretKeyRef `json:"dsnFrom"`
}

// LedgerCRDStatus defines the observed state of a Ledger.
type LedgerCRDStatus struct {
	// Phase is the current lifecycle phase.
	Phase LedgerPhase `json:"phase,omitempty"`

	// Message contains human-readable status information (e.g. failure reason).
	Message string `json:"message,omitempty"`

	// Mode is the observed ledger mode. Tracks spec.mode until promotion completes.
	// +optional
	Mode string `json:"mode,omitempty"`

	// Conditions represent the latest available observations of the Ledger's state.
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// ObservedGeneration is the most recent generation observed by the controller.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// AppliedSpecHash is the hash of the spec at the time of creation.
	// Used to detect post-creation modifications (ledgers are immutable).
	// +optional
	AppliedSpecHash string `json:"appliedSpecHash,omitempty"`
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
