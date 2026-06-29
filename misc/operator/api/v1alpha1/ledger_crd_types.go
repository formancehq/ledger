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
//
// The connection target is described by explicit fields (Host/Port/User/Database/SSLMode).
// Exactly one of PasswordFrom (static credentials via Secret) or AWSIAMAuth (RDS IAM
// token minted per connection) must be set. The operator assembles the DSN before
// invoking ledgerctl on the target pod.
type PostgresMirrorSource struct {
	// Host is the PostgreSQL endpoint hostname (e.g. RDS DB cluster endpoint).
	// +kubebuilder:validation:Required
	Host string `json:"host"`

	// Port is the PostgreSQL port. Defaults to 5432.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=65535
	// +kubebuilder:default=5432
	// +optional
	Port int32 `json:"port,omitempty"`

	// User is the PostgreSQL user.
	// +kubebuilder:validation:Required
	User string `json:"user"`

	// Database is the PostgreSQL database name.
	// +kubebuilder:validation:Required
	Database string `json:"database"`

	// SSLMode is the libpq sslmode parameter (disable, allow, prefer, require,
	// verify-ca, verify-full). Defaults to "require".
	// +kubebuilder:validation:Enum=disable;allow;prefer;require;verify-ca;verify-full
	// +kubebuilder:default=require
	// +optional
	SSLMode string `json:"sslMode,omitempty"`

	// PasswordFrom references a Kubernetes Secret containing the PostgreSQL password.
	// Mutually exclusive with AWSIAMAuth.
	// +optional
	PasswordFrom *SecretKeyRef `json:"passwordFrom,omitempty"`

	// AWSIAMAuth enables AWS RDS IAM authentication: the mirror mints a
	// short-lived (15 min) SigV4 token per connection from the ambient AWS
	// credential chain on the ledger pod (IRSA, instance profile, env, profile).
	// Mutually exclusive with PasswordFrom.
	//
	// In a typical EKS deployment, IRSA is wired by annotating the
	// LedgerService ServiceAccount with eks.amazonaws.com/role-arn, e.g.:
	//   kind: LedgerService
	//   spec:
	//     serviceAccount:
	//       annotations:
	//         eks.amazonaws.com/role-arn: arn:aws:iam::ACCOUNT:role/mirror-rds-iam
	// The bound IAM role must allow rds-db:connect on the RDS db-user ARN
	// (arn:aws:rds-db:REGION:ACCOUNT:dbuser:DB-RESOURCE-ID/USER). The role is
	// shared across every mirror in the LedgerService, so its policy must
	// cover every RDS endpoint addressed by Ledger CRDs in that service.
	// +optional
	AWSIAMAuth *AWSIAMAuthSpec `json:"awsIamAuth,omitempty"`
}

// AWSIAMAuthSpec configures AWS RDS IAM authentication for a Postgres mirror source.
type AWSIAMAuthSpec struct {
	// Region is the AWS region of the RDS instance (e.g. "eu-west-1").
	// Required to sign the IAM authentication token (SigV4).
	// +kubebuilder:validation:Required
	Region string `json:"region"`

	// AssumeRoleArn is an optional STS role ARN to assume before minting the
	// RDS IAM token. When set, the mirror calls sts:AssumeRole on this ARN
	// using the pod's ambient credentials and signs the RDS token with the
	// assumed credentials. This decouples each mirror's IAM identity from the
	// pod's base role, so a single LedgerService can mirror RDS instances
	// across multiple AWS accounts or tenants: the pod's base role only needs
	// sts:AssumeRole on the listed targets (no direct rds-db:connect grant).
	//
	// When left empty, the pod's ambient credentials are used directly and
	// must hold rds-db:connect on the target db-user ARN.
	// +optional
	AssumeRoleArn string `json:"assumeRoleArn,omitempty"`
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
