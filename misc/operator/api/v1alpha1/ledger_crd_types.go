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

	// ClusterRef is the name of the Cluster in the same namespace
	// that provides the gRPC endpoint.
	// +kubebuilder:validation:Required
	ClusterRef string `json:"clusterRef"`

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

	// Indexes declares the account/transaction indexes the operator maintains
	// on this ledger. This is especially useful for mirror ledgers, which are
	// read-only and provisioned entirely through this CRD: it lets you keep the
	// mirror queryable without hand-running `ledgerctl indexes create`.
	//
	// Ownership semantics:
	//   - Field ABSENT (nil) => unmanaged. The operator never lists, creates,
	//     or drops indexes on this ledger. This is the safe default so an
	//     operator upgrade does not touch pre-existing indexes.
	//   - Field PRESENT (even empty {}) => managed. The operator reconciles the
	//     indexes it owns to match this spec: it creates the declared indexes
	//     and drops indexes it previously created that are no longer declared.
	//     It only ever drops indexes it created itself (tracked in
	//     status.appliedIndexes) — externally-created indexes and index kinds
	//     this CRD cannot express are left untouched. An empty {} therefore
	//     means "drop the indexes I previously managed", not "drop every index".
	//
	// Index maintenance is independent of ledger immutability: editing this
	// field never trips the SpecDrifted condition.
	// +optional
	Indexes *LedgerIndexesSpec `json:"indexes,omitempty"`
}

// LedgerIndexesSpec declares the set of indexes the operator maintains on a
// ledger. A nil *LedgerIndexesSpec means "unmanaged"; a non-nil value (even
// with all lists empty) means "managed" (see LedgerCRDSpec.Indexes).
type LedgerIndexesSpec struct {
	// Transaction lists the builtin transaction indexes to maintain.
	// +optional
	// +listType=set
	// +kubebuilder:validation:items:Enum=reference;timestamp;address;sourceAddress;destinationAddress;insertedAt;revertedAt
	Transaction []string `json:"transaction,omitempty"`

	// Account lists the builtin account indexes to maintain. Only "asset"
	// (which backs the `has asset` filter) is supported today.
	// +optional
	// +listType=set
	// +kubebuilder:validation:items:Enum=asset
	Account []string `json:"account,omitempty"`

	// Metadata lists the metadata-key indexes to maintain on account or
	// transaction metadata. Each (target, key) pair must be unique.
	// +optional
	// +kubebuilder:validation:MaxItems=64
	// +kubebuilder:validation:XValidation:rule="self.all(x, self.exists_one(y, y.target == x.target && y.key == x.key))",message="metadata index (target,key) pairs must be unique"
	Metadata []MetadataIndexSpec `json:"metadata,omitempty"`
}

// MetadataIndexSpec declares one metadata-key index. Creating a metadata index
// requires the metadata field to exist in the ledger schema first, so the
// operator declares the field type (via `ledgers set-metadata-type`) before
// creating the index. The declared field type is reconciled too: changing Type
// re-declares the field, which the server treats as a schema change (it bumps
// the index forward-encoding version and schedules a rewrite).
type MetadataIndexSpec struct {
	// Target of the metadata key.
	// +kubebuilder:validation:Enum=account;transaction
	Target string `json:"target"`

	// Key is the metadata key name to index.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=256
	Key string `json:"key"`

	// Type is the metadata field type declared before indexing.
	// +kubebuilder:validation:Enum=string;int64;bool;uint64;int8;int16;int32;uint8;uint16;uint32;datetime
	Type string `json:"type"`
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

	// RewriteRules are CEL rewrite rules applied, in order, to every mirror log
	// entry as the mirror translates v2 source logs into v3 orders. Each rule can
	// rename address segments, transform metadata, or drop transactions (see
	// MirrorRewriteRule). Applies to both HTTP and Postgres sources.
	// +optional
	RewriteRules []MirrorRewriteRule `json:"rewriteRules,omitempty"`
}

// MirrorRewriteRule transforms a mirror log entry during v2→v3 translation.
// A rule is scoped to exactly one variant of MirrorLogEntry.data. That scope
// determines the CEL type of `log` in the rule's predicate and the set of
// actions the rule can carry: `set_account_metadata` inside a
// `savedMetadata` rule is impossible to write, because the proto oneof
// doesn't include it. When Stop is true and the rule matches (scope +
// predicate), no further rules are evaluated. Rewriting is a deterministic
// translation-time projection: the source v2 ledger is never modified.
//
// Exactly one of the scope fields must be set.
type MirrorRewriteRule struct {
	// +optional
	CreatedTransaction *CreatedTransactionRule `json:"createdTransaction,omitempty"`
	// +optional
	RevertedTransaction *RevertedTransactionRule `json:"revertedTransaction,omitempty"`
	// +optional
	SavedMetadata *SavedMetadataRule `json:"savedMetadata,omitempty"`
	// +optional
	DeletedMetadata *DeletedMetadataRule `json:"deletedMetadata,omitempty"`
	// +optional
	AnyVariant *AnyVariantRule `json:"anyVariant,omitempty"`

	// Stop halts the rule chain once this rule matches (scope + predicate).
	// +optional
	Stop bool `json:"stop,omitempty"`
}

// CreatedTransactionRule scopes to MirrorCreatedTransaction. Inside `match`,
// `log` is typed as the created-transaction proto — write `log.metadata["k"]`
// directly, not `log.created_transaction.metadata["k"]`.
type CreatedTransactionRule struct {
	// +optional
	Match string `json:"match,omitempty"`
	// +optional
	Actions []CreatedTransactionAction `json:"actions,omitempty"`
}

type RevertedTransactionRule struct {
	// +optional
	Match string `json:"match,omitempty"`
	// +optional
	Actions []RevertedTransactionAction `json:"actions,omitempty"`
}

type SavedMetadataRule struct {
	// +optional
	Match string `json:"match,omitempty"`
	// +optional
	Actions []SavedMetadataAction `json:"actions,omitempty"`
}

type DeletedMetadataRule struct {
	// +optional
	Match string `json:"match,omitempty"`
	// +optional
	Actions []DeletedMetadataAction `json:"actions,omitempty"`
}

// AnyVariantRule scopes to every variant. `log` is typed as MirrorLogEntry, so
// the predicate must use `has(log.<variant>)` guards. The action set is
// restricted to operations that make sense on every variant.
type AnyVariantRule struct {
	// +optional
	Match string `json:"match,omitempty"`
	// +optional
	Actions []AnyVariantAction `json:"actions,omitempty"`
}

// Per-variant action oneofs. Exactly one field is set per action; setting
// none, or more than one, is rejected server-side at admission.

type CreatedTransactionAction struct {
	// +optional
	RewriteAddress *RewriteAddressAction `json:"rewriteAddress,omitempty"`
	// +optional
	SetMetadata *SetMetadataAction `json:"setMetadata,omitempty"`
	// +optional
	DeleteMetadata *DeleteMetadataAction `json:"deleteMetadata,omitempty"`
	// +optional
	SetAccountMetadata *SetAccountMetadataAction `json:"setAccountMetadata,omitempty"`
	// +optional
	DeleteAccountMetadata *DeleteAccountMetadataAction `json:"deleteAccountMetadata,omitempty"`
	// +optional
	SetAccountMetadataFromAddress *SetAccountMetadataFromAddressAction `json:"setAccountMetadataFromAddress,omitempty"`
	// +optional
	Drop *DropAction `json:"drop,omitempty"`
}

type RevertedTransactionAction struct {
	// +optional
	RewriteAddress *RewriteAddressAction `json:"rewriteAddress,omitempty"`
	// +optional
	SetMetadata *SetMetadataAction `json:"setMetadata,omitempty"`
	// +optional
	DeleteMetadata *DeleteMetadataAction `json:"deleteMetadata,omitempty"`
	// +optional
	Drop *DropAction `json:"drop,omitempty"`
}

type SavedMetadataAction struct {
	// +optional
	RewriteAddress *RewriteAddressAction `json:"rewriteAddress,omitempty"`
	// +optional
	SetMetadata *SetMetadataAction `json:"setMetadata,omitempty"`
	// +optional
	DeleteMetadata *DeleteMetadataAction `json:"deleteMetadata,omitempty"`
	// +optional
	Drop *DropAction `json:"drop,omitempty"`
}

type DeletedMetadataAction struct {
	// +optional
	RewriteAddress *RewriteAddressAction `json:"rewriteAddress,omitempty"`
	// +optional
	Drop *DropAction `json:"drop,omitempty"`
}

type AnyVariantAction struct {
	// +optional
	RewriteAddress *RewriteAddressAction `json:"rewriteAddress,omitempty"`
	// +optional
	Drop *DropAction `json:"drop,omitempty"`
}

// Shared action payloads. Each carries only the parameters an operator
// supplies for that operation — no CEL, no evaluation.

type RewriteAddressAction struct {
	Pattern     string `json:"pattern"`
	Replacement string `json:"replacement"`
}

// SetMetadataAction sets a metadata key. `value` is a literal string; `valueExpr`
// is a CEL expression evaluated against the current variant at commit time
// (e.g. `log.reference`). Exactly one should be set; setting neither writes an
// empty string. Optional `type` coerces the produced string into a typed
// MetadataValue — one of "string" (default), "int64", "bool", "uint64",
// "int8"/"int16"/"int32", "uint8"/"uint16"/"uint32", "datetime".
type SetMetadataAction struct {
	Key string `json:"key"`
	// +optional
	Value string `json:"value,omitempty"`
	// +optional
	ValueExpr string `json:"valueExpr,omitempty"`
	// +optional
	Type string `json:"type,omitempty"`
}

type DeleteMetadataAction struct {
	Key string `json:"key"`
}

// SetAccountMetadataAction — see SetMetadataAction for the literal / valueExpr /
// type semantics on the value field.
type SetAccountMetadataAction struct {
	Account string `json:"account"`
	Key     string `json:"key"`
	// +optional
	Value string `json:"value,omitempty"`
	// +optional
	ValueExpr string `json:"valueExpr,omitempty"`
	// +optional
	Type string `json:"type,omitempty"`
}

type DeleteAccountMetadataAction struct {
	Account string `json:"account"`
	Key     string `json:"key"`
}

// SetAccountMetadataFromAddressAction runs a single RE2 pattern over every
// posting address and, for each match, writes one or more metadata entries
// on the matched account using group-capturing replacements.
type SetAccountMetadataFromAddressAction struct {
	Pattern      string                                     `json:"pattern"`
	Replacements []SetAccountMetadataFromAddressReplacement `json:"replacements"`
}

type SetAccountMetadataFromAddressReplacement struct {
	Key         string `json:"key"`
	Replacement string `json:"replacement"`
	// +optional
	Type string `json:"type,omitempty"`
}

type DropAction struct{}

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
//
// +kubebuilder:validation:XValidation:rule="has(self.passwordFrom) != has(self.awsIamAuth)",message="exactly one of passwordFrom or awsIamAuth must be set"
// +kubebuilder:validation:XValidation:rule="!has(self.awsIamAuth) || !has(self.sslMode) || self.sslMode in ['require','verify-ca','verify-full']",message="awsIamAuth requires sslMode in {require, verify-ca, verify-full}; non-TLS sslmodes would let the SigV4 bearer token travel in cleartext"
type PostgresMirrorSource struct {
	// Host is the PostgreSQL endpoint hostname (e.g. RDS DB cluster endpoint).
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	Host string `json:"host"`

	// Port is the PostgreSQL port. Defaults to 5432.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=65535
	// +kubebuilder:default=5432
	// +optional
	Port int32 `json:"port,omitempty"`

	// User is the PostgreSQL user.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	User string `json:"user"`

	// Database is the PostgreSQL database name.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
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
	// Cluster ServiceAccount with eks.amazonaws.com/role-arn, e.g.:
	//   kind: Cluster
	//   spec:
	//     serviceAccount:
	//       annotations:
	//         eks.amazonaws.com/role-arn: arn:aws:iam::ACCOUNT:role/mirror-rds-iam
	// The bound IAM role must allow rds-db:connect on the RDS db-user ARN
	// (arn:aws:rds-db:REGION:ACCOUNT:dbuser:DB-RESOURCE-ID/USER). The role is
	// shared across every mirror in the Cluster, so its policy must
	// cover every RDS endpoint addressed by Ledger CRDs in that service.
	// +optional
	AWSIAMAuth *AWSIAMAuthSpec `json:"awsIamAuth,omitempty"`
}

// AWSIAMAuthSpec configures AWS RDS IAM authentication for a Postgres mirror source.
type AWSIAMAuthSpec struct {
	// Region is the AWS region of the RDS instance (e.g. "eu-west-1").
	// Required to sign the IAM authentication token (SigV4).
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	Region string `json:"region"`

	// AssumeRoleArn is an optional STS role ARN to assume before minting the
	// RDS IAM token. When set, the mirror calls sts:AssumeRole on this ARN
	// using the pod's ambient credentials and signs the RDS token with the
	// assumed credentials. This decouples each mirror's IAM identity from the
	// pod's base role, so a single Cluster can mirror RDS instances
	// across multiple AWS accounts or tenants: the pod's base role only needs
	// sts:AssumeRole on the listed targets (no direct rds-db:connect grant).
	//
	// When left empty, the pod's ambient credentials are used directly and
	// must hold rds-db:connect on the target db-user ARN.
	// +kubebuilder:validation:MinLength=1
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
	// The indexes field is excluded from this hash since it is mutable.
	// +optional
	AppliedSpecHash string `json:"appliedSpecHash,omitempty"`

	// AppliedIndexes is the set of index identifiers (canonical form) the
	// operator has created on this ledger. It is the operator-owned set that
	// scopes index drops: only indexes listed here are ever dropped, so
	// externally-created and CRD-unrepresentable indexes are preserved.
	// +optional
	// +listType=atomic
	AppliedIndexes []string `json:"appliedIndexes,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=ldg
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
// +kubebuilder:printcolumn:name="Message",type=string,JSONPath=`.status.message`,priority=1

// Ledger manages the lifecycle of a ledger on a Cluster via gRPC.
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
