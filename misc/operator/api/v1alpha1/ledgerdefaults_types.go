package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Cluster
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// LedgerDefaults is a cluster-scoped resource that provides shared default
// values for LedgerService deployments. A LedgerService references it via spec.defaultsRef.
type LedgerDefaults struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec LedgerDefaultsSpec `json:"spec,omitempty"`
}

// +kubebuilder:object:root=true

// LedgerDefaultsList contains a list of LedgerDefaults.
type LedgerDefaultsList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []LedgerDefaults `json:"items"`
}

// LedgerDefaultsSpec defines shared defaults that can be referenced by multiple LedgerServices.
// All fields are optional; only non-zero values are applied as defaults.
type LedgerDefaultsSpec struct {
	// Image configuration for the ledger container.
	// +optional
	Image ImageSpec `json:"image,omitempty"`

	// ImagePullSecrets for private registries.
	// +optional
	ImagePullSecrets []corev1.LocalObjectReference `json:"imagePullSecrets,omitempty"`

	// ServiceAccount configuration.
	// +optional
	ServiceAccount ServiceAccountSpec `json:"serviceAccount,omitempty"`

	// Config holds shared application configuration defaults.
	// +optional
	Config LedgerDefaultsConfig `json:"config,omitempty"`

	// Resources for the ledger container.
	// +optional
	Resources corev1.ResourceRequirements `json:"resources,omitempty"`

	// LivenessProbe configuration.
	// +optional
	LivenessProbe *corev1.Probe `json:"livenessProbe,omitempty"`

	// ReadinessProbe configuration.
	// +optional
	ReadinessProbe *corev1.Probe `json:"readinessProbe,omitempty"`

	// PodSecurityContext for the pod.
	// +optional
	PodSecurityContext *corev1.PodSecurityContext `json:"podSecurityContext,omitempty"`

	// SecurityContext for the container.
	// +optional
	SecurityContext *corev1.SecurityContext `json:"securityContext,omitempty"`

	// NodeSelector for pod scheduling.
	// +optional
	NodeSelector map[string]string `json:"nodeSelector,omitempty"`

	// Tolerations for pod scheduling.
	// +optional
	Tolerations []corev1.Toleration `json:"tolerations,omitempty"`

	// Affinity rules for pod scheduling.
	// +optional
	Affinity *corev1.Affinity `json:"affinity,omitempty"`

	// PodAntiAffinity configuration.
	// +optional
	PodAntiAffinity *PodAntiAffinitySpec `json:"podAntiAffinity,omitempty"`

	// PodDisruptionBudget configuration.
	// +optional
	PodDisruptionBudget *PodDisruptionBudgetSpec `json:"podDisruptionBudget,omitempty"`

	// ServiceMonitor configuration for Prometheus.
	// +optional
	ServiceMonitor *ServiceMonitorSpec `json:"serviceMonitor,omitempty"`

	// NetworkPolicy configuration for egress restrictions.
	// +optional
	NetworkPolicy *NetworkPolicySpec `json:"networkPolicy,omitempty"`
}

// LedgerDefaultsConfig holds the subset of LedgerServiceConfig fields that are safe
// to share across LedgerService deployments. Instance-specific fields like clusterID,
// bindAddr, ports, and directories are intentionally excluded.
type LedgerDefaultsConfig struct {
	// Pebble storage engine configuration.
	// +optional
	Pebble *PebbleConfig `json:"pebble,omitempty"`

	// Raft consensus configuration.
	// +optional
	Raft *RaftConfig `json:"raft,omitempty"`

	// Health check configuration.
	// +optional
	Health *HealthConfig `json:"health,omitempty"`

	// ColdStorage configuration for period archival.
	// +optional
	ColdStorage *ColdStorageConfig `json:"coldStorage,omitempty"`

	// TLS configuration for gRPC connections.
	// +optional
	TLS *TLSConfig `json:"tls,omitempty"`

	// ResponseSigning configuration (Ed25519).
	// +optional
	ResponseSigning *ResponseSigningConfig `json:"responseSigning,omitempty"`

	// Monitoring configuration (OpenTelemetry).
	// +optional
	Monitoring *MonitoringConfig `json:"monitoring,omitempty"`
}

func init() {
	SchemeBuilder.Register(&LedgerDefaults{}, &LedgerDefaultsList{})
}
