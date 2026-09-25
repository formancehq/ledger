package v1alpha1

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

// EventSinkSpec declares one NATS sink in the referenced Ledger cluster.
type EventSinkSpec struct {
	// ClusterRef names a Cluster in the same namespace.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="clusterRef is immutable"
	ClusterRef EventSinkClusterRef `json:"clusterRef"`

	// NATS configures the JetStream destination.
	// +kubebuilder:validation:Required
	NATS EventSinkNATSSpec `json:"nats"`

	// Format is the event serialization format.
	// +kubebuilder:default=json
	// +kubebuilder:validation:Enum=json;protobuf
	// +optional
	Format string `json:"format,omitempty"`

	// BatchSize is the maximum number of events per batch. Zero uses Ledger's default.
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=100000
	// +optional
	BatchSize *int32 `json:"batchSize,omitempty"`

	// BatchDelayMs is the maximum delay before a partial batch is published.
	// +kubebuilder:validation:Minimum=0
	// +optional
	BatchDelayMs *int64 `json:"batchDelayMs,omitempty"`

	// EventTypes filters events. Empty selects all event types.
	// +optional
	// +listType=set
	// +kubebuilder:validation:items:Enum=COMMITTED_TRANSACTION;REVERTED_TRANSACTION;SAVED_METADATA;DELETED_METADATA;CREATED_LEDGER;DELETED_LEDGER;SKIPPED_ORDER
	EventTypes []string `json:"eventTypes,omitempty"`
}

type EventSinkClusterRef struct {
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`
}

type EventSinkNATSSpec struct {
	// URL is the NATS URL. Credentials must be referenced from a Secret in a later API revision.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:Pattern=`^nats://[^@]+$`
	URL string `json:"url"`

	// Topic is the subject prefix.
	// +kubebuilder:validation:MinLength=1
	Topic string `json:"topic"`
}

type EventSinkStatus struct {
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
	// Cursor is the last published event sequence.
	// +optional
	Cursor uint64 `json:"cursor,omitempty"`
	// Error is Ledger's current delivery error, if any.
	// +optional
	Error string `json:"error,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=esink
// +kubebuilder:printcolumn:name="Cluster",type=string,JSONPath=`.spec.clusterRef.name`
// +kubebuilder:printcolumn:name="Synced",type=string,JSONPath=`.status.conditions[?(@.type=="Synced")].status`

// EventSink owns one Ledger runtime sink by its Kubernetes UID.
type EventSink struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   EventSinkSpec   `json:"spec,omitempty"`
	Status EventSinkStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

type EventSinkList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`

	Items []EventSink `json:"items"`
}

func init() {
	SchemeBuilder.Register(&EventSink{}, &EventSinkList{})
}
