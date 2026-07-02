package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:deprecatedversion:warning="ledger.formance.com/LedgerService is deprecated: use ledger.formance.com/Cluster instead. The operator migrates existing LedgerService resources to Cluster on startup."
// +kubebuilder:printcolumn:name="MigratedTo",type=string,JSONPath=`.metadata.annotations.ledger\.formance\.com/migrated-to`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// LedgerService is the deprecated Kind previously used to manage a Raft
// cluster of ledger nodes. It has been renamed to Cluster. Both Spec and
// Status reuse the Cluster types verbatim, so existing manifests keep
// working without change; the operator's LedgerServiceMigrator copies each
// LedgerService into a Cluster of the same name at boot and annotates the
// original with ledger.formance.com/migrated-to. Remove LedgerService
// manifests once migrations have converged.
type LedgerService struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ClusterSpec   `json:"spec,omitempty"`
	Status ClusterStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// LedgerServiceList contains a list of LedgerService.
type LedgerServiceList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`

	Items []LedgerService `json:"items"`
}

func init() {
	SchemeBuilder.Register(&LedgerService{}, &LedgerServiceList{})
}
