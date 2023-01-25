package v1beta1

import (
	authcomponentsv1beta2 "github.com/formancehq/operator/apis/components/v1beta2"
	. "github.com/formancehq/operator/pkg/apis/v1beta2"
)

// +kubebuilder:object:generate=true
type LedgerSpec struct {
	ImageHolder `json:",inline"`
	Scalable    `json:",inline"`
	// +optional
	Postgres PostgresConfig `json:"postgres"`
	// +optional
	LockingStrategy authcomponentsv1beta2.LockingStrategy `json:"locking"`
	// +optional
	Ingress *IngressConfig `json:"ingress"`
}
