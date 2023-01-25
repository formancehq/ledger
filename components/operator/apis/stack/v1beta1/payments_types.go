package v1beta1

import (
	. "github.com/formancehq/operator/pkg/apis/v1beta2"
)

type MongoDBConfig struct{}

// +kubebuilder:object:generate=true
type PaymentsSpec struct {
	ImageHolder `json:",inline"`
	// +optional
	Scaling ScalingSpec `json:"scaling,omitempty"`
	// +optional
	Ingress *IngressConfig `json:"ingress"`
	// +optional
	MongoDB MongoDBConfig `json:"mongoDB"`
}
