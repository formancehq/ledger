package v1beta1

import (
	. "github.com/formancehq/operator/pkg/apis/v1beta2"
)

// +kubebuilder:object:generate=true
type WebhooksSpec struct {
	ImageHolder `json:",inline"`
	// +optional
	Debug bool `json:"debug,omitempty"`
	// +optional
	Scaling ScalingSpec `json:"scaling,omitempty"`
	// +optional
	Ingress *IngressConfig `json:"ingress"`
	// +optional
	MongoDB MongoDBConfig `json:"mongoDB"`
}
