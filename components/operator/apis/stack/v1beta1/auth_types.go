package v1beta1

import (
	authv1beta2 "github.com/formancehq/operator/apis/auth.components/v1beta2"
	"github.com/formancehq/operator/apis/components/v1beta1"
	. "github.com/formancehq/operator/pkg/apis/v1beta2"
)

type AuthSpec struct {
	ImageHolder `json:",inline"`
	// +optional
	Postgres PostgresConfig `json:"postgres"`
	// +optional
	SigningKey string `json:"signingKey"`
	// +optional
	DelegatedOIDCServer *v1beta1.DelegatedOIDCServerConfiguration `json:"delegatedOIDCServer"`
	// +optional
	Ingress *IngressConfig `json:"ingress"`
	// +optional
	Host string `json:"host,omitempty"`
	// +optional
	Scheme string `json:"scheme,omitempty"`
	// +optional
	StaticClients []authv1beta2.StaticClient `json:"staticClients"`
}

func (in *AuthSpec) GetScheme() string {
	if in.Scheme != "" {
		return in.Scheme
	}
	return "https"
}
