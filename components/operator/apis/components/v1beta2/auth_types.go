/*
Copyright 2022.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package v1beta2

import (
	authv1beta2 "github.com/formancehq/operator/apis/auth.components/v1beta2"
	apisv1beta2 "github.com/formancehq/operator/pkg/apis/v1beta2"
	pkgapisv1beta2 "github.com/formancehq/operator/pkg/apis/v1beta2"
	"github.com/formancehq/operator/pkg/typeutils"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

type DelegatedOIDCServerConfiguration struct {
	// +optional
	Issuer string `json:"issuer,omitempty"`
	// +optional
	IssuerFrom *pkgapisv1beta2.ConfigSource `json:"issuerFrom,omitempty"`
	// +optional
	ClientID string `json:"clientID,omitempty"`
	// +optional
	ClientIDFrom *pkgapisv1beta2.ConfigSource `json:"clientIDFrom,omitempty"`
	// +optional
	ClientSecret string `json:"clientSecret,omitempty"`
	// +optional
	ClientSecretFrom *pkgapisv1beta2.ConfigSource `json:"clientSecretFrom,omitempty"`
}

func (cfg DelegatedOIDCServerConfiguration) Env() []corev1.EnvVar {
	return []corev1.EnvVar{
		pkgapisv1beta2.SelectRequiredConfigValueOrReference("DELEGATED_CLIENT_SECRET", "",
			cfg.ClientSecret, cfg.ClientSecretFrom),
		pkgapisv1beta2.SelectRequiredConfigValueOrReference("DELEGATED_CLIENT_ID", "",
			cfg.ClientID, cfg.ClientIDFrom),
		pkgapisv1beta2.SelectRequiredConfigValueOrReference("DELEGATED_ISSUER", "",
			cfg.Issuer, cfg.IssuerFrom),
	}
}

func (cfg *DelegatedOIDCServerConfiguration) Validate() field.ErrorList {
	if cfg == nil {
		return nil
	}
	return typeutils.MergeAll(
		pkgapisv1beta2.ValidateRequiredConfigValueOrReference("issuer", cfg.Issuer, cfg.IssuerFrom),
		pkgapisv1beta2.ValidateRequiredConfigValueOrReference("clientID", cfg.ClientID, cfg.ClientIDFrom),
		pkgapisv1beta2.ValidateRequiredConfigValueOrReference("clientSecret", cfg.ClientSecret, cfg.ClientSecretFrom),
	)
}

// AuthSpec defines the desired state of Auth
type AuthSpec struct {
	pkgapisv1beta2.CommonServiceProperties `json:",inline"`
	apisv1beta2.Scalable                   `json:",inline"`
	Postgres                               PostgresConfigCreateDatabase `json:"postgres"`
	BaseURL                                string                       `json:"baseURL"`

	// SigningKey is a private key
	// The signing key is used by the server to sign JWT tokens
	// The value of this config will be copied to a secret and injected inside
	// the env vars of the server using secret mapping.
	// If not specified, a key will be automatically generated.
	// +optional
	SigningKey string `json:"signingKey"`

	DelegatedOIDCServer DelegatedOIDCServerConfiguration `json:"delegatedOIDCServer"`

	// +optional
	Monitoring *apisv1beta2.MonitoringSpec `json:"monitoring"`

	// +optional
	StaticClients []authv1beta2.StaticClient `json:"staticClients"`
}

//+kubebuilder:object:root=true
//+kubebuilder:subresource:status
//+kubebuilder:subresource:scale:specpath=.spec.replicas,statuspath=.status.replicas,selectorpath=.status.selector
//+kubebuilder:storageversion

// Auth is the Schema for the auths API
type Auth struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   AuthSpec                      `json:"spec,omitempty"`
	Status apisv1beta2.ReplicationStatus `json:"status,omitempty"`
}

func (a *Auth) GetStatus() apisv1beta2.Dirty {
	return &a.Status
}

func (a *Auth) IsDirty(t apisv1beta2.Object) bool {
	return false
}

func (a *Auth) GetConditions() *apisv1beta2.Conditions {
	return &a.Status.Conditions
}

func (a *Auth) HasStaticClients() bool {
	return a.Spec.StaticClients != nil && len(a.Spec.StaticClients) > 0
}

//+kubebuilder:object:root=true

// AuthList contains a list of Auth
type AuthList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Auth `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Auth{}, &AuthList{})
}
