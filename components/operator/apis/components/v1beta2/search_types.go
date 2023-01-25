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
	"fmt"

	pkgapisv1beta2 "github.com/formancehq/operator/pkg/apis/v1beta2"
	"github.com/formancehq/operator/pkg/typeutils"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

const (
	ConditionTypeBenthosReady = "BenthosReady"
)

type ElasticSearchTLSConfig struct {
	// +optional
	Enabled bool `json:"enabled,omitempty"`
	// +optional
	SkipCertVerify bool `json:"skipCertVerify,omitempty"`
}

type ElasticSearchBasicAuthConfig struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type ElasticSearchConfig struct {
	// +optional
	// +kubebuilder:validation:Enum:={http,https}
	// +kubebuilder:validation:default:=https
	Scheme string `json:"scheme,omitempty"`
	// +optional
	Host string `json:"host,omitempty"`
	// +optional
	HostFrom *pkgapisv1beta2.ConfigSource `json:"hostFrom,omitempty"`
	// +optional
	Port uint16 `json:"port,omitempty"`
	// +optional
	PortFrom *pkgapisv1beta2.ConfigSource `json:"portFrom,omitempty"`
	// +optional
	TLS ElasticSearchTLSConfig `json:"tls"`
	// +optional
	BasicAuth *ElasticSearchBasicAuthConfig `json:"basicAuth"`
	// +optional
	PathPrefix string `json:"pathPrefix"`
	// +optional
	UseZinc bool `json:"useZinc,omitempty"`
}

// Deprecated: use env vars
func (in *ElasticSearchConfig) Endpoint() string {
	return fmt.Sprintf("%s://%s:%d%s", in.Scheme, in.Host, in.Port, in.PathPrefix)
}

func (in *ElasticSearchConfig) Env(prefix string) []corev1.EnvVar {
	env := []corev1.EnvVar{
		pkgapisv1beta2.SelectRequiredConfigValueOrReference("OPEN_SEARCH_HOST", prefix, in.Host, in.HostFrom),
		pkgapisv1beta2.SelectRequiredConfigValueOrReference("OPEN_SEARCH_PORT", prefix, in.Port, in.PortFrom),
		pkgapisv1beta2.EnvWithPrefix(prefix, "OPEN_SEARCH_PATH_PREFIX", in.PathPrefix),
		pkgapisv1beta2.EnvWithPrefix(prefix, "OPEN_SEARCH_SCHEME", in.Scheme),
		pkgapisv1beta2.EnvWithPrefix(prefix, "OPEN_SEARCH_SERVICE", pkgapisv1beta2.ComputeEnvVar(prefix, "%s:%s%s",
			"OPEN_SEARCH_HOST",
			"OPEN_SEARCH_PORT",
			"OPEN_SEARCH_PATH_PREFIX",
		)),
	}
	if in.BasicAuth != nil {
		env = append(env,
			pkgapisv1beta2.EnvWithPrefix(prefix, "OPEN_SEARCH_USERNAME", in.BasicAuth.Username),
			pkgapisv1beta2.EnvWithPrefix(prefix, "OPEN_SEARCH_PASSWORD", in.BasicAuth.Password),
		)
	}

	return env
}

func (in *ElasticSearchConfig) Validate() field.ErrorList {
	return typeutils.MergeAll(
		pkgapisv1beta2.ValidateRequiredConfigValueOrReference("host", in.Host, in.HostFrom),
		pkgapisv1beta2.ValidateRequiredConfigValueOrReference("port", in.Port, in.PortFrom),
	)
}

type Batching struct {
	Count  int    `json:"count"`
	Period string `json:"period"`
}

type SearchPostgresConfigs struct {
	Ledger pkgapisv1beta2.PostgresConfigWithDatabase `json:"ledger"`
}

func (c SearchPostgresConfigs) Env() []corev1.EnvVar {
	return c.Ledger.EnvWithDiscriminator("", "LEDGER")
}

// SearchSpec defines the desired state of Search
type SearchSpec struct {
	pkgapisv1beta2.CommonServiceProperties `json:",inline"`
	pkgapisv1beta2.Scalable                `json:",inline"`

	// +optional
	Monitoring      *pkgapisv1beta2.MonitoringSpec `json:"monitoring"`
	ElasticSearch   ElasticSearchConfig            `json:"elasticsearch"`
	KafkaConfig     pkgapisv1beta2.KafkaConfig     `json:"kafka"`
	Index           string                         `json:"index"`
	Batching        Batching                       `json:"batching"`
	PostgresConfigs SearchPostgresConfigs          `json:"postgres"`
}

//+kubebuilder:object:root=true
//+kubebuilder:subresource:status
//+kubebuilder:subresource:scale:specpath=.spec.replicas,statuspath=.status.replicas,selectorpath=.status.selector
//+kubebuilder:storageversion

// Search is the Schema for the searches API
type Search struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   SearchSpec                       `json:"spec,omitempty"`
	Status pkgapisv1beta2.ReplicationStatus `json:"status,omitempty"`
}

func (in *Search) GetStatus() pkgapisv1beta2.Dirty {
	return &in.Status
}

func (in *Search) IsDirty(t pkgapisv1beta2.Object) bool {
	return false
}

func (in *Search) GetConditions() *pkgapisv1beta2.Conditions {
	return &in.Status.Conditions
}

//+kubebuilder:object:root=true

// SearchList contains a list of Search
type SearchList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Search `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Search{}, &SearchList{})
}
