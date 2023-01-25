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

package v1beta1

import (
	"fmt"

	. "github.com/formancehq/operator/pkg/apis/v1beta2"
	. "github.com/formancehq/operator/pkg/typeutils"
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
	HostFrom *ConfigSource `json:"hostFrom,omitempty"`
	// +optional
	Port uint16 `json:"port,omitempty"`
	// +optional
	PortFrom *ConfigSource `json:"portFrom,omitempty"`
	// +optional
	TLS ElasticSearchTLSConfig `json:"tls"`
	// +optional
	BasicAuth *ElasticSearchBasicAuthConfig `json:"basicAuth"`
	// +optional
	PathPrefix string `json:"pathPrefix"`
}

// Deprecated: use env vars
func (in *ElasticSearchConfig) Endpoint() string {
	return fmt.Sprintf("%s://%s:%d%s", in.Scheme, in.Host, in.Port, in.PathPrefix)
}

func (in *ElasticSearchConfig) Env(prefix string) []corev1.EnvVar {
	env := []corev1.EnvVar{
		SelectRequiredConfigValueOrReference("OPEN_SEARCH_HOST", prefix, in.Host, in.HostFrom),
		SelectRequiredConfigValueOrReference("OPEN_SEARCH_PORT", prefix, in.Port, in.PortFrom),
		EnvWithPrefix(prefix, "OPEN_SEARCH_PATH_PREFIX", in.PathPrefix),
		EnvWithPrefix(prefix, "OPEN_SEARCH_SCHEME", in.Scheme),
		EnvWithPrefix(prefix, "OPEN_SEARCH_SERVICE", ComputeEnvVar(prefix, "%s:%s%s",
			"OPEN_SEARCH_HOST",
			"OPEN_SEARCH_PORT",
			"OPEN_SEARCH_PATH_PREFIX",
		)),
	}
	if in.BasicAuth != nil {
		env = append(env,
			EnvWithPrefix(prefix, "OPEN_SEARCH_USERNAME", in.BasicAuth.Username),
			EnvWithPrefix(prefix, "OPEN_SEARCH_PASSWORD", in.BasicAuth.Password),
		)
	}

	return env
}

func (in *ElasticSearchConfig) Validate() field.ErrorList {
	return MergeAll(
		ValidateRequiredConfigValueOrReference("host", in.Host, in.HostFrom),
		ValidateRequiredConfigValueOrReference("port", in.Port, in.PortFrom),
	)
}

type Batching struct {
	Count  int    `json:"count"`
	Period string `json:"period"`
}

// SearchSpec defines the desired state of Search
type SearchSpec struct {
	Scalable    `json:",inline"`
	ImageHolder `json:",inline"`
	// +optional
	Ingress *IngressSpec `json:"ingress"`
	// +optional
	Debug bool `json:"debug"`
	// +optional
	Auth *AuthConfigSpec `json:"auth"`
	// +optional
	Monitoring    *MonitoringSpec     `json:"monitoring"`
	ElasticSearch ElasticSearchConfig `json:"elasticsearch"`
	KafkaConfig   KafkaConfig         `json:"kafka"`
	Index         string              `json:"index"`
	Batching      Batching            `json:"batching"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:subresource:scale:specpath=.spec.replicas,statuspath=.status.replicas,selectorpath=.status.selector

// Search is the Schema for the searches API
type Search struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   SearchSpec        `json:"spec,omitempty"`
	Status ReplicationStatus `json:"status,omitempty"`
}

func (in *Search) GetStatus() Dirty {
	return &in.Status
}

func (in *Search) IsDirty(t Object) bool {
	return false
}

func (in *Search) GetConditions() *Conditions {
	return &in.Status.Conditions
}

func (in *Search) GetImage() string {
	return in.Spec.GetImage("search")
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
