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
	. "github.com/formancehq/operator/pkg/apis/v1beta2"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ServerSpec defines the desired state of Server
type ServerSpec struct {
	ImageHolder `json:",inline"`
	// +optional
	InitContainers []corev1.Container `json:"containers,omitempty"`
	// +optional
	ResourcesConfigMap string `json:"resourcesConfigMap"`
	// +optional
	TemplatesConfigMap string `json:"templatesConfigMap"`
	// +optional
	Env []corev1.EnvVar `json:"env"`
	// +optional
	LogLevel string `json:"logLevel"`
}

// ServerStatus defines the observed state of Server
type ServerStatus struct {
	Status `json:",inline"`
	PodIP  string `json:"podIP,omitempty"`
}

func (in *ServerStatus) IsDirty(t Object) bool {
	if in.Status.IsDirty(t) {
		return true
	}
	server := t.(*Server)
	return in.PodIP != server.Status.PodIP
}

//+kubebuilder:object:root=true
//+kubebuilder:subresource:status

// Server is the Schema for the servers API
type Server struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ServerSpec   `json:"spec,omitempty"`
	Status ServerStatus `json:"status,omitempty"`
}

func (in *Server) GetStatus() Dirty {
	return &in.Status
}

func (in *Server) IsDirty(t Object) bool {
	return false
}

func (in *Server) GetConditions() *Conditions {
	return &in.Status.Conditions
}

//+kubebuilder:object:root=true

// ServerList contains a list of Server
type ServerList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Server `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Server{}, &ServerList{})
}
