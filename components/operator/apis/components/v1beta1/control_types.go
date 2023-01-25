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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type AuthClientConfiguration struct {
	ClientID     string `json:"clientID"`
	ClientSecret string `json:"clientSecret"`
}

// ControlSpec defines the desired state of Control
type ControlSpec struct {
	Scalable    `json:",inline"`
	ImageHolder `json:",inline"`
	// +optional
	Ingress *IngressSpec `json:"ingress"`
	// +optional
	Monitoring *MonitoringSpec `json:"monitoring"`
	// +optional
	Debug       bool   `json:"debug"`
	ApiURLFront string `json:"apiURLFront"`
	ApiURLBack  string `json:"apiURLBack"`

	// +optional
	AuthClientConfiguration *AuthClientConfiguration `json:"auth"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:subresource:scale:specpath=.spec.replicas,statuspath=.status.replicas,selectorpath=.status.selector

// Control is the Schema for the controls API
type Control struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ControlSpec       `json:"spec,omitempty"`
	Status ReplicationStatus `json:"status,omitempty"`
}

func (in *Control) GetStatus() Dirty {
	return &in.Status
}

func (in *Control) IsDirty(t Object) bool {
	return false
}

func (in *Control) GetConditions() *Conditions {
	return &in.Status.Conditions
}

func (in *Control) GetImage() string {
	return in.Spec.GetImage("control")
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:scale:specpath=.spec.replicas,statuspath=.status.replicas,selectorpath=.status.selector

// ControlList contains a list of Control
type ControlList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Control `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Control{}, &ControlList{})
}
