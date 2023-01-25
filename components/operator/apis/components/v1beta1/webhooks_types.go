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

// WebhooksSpec defines the desired state of Webhooks
type WebhooksSpec struct {
	ImageHolder `json:",inline"`
	// +optional
	Ingress *IngressSpec `json:"ingress"`
	// +optional
	Debug bool `json:"debug"`
	// +optional
	Auth *AuthConfigSpec `json:"auth"`
	// +optional
	Monitoring *MonitoringSpec `json:"monitoring"`
	// +optional
	Collector *CollectorConfig `json:"collector"`
	// +optional
	MongoDB MongoDBConfig `json:"mongoDB"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status

// Webhooks is the Schema for the Webhooks API
type Webhooks struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   WebhooksSpec `json:"spec,omitempty"`
	Status Status       `json:"status,omitempty"`
}

func (in *Webhooks) GetStatus() Dirty {
	return &in.Status
}

func (in *Webhooks) GetConditions() *Conditions {
	return &in.Status.Conditions
}

func (in *Webhooks) IsDirty(t Object) bool {
	return false
}

func (in *Webhooks) GetImage() string {
	return in.Spec.GetImage("webhooks")
}

//+kubebuilder:object:root=true

// WebhooksList contains a list of Webhooks
type WebhooksList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Webhooks `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Webhooks{}, &WebhooksList{})
}
