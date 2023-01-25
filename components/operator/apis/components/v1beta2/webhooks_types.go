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
	pkgapisv1beta2 "github.com/formancehq/operator/pkg/apis/v1beta2"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// WebhooksSpec defines the desired state of Webhooks
type WebhooksSpec struct {
	pkgapisv1beta2.CommonServiceProperties `json:",inline"`

	Collector *CollectorConfig `json:"collector"`
	// +optional
	Postgres PostgresConfigCreateDatabase `json:"postgres"`
	// +optional
	Monitoring *pkgapisv1beta2.MonitoringSpec `json:"monitoring"`
}

//+kubebuilder:object:root=true
//+kubebuilder:subresource:status
//+kubebuilder:storageversion

// Webhooks is the Schema for the Webhooks API
type Webhooks struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   WebhooksSpec          `json:"spec,omitempty"`
	Status pkgapisv1beta2.Status `json:"status,omitempty"`
}

func (in *Webhooks) GetStatus() pkgapisv1beta2.Dirty {
	return &in.Status
}

func (in *Webhooks) GetConditions() *pkgapisv1beta2.Conditions {
	return &in.Status.Conditions
}

func (in *Webhooks) IsDirty(t pkgapisv1beta2.Object) bool {
	return false
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
