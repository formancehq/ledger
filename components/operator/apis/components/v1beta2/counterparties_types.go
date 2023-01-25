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

// CounterpartiesSpec defines the desired state of Counterparties
type CounterpartiesSpec struct {
	pkgapisv1beta2.CommonServiceProperties `json:",inline"`

	// +optional
	Enabled bool `json:"enabled,omitempty"`
	// +optional
	Postgres PostgresConfigCreateDatabase `json:"postgres"`
	// +optional
	Monitoring *pkgapisv1beta2.MonitoringSpec `json:"monitoring"`
}

//+kubebuilder:object:root=true
//+kubebuilder:subresource:status
//+kubebuilder:storageversion

// Counterparties is the Schema for the Counterparties API
type Counterparties struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   CounterpartiesSpec    `json:"spec,omitempty"`
	Status pkgapisv1beta2.Status `json:"status,omitempty"`
}

func (in *Counterparties) GetStatus() pkgapisv1beta2.Dirty {
	return &in.Status
}

func (in *Counterparties) GetConditions() *pkgapisv1beta2.Conditions {
	return &in.Status.Conditions
}

func (in *Counterparties) IsDirty(t pkgapisv1beta2.Object) bool {
	return false
}

//+kubebuilder:object:root=true

// CounterpartiesList contains a list of Counterparties
type CounterpartiesList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Counterparties `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Counterparties{}, &CounterpartiesList{})
}
