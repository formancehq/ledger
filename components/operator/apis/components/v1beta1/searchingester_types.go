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
	"encoding/json"

	. "github.com/formancehq/operator/pkg/apis/v1beta2"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// SearchIngesterSpec defines the desired state of SearchIngester
type SearchIngesterSpec struct {
	Reference string `json:"reference"`
	//+kubebuilder:pruning:PreserveUnknownFields
	//+kubebuilder:validation:Type=object
	//+kubebuilder:validation:Schemaless
	Pipeline json.RawMessage `json:"pipeline"` // Should be a map[string]any but controller-gen does not support it
	// +optional
	Debug bool `json:"debug,omitempty"`
}

//+kubebuilder:object:root=true
//+kubebuilder:subresource:status

// SearchIngester is the Schema for the searchingesters API
type SearchIngester struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   SearchIngesterSpec `json:"spec,omitempty"`
	Status Status             `json:"status,omitempty"`
}

func (in *SearchIngester) GetStatus() Dirty {
	return &in.Status
}

func (in *SearchIngester) IsDirty(t Object) bool {
	return false
}

func (in *SearchIngester) GetConditions() *Conditions {
	return &in.Status.Conditions
}

//+kubebuilder:object:root=true

// SearchIngesterList contains a list of SearchIngester
type SearchIngesterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []SearchIngester `json:"items"`
}

func init() {
	SchemeBuilder.Register(&SearchIngester{}, &SearchIngesterList{})
}
