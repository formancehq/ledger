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
	"encoding/json"

	apisv1beta2 "github.com/formancehq/operator/pkg/apis/v1beta2"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// StreamSpec defines the desired state of Stream
type StreamSpec struct {
	// TODO: Add validations pattern
	Reference string `json:"ref"`
	//+kubebuilder:pruning:PreserveUnknownFields
	//+kubebuilder:validation:Type=object
	//+kubebuilder:validation:Schemaless
	Config json.RawMessage `json:"config"`
}

//+kubebuilder:object:root=true
//+kubebuilder:subresource:status
//+kubebuilder:storageversion

// Stream is the Schema for the streams API
type Stream struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   StreamSpec         `json:"spec,omitempty"`
	Status apisv1beta2.Status `json:"status,omitempty"`
}

func (in *Stream) GetStatus() apisv1beta2.Dirty {
	return &in.Status
}

func (in *Stream) IsDirty(t apisv1beta2.Object) bool {
	return false
}

func (in *Stream) GetConditions() *apisv1beta2.Conditions {
	return &in.Status.Conditions
}

//+kubebuilder:object:root=true

// StreamList contains a list of Stream
type StreamList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Stream `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Stream{}, &StreamList{})
}
