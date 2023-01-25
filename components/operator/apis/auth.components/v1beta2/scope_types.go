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
	"reflect"
	"time"

	apisv1beta2 "github.com/formancehq/operator/pkg/apis/v1beta2"
	"github.com/formancehq/operator/pkg/typeutils"
	"github.com/numary/auth/authclient"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ScopeSpec defines the desired state of Scope
type ScopeSpec struct {
	Label string `json:"label"`
	// +optional
	Transient           []string `json:"transient"`
	AuthServerReference string   `json:"authServerReference"`
}

type TransientScopeStatus struct {
	ObservedGeneration int64  `json:"observedGeneration"`
	AuthServerID       string `json:"authServerID"`
	Date               string `json:"date"`
}

// ScopeStatus defines the observed state of Scope
type ScopeStatus struct {
	apisv1beta2.Status `json:",inline"`
	AuthServerID       string                          `json:"authServerID,omitempty"`
	Transient          map[string]TransientScopeStatus `json:"transient,omitempty"`
}

func (in *ScopeStatus) IsDirty(t apisv1beta2.Object) bool {
	if in.Status.IsDirty(t) {
		return true
	}
	scope := t.(*Scope)
	if in.AuthServerID != scope.Status.AuthServerID {
		return true
	}
	if !reflect.DeepEqual(in.Transient, scope.Status.Transient) {
		return true
	}
	return false
}

//+kubebuilder:object:root=true
//+kubebuilder:subresource:status
//+kubebuilder:printcolumn:name="Server ID",type="string",JSONPath=".status.authServerID",description="Auth server ID"
//+kubebuilder:storageversion

// Scope is the Schema for the scopes API
type Scope struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ScopeSpec   `json:"spec,omitempty"`
	Status ScopeStatus `json:"status,omitempty"`
}

func (s *Scope) GetStatus() apisv1beta2.Dirty {
	return &s.Status
}

func (s *Scope) IsDirty(t apisv1beta2.Object) bool {
	return authServerChanges(t, s, s.Spec.AuthServerReference)
}

func (in *Scope) GetConditions() *apisv1beta2.Conditions {
	return &in.Status.Conditions
}

func (s *Scope) AuthServerReference() string {
	return s.Spec.AuthServerReference
}

func (s *Scope) IsInTransient(authScope *authclient.Scope) bool {
	return typeutils.First(authScope.Transient, typeutils.Equal(s.Status.AuthServerID)) != nil
}

func (s *Scope) IsCreatedOnAuthServer() bool {
	return s.Status.AuthServerID != ""
}

func (s *Scope) ClearAuthServerID() {
	s.Status.AuthServerID = ""
}

func (s *Scope) SetRegisteredTransientScope(transientScope *Scope) {
	if s.Status.Transient == nil {
		s.Status.Transient = map[string]TransientScopeStatus{}
	}
	s.Status.Transient[transientScope.Name] = TransientScopeStatus{
		ObservedGeneration: transientScope.Generation,
		AuthServerID:       transientScope.Status.AuthServerID,
		Date:               time.Now().Format(time.RFC3339),
	}
}

func NewScope(name, label, authReference string, transient ...string) *Scope {
	return &Scope{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Spec: ScopeSpec{
			Label:               label,
			Transient:           transient,
			AuthServerReference: authReference,
		},
	}
}

//+kubebuilder:object:root=true

// ScopeList contains a list of Scope
type ScopeList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Scope `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Scope{}, &ScopeList{})
}
