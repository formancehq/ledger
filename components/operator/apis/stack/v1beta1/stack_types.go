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
	authcomponentsv1beta1 "github.com/formancehq/operator/apis/auth.components/v1beta1"
	. "github.com/formancehq/operator/pkg/apis/v1beta2"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type IngressGlobalConfig struct {
	// +optional
	TLS *IngressTLS `json:"tls"`
	// +optional
	Enabled bool `json:"enabled,omitempty"`
	// +optional
	Annotations map[string]string `json:"annotations,omitempty"`
}

// StackSpec defines the desired state of Stack
type StackSpec struct {
	// +optional
	Seed string `json:"seed"`
	// +optional
	ConfigurationSpec `json:",inline"`

	// +optional
	Debug bool `json:"debug"`
	// +required
	Namespace string `json:"namespace,omitempty"`
	// +optional
	// +required
	Host string `json:"host"`
	// +optional
	Scheme string `json:"scheme"`
}

type ServicesSpec struct {
	// +optional
	Control *ControlSpec `json:"control,omitempty"`
	// +optional
	Ledger *LedgerSpec `json:"ledger,omitempty"`
	// +optional
	Payments *PaymentsSpec `json:"payments,omitempty"`
	// +optional
	Search *SearchSpec `json:"search,omitempty"`
	// +optional
	Webhooks *WebhooksSpec `json:"webhooks,omitempty"`
}

const (
	ConditionTypeStackNamespaceReady  = "NamespaceReady"
	ConditionTypeStackAuthReady       = "AuthReady"
	ConditionTypeStackLedgerReady     = "LedgerReady"
	ConditionTypeStackSearchReady     = "SearchReady"
	ConditionTypeStackControlReady    = "ControlReady"
	ConditionTypeStackPaymentsReady   = "PaymentsReady"
	ConditionTypeStackWebhooksReady   = "WebhooksReady"
	ConditionTypeStackMiddlewareReady = "MiddlewareReady"
)

type ControlAuthentication struct {
	ClientID string
}

type StackStatus struct {
	Status `json:",inline"`

	// +optional
	StaticAuthClients map[string]authcomponentsv1beta1.StaticClient `json:"staticAuthClients,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Cluster
// +kubebuilder:printcolumn:name="Status",type=string,JSONPath=`.status.progress`
// +kubebuilder:printcolumn:name="Version",type="string",JSONPath=".spec.version",description="Stack Version"
// +kubebuilder:printcolumn:name="Namespace",type="string",JSONPath=".spec.namespace",description="Stack Namespace"

// Stack is the Schema for the stacks API
type Stack struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   StackSpec   `json:"spec,omitempty"`
	Status StackStatus `json:"status,omitempty"`
}

//+kubebuilder:object:root=true

// StackList contains a list of Stack
type StackList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Stack `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Stack{}, &StackList{})
}
