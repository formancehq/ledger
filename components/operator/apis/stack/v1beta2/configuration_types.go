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
	"strings"

	authcomponentsv1beta2 "github.com/formancehq/operator/apis/auth.components/v1beta2"
	"github.com/formancehq/operator/apis/components/v1beta2"
	apisv1beta2 "github.com/formancehq/operator/pkg/apis/v1beta2"
	"github.com/formancehq/operator/pkg/typeutils"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

// +kubebuilder:object:generate=false
type ServiceConfiguration interface {
	NeedAuthMiddleware() bool
	Spec(stack *Stack, configuration ConfigurationSpec) any
	HTTPPort() int
	AuthClientConfiguration(stack *Stack) *authcomponentsv1beta2.ClientConfiguration
}

// +kubebuilder:object:generate=false
type CustomPathServiceConfiguration interface {
	ServiceConfiguration
	Path() string
}

type ConfigurationServicesSpec struct {
	Auth           AuthSpec           `json:"auth,omitempty"`
	Control        ControlSpec        `json:"control,omitempty"`
	Ledger         LedgerSpec         `json:"ledger,omitempty"`
	Payments       PaymentsSpec       `json:"payments,omitempty"`
	Search         SearchSpec         `json:"search,omitempty"`
	Webhooks       WebhooksSpec       `json:"webhooks,omitempty"`
	Wallets        WalletsSpec        `json:"wallets,omitempty"`
	Orchestration  OrchestrationSpec  `json:"orchestration,omitempty"`
	Counterparties CounterpartiesSpec `json:"counterparties,omitempty"`
}

var (
	_ ServiceConfiguration = AuthSpec{}
	_ ServiceConfiguration = ControlSpec{}
	_ ServiceConfiguration = LedgerSpec{}
	_ ServiceConfiguration = PaymentsSpec{}
	_ ServiceConfiguration = SearchSpec{}
	_ ServiceConfiguration = WebhooksSpec{}
	_ ServiceConfiguration = WalletsSpec{}
	_ ServiceConfiguration = OrchestrationSpec{}
	_ ServiceConfiguration = CounterpartiesSpec{}
)

func (in *ConfigurationServicesSpec) AsServiceConfigurations() map[string]ServiceConfiguration {
	valueOf := reflect.ValueOf(*in)
	ret := make(map[string]ServiceConfiguration)
	for i := 0; i < valueOf.Type().NumField(); i++ {
		ret[strings.ToLower(valueOf.Type().Field(i).Name)] = valueOf.Field(i).Interface().(ServiceConfiguration)
	}
	return ret
}

func GetServiceList() []string {
	typeOf := reflect.TypeOf(ConfigurationServicesSpec{})
	res := make([]string, 0)
	for i := 0; i < typeOf.NumField(); i++ {
		field := typeOf.Field(i)
		res = append(res, field.Name)
	}
	return res
}

type ConfigurationSpec struct {
	Services ConfigurationServicesSpec `json:"services"`
	Kafka    apisv1beta2.KafkaConfig   `json:"kafka"`
	// +optional
	Monitoring *apisv1beta2.MonitoringSpec `json:"monitoring,omitempty"`
	// +optional
	Ingress  IngressGlobalConfig    `json:"ingress,omitempty"`
	Temporal v1beta2.TemporalConfig `json:"temporal"`
}

func (in *ConfigurationSpec) Validate() field.ErrorList {
	return typeutils.MergeAll(
		typeutils.Map(in.Services.Ledger.Validate(), apisv1beta2.AddPrefixToFieldError("services.ledger")),
		typeutils.Map(in.Services.Payments.Validate(), apisv1beta2.AddPrefixToFieldError("services.payments")),
		typeutils.Map(in.Services.Search.Validate(), apisv1beta2.AddPrefixToFieldError("services.search")),
		typeutils.Map(in.Services.Webhooks.Validate(), apisv1beta2.AddPrefixToFieldError("services.webhooks")),
		typeutils.Map(in.Services.Wallets.Validate(), apisv1beta2.AddPrefixToFieldError("services.wallets")),
		typeutils.Map(in.Services.Counterparties.Validate(), apisv1beta2.AddPrefixToFieldError("services.counterparties")),
		typeutils.Map(in.Services.Auth.Validate(), apisv1beta2.AddPrefixToFieldError("services.auth")),
		typeutils.Map(in.Monitoring.Validate(), apisv1beta2.AddPrefixToFieldError("monitoring")),
		typeutils.Map(in.Kafka.Validate(), apisv1beta2.AddPrefixToFieldError("kafka")),
	)
}

func (in *ConfigurationSpec) GetServices() map[string]ServiceConfiguration {
	return in.Services.AsServiceConfigurations()
}

//+kubebuilder:object:root=true
//+kubebuilder:resource:scope=Cluster
//+kubebuilder:subresource:status
//+kubebuilder:storageversion

// Configuration is the Schema for the configurations API
type Configuration struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ConfigurationSpec  `json:"spec,omitempty"`
	Status apisv1beta2.Status `json:"status,omitempty"`
}

func (*Configuration) Hub() {}

//+kubebuilder:object:root=true

// ConfigurationList contains a list of Configuration
type ConfigurationList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Configuration `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Configuration{}, &ConfigurationList{})
}
