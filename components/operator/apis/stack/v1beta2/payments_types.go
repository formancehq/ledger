package v1beta2

import (
	"fmt"

	authcomponentsv1beta2 "github.com/formancehq/operator/apis/auth.components/v1beta2"
	componentsv1beta2 "github.com/formancehq/operator/apis/components/v1beta2"
	apisv1beta2 "github.com/formancehq/operator/pkg/apis/v1beta2"
	"github.com/formancehq/operator/pkg/typeutils"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

// +kubebuilder:object:generate=true
type PaymentsSpec struct {
	// +optional
	Scaling ScalingSpec `json:"scaling,omitempty"`
	// +optional
	Ingress *IngressConfig `json:"ingress"`
	// +optional
	Postgres apisv1beta2.PostgresConfig `json:"postgres"`
}

func (in PaymentsSpec) NeedAuthMiddleware() bool {
	return true
}

func (in PaymentsSpec) Spec(stack *Stack, configuration ConfigurationSpec) any {
	return componentsv1beta2.PaymentsSpec{
		Collector: &componentsv1beta2.CollectorConfig{
			KafkaConfig: configuration.Kafka,
			Topic:       fmt.Sprintf("%s-payments", stack.Name),
		},
		Postgres: componentsv1beta2.PostgresConfigCreateDatabase{
			CreateDatabase: true,
			PostgresConfigWithDatabase: apisv1beta2.PostgresConfigWithDatabase{
				PostgresConfig: configuration.Services.Payments.Postgres,
				Database:       fmt.Sprintf("%s-payments", stack.Name),
			},
		},
	}
}

func (in PaymentsSpec) HTTPPort() int {
	return 8080
}

func (in PaymentsSpec) AuthClientConfiguration(stack *Stack) *authcomponentsv1beta2.ClientConfiguration {
	return nil
}

func (in PaymentsSpec) Validate() field.ErrorList {
	return typeutils.MergeAll(
		typeutils.Map(in.Postgres.Validate(), apisv1beta2.AddPrefixToFieldError("postgres.")),
	)
}
