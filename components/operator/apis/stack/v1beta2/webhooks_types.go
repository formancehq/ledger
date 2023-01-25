package v1beta2

import (
	"fmt"

	authcomponentsv1beta2 "github.com/formancehq/operator/apis/auth.components/v1beta2"
	componentsv1beta2 "github.com/formancehq/operator/apis/components/v1beta2"
	"github.com/formancehq/operator/pkg/apis/v1beta2"
	"github.com/formancehq/operator/pkg/typeutils"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

// +kubebuilder:object:generate=true
type WebhooksSpec struct {
	// +optional
	Debug bool `json:"debug,omitempty"`
	// +optional
	Scaling ScalingSpec `json:"scaling,omitempty"`
	// +optional
	Ingress *IngressConfig `json:"ingress"`
	// +optional
	Postgres v1beta2.PostgresConfig `json:"postgres"`
}

func (in WebhooksSpec) NeedAuthMiddleware() bool {
	return true
}

func (in WebhooksSpec) Spec(stack *Stack, configuration ConfigurationSpec) any {
	return componentsv1beta2.WebhooksSpec{
		Collector: &componentsv1beta2.CollectorConfig{
			KafkaConfig: configuration.Kafka,
			Topic:       fmt.Sprintf("%s-payments %s-ledger", stack.Name, stack.Name),
		},
		Postgres: componentsv1beta2.PostgresConfigCreateDatabase{
			CreateDatabase: true,
			PostgresConfigWithDatabase: v1beta2.PostgresConfigWithDatabase{
				PostgresConfig: configuration.Services.Webhooks.Postgres,
				Database:       fmt.Sprintf("%s-webhooks", stack.Name),
			},
		},
	}
}

func (in WebhooksSpec) HTTPPort() int {
	return 8080
}

func (in WebhooksSpec) AuthClientConfiguration(stack *Stack) *authcomponentsv1beta2.ClientConfiguration {
	return nil
}

func (in WebhooksSpec) Validate() field.ErrorList {
	return typeutils.MergeAll(
		typeutils.Map(in.Postgres.Validate(), v1beta2.AddPrefixToFieldError("postgres.")),
	)
}
