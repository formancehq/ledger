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
type SearchSpec struct {
	ElasticSearchConfig componentsv1beta2.ElasticSearchConfig `json:"elasticSearch"`

	// +optional
	Scaling ScalingSpec `json:"scaling,omitempty"`

	//+optional
	Ingress *IngressConfig `json:"ingress"`

	// +optional
	Batching componentsv1beta2.Batching `json:"batching"`
}

func (in SearchSpec) NeedAuthMiddleware() bool {
	return true
}

func (in SearchSpec) Spec(stack *Stack, configuration ConfigurationSpec) any {
	return componentsv1beta2.SearchSpec{
		ElasticSearch: configuration.Services.Search.ElasticSearchConfig,
		KafkaConfig:   configuration.Kafka,
		Index:         stack.Name,
		Batching:      configuration.Services.Search.Batching,
		PostgresConfigs: componentsv1beta2.SearchPostgresConfigs{
			Ledger: v1beta2.PostgresConfigWithDatabase{
				PostgresConfig: configuration.Services.Ledger.Postgres,
				Database:       fmt.Sprintf("%s-ledger", stack.Name),
			},
		},
	}
}

func (in SearchSpec) HTTPPort() int {
	return 8080
}

func (in SearchSpec) AuthClientConfiguration(stack *Stack) *authcomponentsv1beta2.ClientConfiguration {
	return nil
}

func (in SearchSpec) Validate() field.ErrorList {
	return typeutils.Map(in.ElasticSearchConfig.Validate(), v1beta2.AddPrefixToFieldError("elasticSearch"))
}
