package v1beta2

import (
	"fmt"

	authcomponentsv1beta2 "github.com/formancehq/operator/apis/auth.components/v1beta2"
	componentsv1beta2 "github.com/formancehq/operator/apis/components/v1beta2"
	apisv1beta2 "github.com/formancehq/operator/pkg/apis/v1beta2"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

// +kubebuilder:object:generate=true
type OrchestrationSpec struct {
	apisv1beta2.DevProperties `json:",inline"`
	// +optional
	Scaling ScalingSpec `json:"scaling,omitempty"`
	// +optional
	Ingress *IngressConfig `json:"ingress"`
	// +optional
	Postgres apisv1beta2.PostgresConfig `json:"postgres"`
}

func (in OrchestrationSpec) NeedAuthMiddleware() bool {
	return true
}

func (in OrchestrationSpec) Spec(stack *Stack, configuration ConfigurationSpec) any {
	return componentsv1beta2.OrchestrationSpec{
		StackURL: stack.URL(),
		Temporal: configuration.Temporal,
		Postgres: componentsv1beta2.PostgresConfigCreateDatabase{
			CreateDatabase: true,
			PostgresConfigWithDatabase: apisv1beta2.PostgresConfigWithDatabase{
				PostgresConfig: configuration.Services.Orchestration.Postgres,
				Database:       fmt.Sprintf("%s-orchestration", stack.Name),
			},
		},
	}
}

func (in OrchestrationSpec) HTTPPort() int {
	return 8080
}

func (in OrchestrationSpec) AuthClientConfiguration(stack *Stack) *authcomponentsv1beta2.ClientConfiguration {
	ret := authcomponentsv1beta2.NewClientConfiguration()
	return &ret
}

func (in OrchestrationSpec) Validate() field.ErrorList {
	return field.ErrorList{}
}
