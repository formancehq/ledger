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
type CounterpartiesSpec struct {
	// +optional
	Enabled bool `json:"enabled,omitempty"`
	// +optional
	Debug bool `json:"debug,omitempty"`
	// +optional
	Scaling ScalingSpec `json:"scaling,omitempty"`
	// +optional
	Postgres apisv1beta2.PostgresConfig `json:"postgres"`
}

func (in CounterpartiesSpec) NeedAuthMiddleware() bool {
	return true
}

func (in CounterpartiesSpec) Spec(stack *Stack, configuration ConfigurationSpec) any {
	return componentsv1beta2.CounterpartiesSpec{
		Enabled: configuration.Services.Counterparties.Enabled,
		Postgres: componentsv1beta2.PostgresConfigCreateDatabase{
			CreateDatabase: true,
			PostgresConfigWithDatabase: apisv1beta2.PostgresConfigWithDatabase{
				PostgresConfig: configuration.Services.Counterparties.Postgres,
				Database:       fmt.Sprintf("%s-counterparties", stack.Name),
			},
		},
	}
}

func (in CounterpartiesSpec) HTTPPort() int {
	return 8080
}

func (in CounterpartiesSpec) AuthClientConfiguration(stack *Stack) *authcomponentsv1beta2.ClientConfiguration {
	return nil
}

func (in CounterpartiesSpec) Validate() field.ErrorList {
	return typeutils.MergeAll(
		typeutils.Map(in.Postgres.Validate(), apisv1beta2.AddPrefixToFieldError("postgres.")),
	)
}
