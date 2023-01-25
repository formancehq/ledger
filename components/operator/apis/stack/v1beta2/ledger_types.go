package v1beta2

import (
	"fmt"

	beta2 "github.com/formancehq/operator/apis/auth.components/v1beta2"
	componentsv1beta2 "github.com/formancehq/operator/apis/components/v1beta2"
	"github.com/formancehq/operator/pkg/apis/v1beta2"
	apisv1beta2 "github.com/formancehq/operator/pkg/apis/v1beta2"
	"github.com/formancehq/operator/pkg/typeutils"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

// +kubebuilder:object:generate=true
type LedgerSpec struct {
	apisv1beta2.Scalable `json:",inline"`
	Postgres             apisv1beta2.PostgresConfig `json:"postgres"`
	// +optional
	LockingStrategy componentsv1beta2.LockingStrategy `json:"locking"`
	// +optional
	Ingress *IngressConfig `json:"ingress"`
}

func (in LedgerSpec) NeedAuthMiddleware() bool {
	return true
}

func (in LedgerSpec) Spec(stack *Stack, configuration ConfigurationSpec) any {
	return componentsv1beta2.LedgerSpec{
		LockingStrategy: configuration.Services.Ledger.LockingStrategy,
		Postgres: componentsv1beta2.PostgresConfigCreateDatabase{
			PostgresConfigWithDatabase: apisv1beta2.PostgresConfigWithDatabase{
				Database:       fmt.Sprintf("%s-ledger", stack.Name),
				PostgresConfig: configuration.Services.Ledger.Postgres,
			},
			CreateDatabase: true,
		},
		Collector: &componentsv1beta2.CollectorConfig{
			KafkaConfig: configuration.Kafka,
			Topic:       fmt.Sprintf("%s-ledger", stack.Name),
		},
	}
}

func (in LedgerSpec) HTTPPort() int {
	return 8080
}

func (in LedgerSpec) AuthClientConfiguration(stack *Stack) *beta2.ClientConfiguration {
	return nil
}

func (in LedgerSpec) Validate() field.ErrorList {
	ret := typeutils.Map(in.Postgres.Validate(), v1beta2.AddPrefixToFieldError("postgres"))
	ret = append(ret, typeutils.Map(in.LockingStrategy.Validate(), v1beta2.AddPrefixToFieldError("locking"))...)
	return ret
}
