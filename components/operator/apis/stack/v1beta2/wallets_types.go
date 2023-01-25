package v1beta2

import (
	authcomponentsv1beta2 "github.com/formancehq/operator/apis/auth.components/v1beta2"
	componentsv1beta2 "github.com/formancehq/operator/apis/components/v1beta2"
	"github.com/formancehq/operator/pkg/apis/v1beta2"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

// +kubebuilder:object:generate=true
type WalletsSpec struct {
	v1beta2.DevProperties `json:",inline"`
	// +optional
	Scaling ScalingSpec `json:"scaling,omitempty"`
	// +optional
	Ingress *IngressConfig `json:"ingress"`
}

func (in WalletsSpec) NeedAuthMiddleware() bool {
	return true
}

func (in WalletsSpec) Spec(stack *Stack, configuration ConfigurationSpec) any {
	return componentsv1beta2.WalletsSpec{
		StackURL: stack.URL(),
	}
}

func (in WalletsSpec) HTTPPort() int {
	return 8080
}

func (in WalletsSpec) AuthClientConfiguration(stack *Stack) *authcomponentsv1beta2.ClientConfiguration {
	ret := authcomponentsv1beta2.NewClientConfiguration()
	return &ret
}

func (in WalletsSpec) Validate() field.ErrorList {
	return field.ErrorList{}
}
