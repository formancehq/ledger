package v1beta2

import (
	"fmt"

	authcomponentsv1beta2 "github.com/formancehq/operator/apis/auth.components/v1beta2"
	componentsv1beta2 "github.com/formancehq/operator/apis/components/v1beta2"
)

// +kubebuilder:object:generate=true
type ControlSpec struct {
	// +optional
	Scaling ScalingSpec `json:"scaling,omitempty"`
	// +optional
	Ingress *IngressConfig `json:"ingress"`
}

func (in ControlSpec) NeedAuthMiddleware() bool {
	return false
}

func (in ControlSpec) Spec(stack *Stack, configuration ConfigurationSpec) any {
	return componentsv1beta2.ControlSpec{
		ApiURLFront: fmt.Sprintf("%s/api", stack.URL()),
		ApiURLBack:  fmt.Sprintf("%s/api", stack.URL()),
	}
}

func (in ControlSpec) HTTPPort() int {
	return 3000
}

func (in ControlSpec) Path() string {
	return "/"
}

func (in ControlSpec) AuthClientConfiguration(stack *Stack) *authcomponentsv1beta2.ClientConfiguration {
	ret := authcomponentsv1beta2.NewClientConfiguration().
		WithAdditionalScopes("profile", "email", "offline").
		WithRedirectUris(fmt.Sprintf("%s/auth/login", stack.URL())).
		WithPostLogoutRedirectUris(fmt.Sprintf("%s/auth/destroy", stack.URL()))
	return &ret
}
