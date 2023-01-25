package v1beta2

import (
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
)

type OAuth2ConfigSpec struct {

	// +required
	IntrospectUrl string `json:"introspectUrl"`

	// +optional
	Audiences []string `json:"audiences"`

	// +optional
	AudienceWildcard bool `json:"audienceWildcard"`

	//+optional
	ProtectedByScopes bool `json:"ProtectedByScopes"`
}

type HTTPBasicConfigSpec struct {
	// +optional
	Enabled bool `json:"enabled"`

	// +optional
	Credentials map[string]string `json:"credentials"`
}

type AuthConfigSpec struct {
	// +optional
	OAuth2 *OAuth2ConfigSpec `json:"oauth2,omitempty"`

	// +optional
	HTTPBasic *HTTPBasicConfigSpec `json:"basic,omitempty"`
}

func (spec *AuthConfigSpec) Env(prefix string) []corev1.EnvVar {

	ret := []corev1.EnvVar{}
	if spec == nil {
		return ret
	}
	if spec.OAuth2 != nil {
		ret = append(ret, EnvWithPrefix(prefix, "AUTH_BEARER_ENABLED", "true"))
		ret = append(ret, EnvWithPrefix(prefix, "AUTH_BEARER_INTROSPECT_URL", spec.OAuth2.IntrospectUrl))
		if spec.OAuth2.AudienceWildcard {
			ret = append(ret, EnvWithPrefix(prefix, "AUTH_BEARER_AUDIENCES_WILDCARD", "true"))
		} else {
			ret = append(ret, EnvWithPrefix(prefix, "AUTH_BEARER_AUDIENCE", strings.Join(spec.OAuth2.Audiences, " ")))
		}
		if spec.OAuth2.ProtectedByScopes {
			ret = append(ret, EnvWithPrefix(prefix, "AUTH_BEARER_USE_SCOPES", "true"))
		}
	}
	if spec.HTTPBasic != nil && spec.HTTPBasic.Enabled {
		credentials := ""
		for k, v := range spec.HTTPBasic.Credentials {
			credentials += fmt.Sprintf("%s:%s ", k, v)
		}
		ret = append(ret, EnvWithPrefix(prefix, "AUTH_BASIC_ENABLED", "true"))
		ret = append(ret, EnvWithPrefix(prefix, "AUTH_BASIC_CREDENTIALS", credentials))
	}
	return ret
}
