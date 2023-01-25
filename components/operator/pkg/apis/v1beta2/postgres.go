// +kubebuilder:object:generate=true
package v1beta2

import (
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

type ConfigSource struct {
	// Selects a key of a ConfigMap.
	// +optional
	ConfigMapKeyRef *corev1.ConfigMapKeySelector `json:"configMapKeyRef,omitempty" protobuf:"bytes,3,opt,name=configMapKeyRef"`
	// Selects a key of a secret in the pod's namespace
	// +optional
	SecretKeyRef *corev1.SecretKeySelector `json:"secretKeyRef,omitempty" protobuf:"bytes,4,opt,name=secretKeyRef"`
}

func (c *ConfigSource) Env() *corev1.EnvVarSource {
	return &corev1.EnvVarSource{
		ConfigMapKeyRef: c.ConfigMapKeyRef,
		SecretKeyRef:    c.SecretKeyRef,
	}
}

type PostgresConfig struct {
	// +optional
	Port int `json:"port"`
	// +optional
	PortFrom *ConfigSource `json:"portFrom"`
	// +optional
	Host string `json:"host"`
	// +optional
	HostFrom *ConfigSource `json:"hostFrom"`
	// +optional
	Username string `json:"username"`
	// +optional
	UsernameFrom *ConfigSource `json:"usernameFrom"`
	// +optional
	Password string `json:"password"`
	// +optional
	PasswordFrom *ConfigSource `json:"passwordFrom"`
	// +optional
	DisableSSLMode bool `json:"disableSSLMode"`
}

type PostgresConfigWithDatabase struct {
	PostgresConfig `json:",inline"`
	// +optional
	Database string `json:"database"`
	// +optional
	DatabaseFrom *ConfigSource `json:"databaseFrom"`
}

func (c *PostgresConfigWithDatabase) Env(prefix string) []corev1.EnvVar {
	return c.EnvWithDiscriminator(prefix, "")
}

func (c *PostgresConfigWithDatabase) EnvWithDiscriminator(prefix, discriminator string) []corev1.EnvVar {
	discriminator = strings.ToUpper(discriminator)
	withDiscriminator := func(v string) string {
		if discriminator == "" {
			return v
		}
		return fmt.Sprintf("%s_%s", v, discriminator)
	}
	ret := make([]corev1.EnvVar, 0)
	ret = append(ret, SelectRequiredConfigValueOrReference(withDiscriminator("POSTGRES_DATABASE"), prefix,
		c.Database, c.DatabaseFrom))
	return append(ret, c.PostgresConfig.EnvWithDiscriminator(prefix, discriminator)...)
}

func (c *PostgresConfigWithDatabase) Validate() field.ErrorList {
	ret := field.ErrorList{}
	ret = append(ret, c.PostgresConfig.Validate()...)
	return append(ret, ValidateRequiredConfigValueOrReference("database", c.Database, c.DatabaseFrom)...)
}

func (c *PostgresConfig) Validate() field.ErrorList {
	ret := field.ErrorList{}
	ret = append(ret, ValidateRequiredConfigValueOrReference("host", c.Host, c.HostFrom)...)
	ret = append(ret, ValidateRequiredConfigValueOrReference("port", c.Port, c.PortFrom)...)
	ret = append(ret, ValidateRequiredConfigValueOrReferenceOnly("username", c.Username, c.UsernameFrom)...)

	if c.Username != "" || c.UsernameFrom != nil {
		ret = append(ret, ValidateRequiredConfigValueOrReference("password", c.Password, c.PasswordFrom)...)
	}
	return ret
}

func (c *PostgresConfig) EnvWithDiscriminator(prefix, discriminator string) []corev1.EnvVar {

	discriminator = strings.ToUpper(discriminator)
	withDiscriminator := func(v string) string {
		if discriminator == "" {
			return v
		}
		return fmt.Sprintf("%s_%s", v, discriminator)
	}

	ret := make([]corev1.EnvVar, 0)
	ret = append(ret, SelectRequiredConfigValueOrReference(withDiscriminator("POSTGRES_HOST"), prefix, c.Host, c.HostFrom))
	ret = append(ret, SelectRequiredConfigValueOrReference(withDiscriminator("POSTGRES_PORT"), prefix, c.Port, c.PortFrom))

	if c.Username != "" || c.UsernameFrom != nil {
		ret = append(ret, SelectRequiredConfigValueOrReference(withDiscriminator("POSTGRES_USERNAME"), prefix, c.Username, c.UsernameFrom))
		ret = append(ret, SelectRequiredConfigValueOrReference(withDiscriminator("POSTGRES_PASSWORD"), prefix, c.Password, c.PasswordFrom))

		ret = append(ret, EnvWithPrefix(prefix, withDiscriminator("POSTGRES_NO_DATABASE_URI"),
			ComputeEnvVar(prefix, "postgresql://%s:%s@%s:%s",
				withDiscriminator("POSTGRES_USERNAME"),
				withDiscriminator("POSTGRES_PASSWORD"),
				withDiscriminator("POSTGRES_HOST"),
				withDiscriminator("POSTGRES_PORT"),
			),
		))
	} else {
		ret = append(ret, EnvWithPrefix(prefix, withDiscriminator("POSTGRES_NO_DATABASE_URI"),
			ComputeEnvVar(prefix, "postgresql://%s:%s", withDiscriminator("POSTGRES_HOST"), withDiscriminator("POSTGRES_PORT")),
		))
	}
	fmt := "%s/%s"
	if c.DisableSSLMode {
		fmt += "?sslmode=disable"
	}
	ret = append(ret, EnvWithPrefix(prefix, withDiscriminator("POSTGRES_URI"),
		ComputeEnvVar(prefix, fmt, withDiscriminator("POSTGRES_NO_DATABASE_URI"), withDiscriminator("POSTGRES_DATABASE")),
	))

	return ret
}

func (c *PostgresConfig) Env(prefix string) []corev1.EnvVar {
	return c.EnvWithDiscriminator(prefix, "")
}
