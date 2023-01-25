package v1beta2

import (
	"strings"

	"github.com/formancehq/operator/pkg/typeutils"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

type KafkaSASLConfig struct {
	// +optional
	Username string `json:"username,omitempty"`
	// +optional
	UsernameFrom *ConfigSource `json:"usernameFrom,omitempty"`
	// +optional
	Password string `json:"password,omitempty"`
	// +optional
	PasswordFrom *ConfigSource `json:"passwordFrom,omitempty"`
	Mechanism    string        `json:"mechanism"`
	ScramSHASize string        `json:"scramSHASize"`
}

func (cfg *KafkaSASLConfig) Env(prefix string) []corev1.EnvVar {
	if cfg == nil {
		return []corev1.EnvVar{}
	}
	return []corev1.EnvVar{
		EnvWithPrefix(prefix, "PUBLISHER_KAFKA_SASL_ENABLED", "true"),
		SelectRequiredConfigValueOrReference("PUBLISHER_KAFKA_SASL_USERNAME", prefix,
			cfg.Username, cfg.UsernameFrom),
		SelectRequiredConfigValueOrReference("PUBLISHER_KAFKA_SASL_PASSWORD", prefix,
			cfg.Password, cfg.PasswordFrom),
		EnvWithPrefix(prefix, "PUBLISHER_KAFKA_SASL_MECHANISM", cfg.Mechanism),
		EnvWithPrefix(prefix, "PUBLISHER_KAFKA_SASL_SCRAM_SHA_SIZE", cfg.ScramSHASize),
	}
}

func (cfg *KafkaSASLConfig) Validate() field.ErrorList {
	if cfg == nil {
		return field.ErrorList{}
	}
	return typeutils.MergeAll(
		ValidateRequiredConfigValueOrReference("username",
			cfg.Username, cfg.UsernameFrom),
		ValidateRequiredConfigValueOrReference("password",
			cfg.Password, cfg.PasswordFrom),
	)
}

type KafkaConfig struct {
	// +optional
	Brokers []string `json:"brokers"`
	// +optional
	BrokersFrom *ConfigSource `json:"brokersFrom"`
	// +optional
	TLS bool `json:"tls"`
	// +optional
	SASL *KafkaSASLConfig `json:"sasl,omitempty"`
}

func (s *KafkaConfig) Env(prefix string) []corev1.EnvVar {

	ret := make([]corev1.EnvVar, 0)
	ret = append(ret,
		EnvWithPrefix(prefix, "PUBLISHER_KAFKA_ENABLED", "true"),
		SelectRequiredConfigValueOrReference("PUBLISHER_KAFKA_BROKER", prefix,
			strings.Join(s.Brokers, ","), s.BrokersFrom),
	)
	if s.SASL != nil {
		ret = append(ret, s.SASL.Env(prefix)...)
	}
	if s.TLS {
		ret = append(ret, EnvWithPrefix(prefix, "PUBLISHER_KAFKA_TLS_ENABLED", "true"))
	}

	return ret
}

func (in *KafkaConfig) Validate() field.ErrorList {
	if in == nil {
		return nil
	}
	return typeutils.MergeAll(
		typeutils.Map(in.SASL.Validate(), AddPrefixToFieldError("sasl.")),
		ValidateRequiredConfigValueOrReference("brokers.",
			strings.Join(in.Brokers, ","), in.BrokersFrom),
	)
}
