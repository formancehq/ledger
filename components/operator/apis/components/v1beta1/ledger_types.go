/*
Copyright 2022.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package v1beta1

import (
	"time"

	. "github.com/formancehq/operator/pkg/apis/v1beta2"
	. "github.com/formancehq/operator/pkg/typeutils"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

type LockingStrategyRedisConfig struct {
	// +optional
	Uri string `json:"uri,omitempty"`
	// +optional
	UriFrom *ConfigSource `json:"uriFrom,omitempty"`
	// +optional
	TLS bool `json:"tls"`
	// +optional
	InsecureTLS bool `json:"insecure,omitempty"`
	// +optional
	Duration time.Duration `json:"duration,omitempty"`
	// +optional
	Retry time.Duration `json:"retry,omitempty"`
}

func (cfg LockingStrategyRedisConfig) Env(prefix string) []corev1.EnvVar {
	ret := []corev1.EnvVar{
		SelectRequiredConfigValueOrReference("LOCK_STRATEGY_REDIS_URL", prefix,
			cfg.Uri, cfg.UriFrom),
	}
	if cfg.Duration != 0 {
		ret = append(ret, EnvWithPrefix(prefix, "LOCK_STRATEGY_REDIS_DURATION", cfg.Duration.String()))
	}
	if cfg.Retry != 0 {
		ret = append(ret, EnvWithPrefix(prefix, "LOCK_STRATEGY_REDIS_RETRY", cfg.Retry.String()))
	}
	if cfg.TLS {
		ret = append(ret, EnvWithPrefix(prefix, "LOCK_STRATEGY_REDIS_TLS_ENABLED", "true"))
	}
	if cfg.InsecureTLS {
		ret = append(ret, EnvWithPrefix(prefix, "LOCK_STRATEGY_REDIS_TLS_INSECURE", "true"))
	}
	return ret
}

func (cfg *LockingStrategyRedisConfig) Validate() field.ErrorList {
	if cfg == nil {
		return field.ErrorList{}
	}
	return ValidateRequiredConfigValueOrReference("uri", cfg.Uri, cfg.UriFrom)
}

type LockingStrategy struct {
	// +kubebuilder:Enum:={memory,redis}
	// +kubebuilder:default:=memory
	// +optional
	Strategy string `json:"strategy,omitempty"`
	// +optional
	Redis *LockingStrategyRedisConfig `json:"redis"`
}

func (s LockingStrategy) Env(prefix string) []corev1.EnvVar {
	ret := make([]corev1.EnvVar, 0)
	if s.Redis != nil {
		ret = append(ret, s.Redis.Env(prefix)...)
	}
	ret = append(ret, EnvWithPrefix(prefix, "LOCK_STRATEGY", s.Strategy))
	return ret
}

func (s *LockingStrategy) Validate() field.ErrorList {
	ret := field.ErrorList{}
	switch {
	case s.Strategy == "redis" && s.Redis == nil:
		ret = append(ret, field.Required(field.NewPath("redis"), "config must be specified"))
	case s.Strategy != "redis" && s.Redis != nil:
		ret = append(ret, field.Required(field.NewPath("redis"), "config must not be specified if locking strategy is memory"))
	}
	return MergeAll(ret, s.Redis.Validate())
}

type PostgresConfigCreateDatabase struct {
	PostgresConfigWithDatabase `json:",inline"`
	CreateDatabase             bool `json:"createDatabase"`
}

type CollectorConfig struct {
	KafkaConfig `json:",inline"`
	Topic       string `json:"topic"`
}

func (c CollectorConfig) Env(prefix string) []corev1.EnvVar {
	ret := c.KafkaConfig.Env(prefix)
	return append(ret, EnvWithPrefix(prefix, "PUBLISHER_TOPIC_MAPPING", "*:"+c.Topic))
}

// LedgerSpec defines the desired state of Ledger
type LedgerSpec struct {
	Scalable    `json:",inline"`
	ImageHolder `json:",inline"`
	// +optional
	Ingress *IngressSpec `json:"ingress"`
	// +optional
	Debug bool `json:"debug"`
	// +optional
	Postgres PostgresConfigCreateDatabase `json:"postgres"`
	// +optional
	Auth *AuthConfigSpec `json:"auth"`
	// +optional
	Monitoring *MonitoringSpec `json:"monitoring"`
	// +optional
	Collector *CollectorConfig `json:"collector"`

	LockingStrategy LockingStrategy `json:"locking"`
}

// Ledger is the Schema for the ledgers API
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:subresource:scale:specpath=.spec.replicas,statuspath=.status.replicas,selectorpath=.status.selector
type Ledger struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec LedgerSpec `json:"spec"`
	// +optional
	Status ReplicationStatus `json:"status"`
}

func (a *Ledger) GetStatus() Dirty {
	return &a.Status
}

func (a *Ledger) IsDirty(t Object) bool {
	return false
}

func (a *Ledger) GetConditions() *Conditions {
	return &a.Status.Conditions
}

func (in *Ledger) GetImage() string {
	return in.Spec.GetImage("ledger")
}

//+kubebuilder:object:root=true

// LedgerList contains a list of Ledger
type LedgerList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Ledger `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Ledger{}, &LedgerList{})
}
