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

package v1beta2

import (
	"time"

	pkgapisv1beta2 "github.com/formancehq/operator/pkg/apis/v1beta2"
	"github.com/formancehq/operator/pkg/typeutils"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

type LockingStrategyRedisConfig struct {
	// +optional
	Uri string `json:"uri,omitempty"`
	// +optional
	UriFrom *pkgapisv1beta2.ConfigSource `json:"uriFrom,omitempty"`
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
		pkgapisv1beta2.SelectRequiredConfigValueOrReference("LOCK_STRATEGY_REDIS_URL", prefix,
			cfg.Uri, cfg.UriFrom),
	}
	if cfg.Duration != 0 {
		ret = append(ret, pkgapisv1beta2.EnvWithPrefix(prefix, "LOCK_STRATEGY_REDIS_DURATION", cfg.Duration.String()))
	}
	if cfg.Retry != 0 {
		ret = append(ret, pkgapisv1beta2.EnvWithPrefix(prefix, "LOCK_STRATEGY_REDIS_RETRY", cfg.Retry.String()))
	}
	if cfg.TLS {
		ret = append(ret, pkgapisv1beta2.EnvWithPrefix(prefix, "LOCK_STRATEGY_REDIS_TLS_ENABLED", "true"))
	}
	if cfg.InsecureTLS {
		ret = append(ret, pkgapisv1beta2.EnvWithPrefix(prefix, "LOCK_STRATEGY_REDIS_TLS_INSECURE", "true"))
	}
	return ret
}

func (cfg *LockingStrategyRedisConfig) Validate() field.ErrorList {
	if cfg == nil {
		return field.ErrorList{}
	}
	return pkgapisv1beta2.ValidateRequiredConfigValueOrReference("uri", cfg.Uri, cfg.UriFrom)
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
	ret = append(ret, pkgapisv1beta2.EnvWithPrefix(prefix, "LOCK_STRATEGY", s.Strategy))
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
	return typeutils.MergeAll(ret, s.Redis.Validate())
}

// LedgerSpec defines the desired state of Ledger
type LedgerSpec struct {
	pkgapisv1beta2.CommonServiceProperties `json:",inline"`
	pkgapisv1beta2.Scalable                `json:",inline"`

	// +optional
	Postgres PostgresConfigCreateDatabase `json:"postgres"`
	// +optional
	Monitoring *pkgapisv1beta2.MonitoringSpec `json:"monitoring"`
	// +optional
	Collector *CollectorConfig `json:"collector"`

	LockingStrategy LockingStrategy `json:"locking"`
}

//+kubebuilder:object:root=true
//+kubebuilder:subresource:status
//+kubebuilder:subresource:scale:specpath=.spec.replicas,statuspath=.status.replicas,selectorpath=.status.selector
//+kubebuilder:storageversion

// Ledger is the Schema for the ledgers API
type Ledger struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec LedgerSpec `json:"spec"`
	// +optional
	Status pkgapisv1beta2.ReplicationStatus `json:"status"`
}

func (a *Ledger) GetStatus() pkgapisv1beta2.Dirty {
	return &a.Status
}

func (a *Ledger) IsDirty(t pkgapisv1beta2.Object) bool {
	return false
}

func (a *Ledger) GetConditions() *pkgapisv1beta2.Conditions {
	return &a.Status.Conditions
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
