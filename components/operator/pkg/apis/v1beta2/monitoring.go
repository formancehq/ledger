// +kubebuilder:object:generate=true
package v1beta2

import (
	"fmt"

	"github.com/formancehq/operator/pkg/typeutils"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

type MonitoringSpec struct {
	// +optional
	Traces *TracesSpec `json:"traces,omitempty"`
}

func (in *MonitoringSpec) Env(prefix string) []v1.EnvVar {
	ret := make([]v1.EnvVar, 0)
	if in.Traces != nil {
		ret = append(ret, in.Traces.Env(prefix)...)
	}
	return ret
}

func (in *MonitoringSpec) Validate() field.ErrorList {
	if in == nil {
		return field.ErrorList{}
	}
	return in.Traces.Validate()
}

type TracesOtlpSpec struct {
	// +optional
	Endpoint string `json:"endpoint,omitempty"`
	// +optional
	EndpointFrom *v1.EnvVarSource `json:"endpointFrom,omitempty"`
	// +optional
	Port int32 `json:"port,omitempty"`
	// +optional
	PortFrom *v1.EnvVarSource `json:"portFrom,omitempty"`
	// +optional
	Insecure bool `json:"insecure,omitempty"`
	// +kubebuilder:validation:Enum:={grpc,http}
	// +kubebuilder:validation:default:=grpc
	// +optional
	Mode string `json:"mode,omitempty"`
	// +optional
	ResourceAttributes string `json:"resourceAttributes,omitempty"`
}

func (in *TracesOtlpSpec) Env(prefix string) []v1.EnvVar {
	env := []v1.EnvVar{
		EnvWithPrefix(prefix, "OTEL_TRACES", "true"),
		EnvWithPrefix(prefix, "OTEL_TRACES_EXPORTER", "otlp"),
		EnvWithPrefix(prefix, "OTEL_TRACES_EXPORTER_OTLP_INSECURE", fmt.Sprintf("%t", in.Insecure)),
		EnvWithPrefix(prefix, "OTEL_TRACES_EXPORTER_OTLP_MODE", in.Mode),
		SelectRequiredConfigValueOrReference("OTEL_TRACES_PORT", prefix, in.Port, in.PortFrom),
		SelectRequiredConfigValueOrReference("OTEL_TRACES_ENDPOINT", prefix,
			in.Endpoint, in.EndpointFrom),
		EnvWithPrefix(prefix, "OTEL_TRACES_EXPORTER_OTLP_ENDPOINT",
			ComputeEnvVar(prefix, "%s:%s", "OTEL_TRACES_ENDPOINT", "OTEL_TRACES_PORT")),
		EnvWithPrefix(prefix, "OTEL_RESOURCE_ATTRIBUTES", in.ResourceAttributes),
	}

	return env
}

func (in *TracesOtlpSpec) Validate() field.ErrorList {
	return typeutils.MergeAll(
		ValidateRequiredConfigValueOrReference("endpoint", in.Endpoint, in.EndpointFrom),
		ValidateRequiredConfigValueOrReference("port", in.Port, in.PortFrom),
	)
}

type TracesSpec struct {
	// +optional
	Otlp *TracesOtlpSpec `json:"otlp,omitempty"`
}

func (in *TracesSpec) Env(prefix string) []v1.EnvVar {
	ret := make([]v1.EnvVar, 0)
	if in.Otlp != nil {
		ret = append(ret, in.Otlp.Env(prefix)...)
	}
	return ret
}

func (in *TracesSpec) Validate() field.ErrorList {
	if in == nil {
		return field.ErrorList{}
	}
	return in.Otlp.Validate()
}
