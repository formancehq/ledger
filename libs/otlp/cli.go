package otlp

import (
	"sync"

	flag "github.com/spf13/pflag"
)

var (
	once sync.Once
)

const (
	OtelResourceAttributes = "otel-resource-attributes"
	OtelServiceName        = "otel-service-name"
)

func InitOTLPFlags(flags *flag.FlagSet) {
	once.Do(func() {
		flags.String(OtelServiceName, "", "OpenTelemetry service name")
		flags.StringSlice(OtelResourceAttributes, []string{}, "Additional OTLP resource attributes")
	})
}
