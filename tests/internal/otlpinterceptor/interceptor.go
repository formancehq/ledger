package otlpinterceptor

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

var _ component.TracesProcessor = (*interceptor)(nil)
var GlobalInterceptor = &interceptor{}

type interceptor struct {
	traces Traces
}

func (i *interceptor) Start(ctx context.Context, host component.Host) error {
	return nil
}

func (i *interceptor) Shutdown(ctx context.Context) error {
	return nil
}

func (i *interceptor) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{}
}

func (i *interceptor) ConsumeTraces(ctx context.Context, td ptrace.Traces) error {
	i.traces = append(i.traces, td)
	return nil
}

func (i *interceptor) Traces() Traces {
	return i.traces
}
