package otlpinterceptor

import (
	"fmt"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

type Traces []ptrace.Traces

func (traces Traces) Filter(fn func(traces ptrace.Traces) bool) Traces {
	ret := Traces{}
	for _, trace := range traces {
		if fn(trace) {
			ret = append(ret, trace)
		}
	}
	return ret
}

func (traces Traces) RootSpans() Traces {
	return traces.FilterSpanID(pcommon.NewSpanID([8]byte{0, 0, 0, 0, 0, 0, 0, 0}))
}

func (traces Traces) FilterTraceID(traceId string) Traces {
	return traces.Filter(func(traces ptrace.Traces) bool {
		for i := 0; i < traces.ResourceSpans().Len(); i++ {
			resourceSpan := traces.ResourceSpans().At(i)
			for j := 0; j < resourceSpan.ScopeSpans().Len(); j++ {
				scopeSpan := resourceSpan.ScopeSpans().At(j)
				for k := 0; k < scopeSpan.Spans().Len(); k++ {
					span := scopeSpan.Spans().At(k)
					if span.TraceID().HexString() == traceId {
						return true
					}
				}
			}
		}
		return false
	})
}

func (traces Traces) FilterSpanID(id pcommon.SpanID) Traces {
	return traces.Filter(func(traces ptrace.Traces) bool {
		for i := 0; i < traces.ResourceSpans().Len(); i++ {
			resourceSpan := traces.ResourceSpans().At(i)
			for j := 0; j < resourceSpan.ScopeSpans().Len(); j++ {
				scopeSpan := resourceSpan.ScopeSpans().At(j)
				for k := 0; k < scopeSpan.Spans().Len(); k++ {
					span := scopeSpan.Spans().At(k)
					if span.ParentSpanID().HexString() == id.HexString() {
						return true
					}
				}
			}
		}
		return false
	})
}

type Span struct {
	SubSpans   []*Span
	Name       string
	Attributes map[string]any
}

func (s Span) WithAttributes(attributes map[string]any) Span {
	s.Attributes = attributes
	return s
}

func (s Span) AddSubSpans(spans ...Span) Span {
	for _, span := range spans {
		cp := span
		s.SubSpans = append(s.SubSpans, &cp)
	}
	return s
}

func (t Span) check(allTraces Traces, otelTrace ptrace.Traces) (bool, error) {

	span := otelTrace.
		ResourceSpans().At(0).
		ScopeSpans().At(0).
		Spans().At(0)

	if t.Name != span.Name() {
		return false, fmt.Errorf("expected name '%s', got '%s'", t.Name, span.Name())
	}

	rawSpanAttributes := span.Attributes().AsRaw()
	for k, v := range t.Attributes {
		if rawSpanAttributes[k] == nil {
			return false, fmt.Errorf("attribute '%s' not found", k)
		}
		if rawSpanAttributes[k] != v {
			return false, fmt.Errorf("expect value '%s' for attribute '%s', got '%s'", v, k, rawSpanAttributes[k])
		}
	}

	subTraces := allTraces.FilterSpanID(span.SpanID())
	if len(t.SubSpans) != len(subTraces) {
		return false, fmt.Errorf("expected %d sub spans and got %d", len(t.SubSpans), len(subTraces))
	}
	for i, subSpan := range t.SubSpans {
		_, err := subSpan.check(subTraces, subTraces[i])
		if err != nil {
			return false, err
		}
	}
	return true, nil
}

func NewSpan(name string) Span {
	return Span{
		Name: name,
	}
}

type Trace = Span

func NewTrace(name string) Span {
	return NewSpan(name)
}
