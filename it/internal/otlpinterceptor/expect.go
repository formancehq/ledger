package otlpinterceptor

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/numary/ledger/cmd"
	"github.com/onsi/gomega/types"
)

func traceID(res *http.Response) string {
	return res.Header.Get(cmd.TraceIdHeader)
}

type haveTrace struct {
	trace Trace
}

func (a haveTrace) Match(actual interface{}) (success bool, err error) {
	rsp, ok := actual.(*http.Response)
	if !ok {
		return false, errors.New("have trace expect an object of *http.Response")
	}

	traceSpans := GlobalInterceptor.Traces().FilterTraceID(traceID(rsp))
	traceWithRootSpanOnly := traceSpans.RootSpans()[0]

	return a.trace.check(traceSpans, traceWithRootSpanOnly)
}

func (a haveTrace) FailureMessage(actual interface{}) (message string) {
	return fmt.Sprintf("expected '%s' to have trace \r\n%#v", actual, a.trace)
}

func (a haveTrace) NegatedFailureMessage(actual interface{}) (message string) {
	return fmt.Sprintf("expected '%s' to not have trace \r\n%#v", actual, a.trace)
}

var _ types.GomegaMatcher = &haveTrace{}

func HaveTrace(trace Trace) *haveTrace {
	return &haveTrace{
		trace: trace,
	}
}
