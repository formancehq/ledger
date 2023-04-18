package httpclient

import (
	"net/http"
	"net/http/httputil"

	"github.com/formancehq/stack/libs/go-libs/logging"
)

type httpTransport struct {
	underlying http.RoundTripper
}

func (h httpTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	data, err := httputil.DumpRequest(request, true)
	if err != nil {
		panic(err)
	}
	logging.FromContext(request.Context()).Debug(string(data))

	rsp, err := h.underlying.RoundTrip(request)
	if err != nil {
		return nil, err
	}

	data, err = httputil.DumpResponse(rsp, true)
	if err != nil {
		panic(err)
	}
	logging.FromContext(request.Context()).Debug(string(data))

	return rsp, nil
}

var _ http.RoundTripper = &httpTransport{}

func NewDebugHTTPTransport(underlying http.RoundTripper) *httpTransport {
	return &httpTransport{
		underlying: underlying,
	}
}
