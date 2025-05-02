package cmd

import (
	"context"
	"crypto/tls"
	"fmt"
	"github.com/formancehq/ledger/pkg/client"
	"golang.org/x/oauth2"
	"k8s.io/client-go/tools/clientcmd/api"
	"net/http"
	"net/http/httputil"
)

func createAPIClient(namespace, service, token string, config *api.Config, printer func(context.Context) func(fmt string, args ...interface{})) *client.Formance {
	var httpTransport http.RoundTripper = &http.Transport{
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: true,
		},
	}
	httpTransport = newDebugHTTPTransport(httpTransport, printer)

	oauth2Client := oauth2.NewClient(
		context.WithValue(context.Background(), oauth2.HTTPClient, &http.Client{
			Transport: httpTransport,
		}),
		oauth2.StaticTokenSource(&oauth2.Token{
			TokenType:   "Bearer",
			AccessToken: token,
		}),
	)

	return client.New(
		client.WithServerURL(fmt.Sprintf(
			"%s/api/v1/namespaces/%s/services/%s:8080/proxy",
			config.Clusters[config.Contexts[config.CurrentContext].Cluster].Server,
			namespace,
			service,
		)),
		client.WithClient(oauth2Client),
	)
}

type httpTransport struct {
	underlying http.RoundTripper
	printer    func(ctx context.Context) func(fmt string, args ...interface{})
}

func (h httpTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	data, err := httputil.DumpRequest(request, true)
	if err != nil {
		panic(err)
	}

	h.printer(request.Context())(string(data) + "\r\n")

	rsp, err := h.underlying.RoundTrip(request)
	if err != nil {
		return nil, err
	}

	data, err = httputil.DumpResponse(rsp, true)
	if err != nil {
		panic(err)
	}

	h.printer(request.Context())(string(data) + "\r\n")

	return rsp, nil
}

var _ http.RoundTripper = &httpTransport{}

func newDebugHTTPTransport(underlying http.RoundTripper, printer func(ctx context.Context) func(fmt string, args ...interface{})) *httpTransport {
	return &httpTransport{
		underlying: underlying,
		printer:    printer,
	}
}
