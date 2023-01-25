package client

import (
	"context"
	"fmt"
	"net/http"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

type apiTransport struct {
	authToken  string
	underlying *otelhttp.Transport
}

func (t *apiTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	req.Header.Add("X-Auth-Token", t.authToken)

	return t.underlying.RoundTrip(req)
}

type Client struct {
	httpClient *http.Client
	endpoint   string
	loginID    string
	apiKey     string
}

func (c *Client) buildEndpoint(path string, args ...interface{}) string {
	return fmt.Sprintf("%s/%s", c.endpoint, fmt.Sprintf(path, args...))
}

const devAPIEndpoint = "https://devapi.currencycloud.com"

func newAuthenticatedHTTPClient(authToken string) *http.Client {
	return &http.Client{
		Transport: &apiTransport{
			authToken:  authToken,
			underlying: otelhttp.NewTransport(http.DefaultTransport),
		},
	}
}

func newHTTPClient() *http.Client {
	return &http.Client{
		Transport: otelhttp.NewTransport(http.DefaultTransport),
	}
}

// NewClient creates a new client for the CurrencyCloud API.
func NewClient(ctx context.Context, loginID, apiKey, endpoint string) (*Client, error) {
	if endpoint == "" {
		endpoint = devAPIEndpoint
	}

	c := &Client{
		httpClient: newHTTPClient(),
		endpoint:   endpoint,
		loginID:    loginID,
		apiKey:     apiKey,
	}

	authToken, err := c.authenticate(ctx)
	if err != nil {
		return nil, err
	}

	c.httpClient = newAuthenticatedHTTPClient(authToken)

	return c, nil
}
