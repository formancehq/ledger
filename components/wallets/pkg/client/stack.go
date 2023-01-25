package client

import (
	"context"
	"net/http"

	sdk "github.com/formancehq/formance-sdk-go"
	"github.com/pkg/errors"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/clientcredentials"
)

func GetAuthenticatedClient(ctx context.Context, clientID, clientSecret, stackURL string) (*http.Client, error) {
	if clientID == "" || clientSecret == "" {
		return nil, errors.New("STACK_CLIENT_ID and STACK_CLIENT_SECRET must be set")
	}

	clientCredentialsConfig := clientcredentials.Config{
		ClientID:     clientID,
		ClientSecret: clientSecret,
		TokenURL:     stackURL + "/api/auth/oauth/token",
	}
	underlyingHTTPClient := &http.Client{
		Transport: otelhttp.NewTransport(http.DefaultTransport),
	}

	return clientCredentialsConfig.Client(context.WithValue(ctx, oauth2.HTTPClient, underlyingHTTPClient)), nil
}

func NewStackClient(clientID, clientSecret, stackURL string) (*sdk.APIClient, error) {
	config := sdk.NewConfiguration()
	config.Servers = sdk.ServerConfigurations{{
		URL: stackURL,
	}}

	httpClient, err := GetAuthenticatedClient(context.Background(), clientID, clientSecret, stackURL)
	if err != nil {
		return nil, err
	}
	config.HTTPClient = httpClient

	return sdk.NewAPIClient(config), nil
}
