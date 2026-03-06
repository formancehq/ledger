package v2

import (
	"context"
	"net/http"

	"golang.org/x/oauth2/clientcredentials"
)

// NewOAuth2ClientCredentialsClient returns an *http.Client that automatically
// obtains, caches, and refreshes an OAuth2 access token using the client
// credentials grant.
func NewOAuth2ClientCredentialsClient(clientID, clientSecret, tokenEndpoint string, scopes []string) *http.Client {
	cfg := &clientcredentials.Config{
		ClientID:     clientID,
		ClientSecret: clientSecret,
		TokenURL:     tokenEndpoint,
		Scopes:       scopes,
	}

	return cfg.Client(context.Background())
}
