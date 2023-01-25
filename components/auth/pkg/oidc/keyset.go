package oidc

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/formancehq/auth/pkg/delegatedauth"
	"github.com/zitadel/oidc/pkg/client"
	"gopkg.in/square/go-jose.v2"
)

func ReadKeySet(httpClient *http.Client, ctx context.Context, configuration delegatedauth.Config) (*jose.JSONWebKeySet, error) {
	// TODO: Inefficient, should keep public keys locally and use them instead of calling the network
	discoveryConfiguration, err := client.Discover(configuration.Issuer, httpClient)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, discoveryConfiguration.JwksURI, nil)
	if err != nil {
		return nil, err
	}

	rsp, err := httpClient.Do(req)
	if err != nil {
		return nil, err
	}

	keySet := jose.JSONWebKeySet{}
	if err := json.NewDecoder(rsp.Body).Decode(&keySet); err != nil {
		return nil, err
	}

	return &keySet, nil
}
