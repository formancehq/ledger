package client

import (
	sdk "github.com/formancehq/formance-sdk-go"
	"go.uber.org/fx"
)

func NewModule(clientID string, clientSecret string, tokenURL string) fx.Option {
	return fx.Provide(func() (*sdk.APIClient, error) {
		return NewStackClient(clientID, clientSecret, tokenURL)
	})
}
