package activities

import (
	sdk "github.com/formancehq/formance-sdk-go"
)

type Activities struct {
	client *sdk.APIClient
}

func New(client *sdk.APIClient) Activities {
	return Activities{
		client: client,
	}
}
