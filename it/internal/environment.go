package internal

import (
	"github.com/numary/numary-sdk-go"
)

type Environment struct {
	*ledgerclient.APIClient
}

func (e *Environment) ServerConfig() ledgerclient.ConfigInfo {
	//TODO Make configurable
	return ledgerclient.ConfigInfo{
		Config: ledgerclient.Config{
			Storage: ledgerclient.LedgerStorage{
				Driver:  "postgres",
				Ledgers: []string{},
			},
		},
		Server:  "numary-ledger",
		Version: "develop",
	}
}

func NewEnvironment(client *ledgerclient.APIClient) *Environment {
	return &Environment{
		APIClient: client,
	}
}
