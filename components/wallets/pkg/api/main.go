package api

import (
	wallet "github.com/formancehq/wallets/pkg"
)

type MainHandler struct {
	manager *wallet.Manager
}

func NewMainHandler(funding *wallet.Manager) *MainHandler {
	return &MainHandler{
		manager: funding,
	}
}
