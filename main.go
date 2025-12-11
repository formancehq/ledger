package main

import (
	"github.com/formancehq/go-libs/v3/service"
	"github.com/formancehq/ledger-v3-poc/cmd/server"
)

func main() {
	rootCmd := server.NewRootCommand()
	service.Execute(rootCmd)
}