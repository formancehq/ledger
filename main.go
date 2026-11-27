package main

import (
	"github.com/formancehq/go-libs/v5/pkg/service"

	"github.com/formancehq/ledger/v3/cmd/server"
)

func main() {
	rootCmd := server.NewServiceRootCommand()
	service.Execute(rootCmd)
}
