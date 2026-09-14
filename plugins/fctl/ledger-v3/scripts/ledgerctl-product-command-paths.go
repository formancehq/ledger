//go:build ignore

// This helper is run from the repository root by catalogue_test.go. Keeping it
// in the root module lets the contract test construct the real ledgerctl tree
// without pulling the entire product CLI dependency graph into the plugin
// module metadata.
package main

import (
	"encoding/json"
	"os"
	"sort"
	"strings"

	"github.com/spf13/cobra"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/accounts"
	"github.com/formancehq/ledger/v3/cmd/ledgerctl/accounttypes"
	"github.com/formancehq/ledger/v3/cmd/ledgerctl/audit"
	"github.com/formancehq/ledger/v3/cmd/ledgerctl/indexes"
	"github.com/formancehq/ledger/v3/cmd/ledgerctl/ledgers"
	"github.com/formancehq/ledger/v3/cmd/ledgerctl/logs"
	"github.com/formancehq/ledger/v3/cmd/ledgerctl/numscripts"
	"github.com/formancehq/ledger/v3/cmd/ledgerctl/queries"
	"github.com/formancehq/ledger/v3/cmd/ledgerctl/transactions"
)

func main() {
	constructors := []func() *cobra.Command{
		accounttypes.NewCommand,
		accounts.NewCommand,
		audit.NewCommand,
		indexes.NewCommand,
		ledgers.NewCommand,
		logs.NewCommand,
		numscripts.NewCommand,
		queries.NewCommand,
		transactions.NewCommand,
	}

	// Mirror promotion changes the product's replication lifecycle and remains
	// an operator action. Every other runnable command under these product-owned
	// ledgerctl roots is part of the portable product surface.
	excluded := map[string]struct{}{
		"ledgers promote": {},
	}

	var paths []string
	for _, construct := range constructors {
		collectRunnableCommandPaths(construct(), nil, excluded, &paths)
	}
	sort.Strings(paths)

	if err := json.NewEncoder(os.Stdout).Encode(paths); err != nil {
		panic(err)
	}
}

func collectRunnableCommandPaths(command *cobra.Command, parents []string, excluded map[string]struct{}, paths *[]string) {
	path := append(append([]string(nil), parents...), command.Name())
	joined := strings.Join(path, " ")
	if command.Run != nil || command.RunE != nil {
		if _, skip := excluded[joined]; !skip {
			*paths = append(*paths, joined)
		}
	}
	for _, child := range command.Commands() {
		collectRunnableCommandPaths(child, path, excluded, paths)
	}
}
