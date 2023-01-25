package ledger

import (
	"github.com/formancehq/fctl/cmd/ledger/accounts"
	"github.com/formancehq/fctl/cmd/ledger/internal"
	"github.com/formancehq/fctl/cmd/ledger/transactions"
	fctl "github.com/formancehq/fctl/pkg"
	"github.com/spf13/cobra"
)

func NewCommand() *cobra.Command {
	return fctl.NewStackCommand("ledger",
		fctl.WithAliases("l"),
		fctl.WithPersistentStringFlag(internal.LedgerFlag, "default", "Specific ledger"),
		fctl.WithShortDescription("Ledger management"),
		fctl.WithChildCommands(
			NewBalancesCommand(),
			NewSendCommand(),
			NewStatsCommand(),
			NewServerInfoCommand(),
			NewListCommand(),
			transactions.NewLedgerTransactionsCommand(),
			accounts.NewLedgerAccountsCommand(),
		),
	)
}
