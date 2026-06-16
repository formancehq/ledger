package auth

import (
	"errors"
	"fmt"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
)

// NewLogoutCommand returns the "auth logout" command.
func NewLogoutCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "logout",
		Short: "Remove stored token from the OS keychain",
		Long: `Remove the JWT bearer token stored in the OS keychain for the current
--server address.`,
		Args:              cobra.ExactArgs(0),
		ValidArgsFunction: cobra.NoFileCompletions,
		RunE:              runLogout,
	}
}

func runLogout(cmd *cobra.Command, _ []string) error {
	server, _ := cmd.Flags().GetString("server")

	err := cmdutil.GetKeyring(cmd).Delete(server)
	if errors.Is(err, cmdutil.ErrTokenNotFound) {
		pterm.Info.Printfln("No token stored for server %s", pterm.Bold.Sprint(server))

		return nil
	}

	if err != nil {
		return fmt.Errorf("removing token from keychain: %w", err)
	}

	pterm.Success.Printfln("Token removed for server %s", pterm.Bold.Sprint(server))

	return nil
}
