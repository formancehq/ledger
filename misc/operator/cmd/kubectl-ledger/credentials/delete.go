package credentials

import (
	"fmt"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"

	"github.com/formance/ledger/operator/cmd/kubectl-ledger/cmdutil"
)

type deleteFlags struct {
	yes bool
}

func newDeleteCommand(opts *cmdutil.Options) *cobra.Command {
	var f deleteFlags

	cmd := &cobra.Command{
		Use:     "delete [name]",
		Aliases: []string{"rm", "del"},
		Short:   "Delete a Credentials resource",
		Args:    cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runDelete(cmd, opts, &f, args)
		},
	}

	cmd.Flags().BoolVarP(&f.yes, "yes", "y", false, "Skip confirmation")

	return cmd
}

func runDelete(cmd *cobra.Command, opts *cmdutil.Options, f *deleteFlags, args []string) error {
	ctx := cmd.Context()

	name, err := cmdutil.ResolveCredentialsName(ctx, opts, args)
	if err != nil {
		return err
	}

	crdClient, err := opts.CRDClient()
	if err != nil {
		return fmt.Errorf("creating CRD client: %w", err)
	}

	agent, err := cmdutil.GetCredentials(ctx, crdClient, name)
	if err != nil {
		return fmt.Errorf("getting agent %q: %w", name, err)
	}

	if !f.yes {
		pterm.Println()
		rows := [][]string{
			{"Name", pterm.Cyan(name)},
			{"Scope", "Cluster"},
			{"Key ID", agent.Status.KeyID},
		}
		if len(agent.Status.MatchedServices) > 0 {
			rows = append(rows, []string{
				"Matched Services",
				pterm.Yellow(fmt.Sprintf("%d Cluster(s)", len(agent.Status.MatchedServices))),
			})
		}
		cmdutil.RenderBoxedTable(rows)
		pterm.Println()

		pterm.Warning.Println("This will also delete the associated Secret containing the Ed25519 keypair.")

		confirm, err := cmdutil.PromptConfirm(
			fmt.Sprintf("Delete Credentials %s?", pterm.Cyan(name)),
			false,
		)
		if err != nil {
			return err
		}
		if !confirm {
			pterm.Warning.Println("Aborted.")

			return nil
		}
	}

	spinner, _ := pterm.DefaultSpinner.Start("Deleting Credentials...")

	if err := crdClient.Delete(ctx, agent); err != nil {
		spinner.Fail("Failed to delete Credentials")

		return fmt.Errorf("deleting agent %q: %w", name, err)
	}

	spinner.Success(fmt.Sprintf("Credentials %s deleted", pterm.Cyan(name)))

	return nil
}
