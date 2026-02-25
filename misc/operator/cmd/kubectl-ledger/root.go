package main

import (
	"github.com/spf13/cobra"

	"github.com/formancehq/ledger-v3-poc/operator/cmd/kubectl-ledger/cmdutil"
	configcmd "github.com/formancehq/ledger-v3-poc/operator/cmd/kubectl-ledger/config"
	"github.com/formancehq/ledger-v3-poc/operator/cmd/kubectl-ledger/create"
	deletecmd "github.com/formancehq/ledger-v3-poc/operator/cmd/kubectl-ledger/delete"
	"github.com/formancehq/ledger-v3-poc/operator/cmd/kubectl-ledger/get"
	"github.com/formancehq/ledger-v3-poc/operator/cmd/kubectl-ledger/list"
	"github.com/formancehq/ledger-v3-poc/operator/cmd/kubectl-ledger/logs"
	"github.com/formancehq/ledger-v3-poc/operator/cmd/kubectl-ledger/portforward"
	"github.com/formancehq/ledger-v3-poc/operator/cmd/kubectl-ledger/restart"
	"github.com/formancehq/ledger-v3-poc/operator/cmd/kubectl-ledger/scale"
	"github.com/formancehq/ledger-v3-poc/operator/cmd/kubectl-ledger/status"
)

func newRootCommand() *cobra.Command {
	opts := &cmdutil.Options{}

	cmd := &cobra.Command{
		Use:   "kubectl-ledger",
		Short: "Manage Ledger deployments on Kubernetes",
		Long:  "A kubectl plugin for managing Ledger CRDs deployed by the Formance Ledger Operator.",
		SilenceUsage:  true,
		SilenceErrors: true,
	}

	opts.AddFlags(cmd)

	cmd.AddCommand(
		list.NewCommand(opts),
		get.NewCommand(opts),
		create.NewCommand(opts),
		deletecmd.NewCommand(opts),
		status.NewCommand(opts),
		scale.NewCommand(opts),
		restart.NewCommand(opts),
		logs.NewCommand(opts),
		portforward.NewCommand(opts),
		configcmd.NewCommand(opts),
		newVersionCommand(),
	)

	return cmd
}
