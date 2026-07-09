package main

import (
	"github.com/spf13/cobra"

	"github.com/formance/ledger/operator/cmd/kubectl-ledger/backup"
	"github.com/formance/ledger/operator/cmd/kubectl-ledger/cmdutil"
	"github.com/formance/ledger/operator/cmd/kubectl-ledger/create"
	credentialscmd "github.com/formance/ledger/operator/cmd/kubectl-ledger/credentials"
	deletecmd "github.com/formance/ledger/operator/cmd/kubectl-ledger/delete"
	"github.com/formance/ledger/operator/cmd/kubectl-ledger/explain"
	"github.com/formance/ledger/operator/cmd/kubectl-ledger/get"
	"github.com/formance/ledger/operator/cmd/kubectl-ledger/list"
	"github.com/formance/ledger/operator/cmd/kubectl-ledger/logs"
	"github.com/formance/ledger/operator/cmd/kubectl-ledger/portforward"
	"github.com/formance/ledger/operator/cmd/kubectl-ledger/restart"
	"github.com/formance/ledger/operator/cmd/kubectl-ledger/scale"
	"github.com/formance/ledger/operator/cmd/kubectl-ledger/update"
)

func newRootCommand() *cobra.Command {
	opts := &cmdutil.Options{}

	cmd := &cobra.Command{
		Use:           "kubectl-ledger",
		Short:         "Manage Cluster deployments on Kubernetes",
		Long:          "A kubectl plugin for managing Cluster CRDs deployed by the Formance Cluster Operator.",
		SilenceUsage:  true,
		SilenceErrors: true,
	}

	opts.AddFlags(cmd)

	cmd.AddCommand(
		list.NewCommand(opts),
		get.NewCommand(opts),
		create.NewCommand(opts),
		update.NewCommand(opts),
		deletecmd.NewCommand(opts),
		scale.NewCommand(opts),
		restart.NewCommand(opts),
		logs.NewCommand(opts),
		portforward.NewCommand(opts),
		credentialscmd.NewCommand(opts),
		backup.NewCommand(opts),
		explain.NewCommand(opts.RESTConfig),
		newVersionCommand(),
	)

	return cmd
}
