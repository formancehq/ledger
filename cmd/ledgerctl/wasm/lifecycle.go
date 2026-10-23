package wasm

import (
	"fmt"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
	wasmpb "github.com/formancehq/ledger/v3/internal/proto/wasmpb"
)

func newListCommand() *cobra.Command {
	return &cobra.Command{Use: "list", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, _ []string) error {
		client, conn, err := wasmClient(cmd)
		if err != nil {
			return err
		}
		defer func() { _ = conn.Close() }()
		ctx, cancel := cmdutil.GetContext(cmd)
		defer cancel()
		response, err := client.List(ctx, &wasmpb.ListWasmModulesRequest{})
		if err != nil {
			return err
		}
		for _, module := range response.GetModules() {
			pterm.Printf("%s\t%s\t%d bytes\n", module.GetModuleId(), module.GetStatus().String(), module.GetSize())
		}

		return nil
	}}
}

func newStopCommand() *cobra.Command {
	return &cobra.Command{Use: "stop <module-id>", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		client, conn, err := wasmClient(cmd)
		if err != nil {
			return err
		}
		defer func() { _ = conn.Close() }()
		ctx, cancel := cmdutil.GetContext(cmd)
		defer cancel()
		_, err = client.Stop(ctx, &wasmpb.StopWasmModuleRequest{ModuleId: args[0]})

		return err
	}}
}

func newGetCommand() *cobra.Command {
	return &cobra.Command{Use: "get <module-id>", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		client, conn, err := wasmClient(cmd)
		if err != nil {
			return err
		}
		defer func() { _ = conn.Close() }()
		ctx, cancel := cmdutil.GetContext(cmd)
		defer cancel()
		module, err := client.Get(ctx, &wasmpb.GetWasmModuleRequest{ModuleId: args[0]})
		if err != nil {
			return err
		}
		pterm.Printf("%s\t%s\t%d bytes\n", module.GetModuleId(), module.GetStatus().String(), module.GetSize())
		if module.GetLastError() != "" {
			pterm.Printf("error: %s\n", module.GetLastError())
		}

		return nil
	}}
}

func newDeleteCommand() *cobra.Command {
	return &cobra.Command{Use: "delete <module-id>", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		client, conn, err := wasmClient(cmd)
		if err != nil {
			return err
		}
		defer func() { _ = conn.Close() }()
		ctx, cancel := cmdutil.GetContext(cmd)
		defer cancel()
		_, err = client.Delete(ctx, &wasmpb.DeleteWasmModuleRequest{ModuleId: args[0]})

		return err
	}}
}

func wasmClient(cmd *cobra.Command) (wasmpb.WasmServiceClient, interface{ Close() error }, error) {
	_, conn, err := cmdutil.GetClient(cmd)
	if err != nil {
		return nil, nil, fmt.Errorf("connect to Ledger: %w", err)
	}

	return wasmpb.NewWasmServiceClient(conn), conn, nil
}
