package wasm

import (
	"fmt"
	"io"
	"os"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
	wasmpb "github.com/formancehq/ledger/v3/internal/proto/wasmpb"
)

func newUploadCommand() *cobra.Command {
	cmd := &cobra.Command{Use: "upload <wasm-file>", Args: cobra.ExactArgs(1), RunE: runUpload}
	cmd.Flags().String("config", "", "Path to the module configuration JSON")

	return cmd
}

func runUpload(cmd *cobra.Command, args []string) error {
	module, err := os.Open(args[0])
	if err != nil {
		return fmt.Errorf("open WASM module: %w", err)
	}
	defer func() { _ = module.Close() }()
	var config []byte
	if path, _ := cmd.Flags().GetString("config"); path != "" {
		config, err = os.ReadFile(path)
		if err != nil {
			return fmt.Errorf("read config: %w", err)
		}
	}
	_, conn, err := cmdutil.GetClient(cmd)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	ctx, cancel := cmdutil.GetContext(cmd)
	defer cancel()
	stream, err := wasmpb.NewWasmServiceClient(conn).Upload(ctx)
	if err != nil {
		return err
	}
	const chunkSize = 1 << 20
	buf := make([]byte, chunkSize)
	first := true
	for {
		n, readErr := module.Read(buf)
		if n > 0 {
			request := &wasmpb.UploadWasmRequest{Chunk: append([]byte(nil), buf[:n]...)}
			if first {
				request.Config = config
				first = false
			}
			if err := stream.Send(request); err != nil {
				return err
			}
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return fmt.Errorf("read WASM module: %w", readErr)
		}
	}
	response, err := stream.CloseAndRecv()
	if err != nil {
		return err
	}
	pterm.Printf("Uploaded WASM module %s\n", response.GetModuleId())

	return nil
}
