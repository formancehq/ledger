package restore

import (
	"fmt"

	"github.com/spf13/cobra"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger/v3/internal/proto/restorepb"
)

func NewCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "restore",
		Short: "Backup restore operations",
		Long:  "Upload, validate, preview, and finalize backup restores on a server running in --restore mode",
	}

	cmd.AddCommand(NewDownloadCommand())
	cmd.AddCommand(NewValidateCommand())
	cmd.AddCommand(NewPreviewCommand())
	cmd.AddCommand(NewFinalizeCommand())

	return cmd
}

// getRestoreClient creates a gRPC client connection and returns the RestoreService client.
func getRestoreClient(cmd *cobra.Command) (restorepb.RestoreServiceClient, *grpc.ClientConn, error) {
	serverAddr, _ := cmd.Flags().GetString("server")

	// Share the root command's credential resolution so restore honors the same
	// --insecure / --tls-ca-cert / --tls-server-name persistent flags (and their
	// mutual-exclusion guard) as every other command. The restore server is
	// commonly dialed from inside a pod, where --tls-server-name is needed to
	// match a cert whose SANs cover only the in-cluster DNS names.
	creds, err := cmdutil.GetClientTransportCredentials(cmd)
	if err != nil {
		return nil, nil, err
	}

	conn, err := grpc.NewClient(serverAddr,
		grpc.WithTransportCredentials(creds),
		grpc.WithStatsHandler(otelgrpc.NewClientHandler()), // Emit a client span per RPC and propagate W3C trace context.
	)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to connect to gRPC server: %w", err)
	}

	return restorepb.NewRestoreServiceClient(conn), conn, nil
}
