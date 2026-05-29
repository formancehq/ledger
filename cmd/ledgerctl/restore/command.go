package restore

import (
	"crypto/tls"
	"fmt"

	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

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
	insecureMode, _ := cmd.Flags().GetBool("insecure")

	var creds credentials.TransportCredentials
	if insecureMode {
		creds = insecure.NewCredentials()
	} else {
		creds = credentials.NewTLS(&tls.Config{
			MinVersion: tls.VersionTLS12,
		})
	}

	conn, err := grpc.NewClient(serverAddr,
		grpc.WithTransportCredentials(creds),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to connect to gRPC server: %w", err)
	}

	return restorepb.NewRestoreServiceClient(conn), conn, nil
}
