package main

import (
	"crypto/tls"
	"fmt"

	"github.com/formancehq/ledger-v3-poc/internal/proto/restorepb"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

// newRestoreCommand creates the restore parent command.
func newRestoreCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "restore",
		Short: "Backup restore operations",
		Long:  "Upload, validate, preview, and finalize backup restores on a server running in --restore mode",
	}

	cmd.AddCommand(newRestoreUploadCommand())
	cmd.AddCommand(newRestoreValidateCommand())
	cmd.AddCommand(newRestorePreviewCommand())
	cmd.AddCommand(newRestoreFinalizeCommand())

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
