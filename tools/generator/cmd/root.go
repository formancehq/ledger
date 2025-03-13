package cmd

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"github.com/formancehq/go-libs/v2/logging"
	ledgerclient "github.com/formancehq/ledger/pkg/client"
	"github.com/formancehq/ledger/pkg/client/models/components"
	"github.com/formancehq/ledger/pkg/client/models/operations"
	"github.com/formancehq/ledger/pkg/client/models/sdkerrors"
	"github.com/formancehq/ledger/pkg/generate"
	"github.com/spf13/cobra"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/clientcredentials"
	"net/http"
	"os"
	"strings"
)

const (
	parallelFlag           = "parallel"
	ledgerFlag             = "ledger"
	ledgerMetadataFlag     = "ledger-metadata"
	ledgerBucketFlag       = "ledger-bucket"
	ledgerFeatureFlag      = "ledger-feature"
	untilLogIDFlag         = "until-log-id"
	clientIDFlag           = "client-id"
	clientSecretFlag       = "client-secret"
	authUrlFlag            = "auth-url"
	insecureSkipVerifyFlag = "insecure-skip-verify"
	httpClientTimeoutFlag  = "http-client-timeout"
	debugFlag              = "debug"
)

var (
	rootCmd = &cobra.Command{
		Use:          "generator <ledger-url> <script-location>",
		Short:        "Generate data for a ledger. WARNING: This is an experimental tool.",
		RunE:         run,
		Args:         cobra.ExactArgs(2),
		SilenceUsage: true,
	}
)

func init() {
	rootCmd.Flags().String(clientIDFlag, "", "Client ID")
	rootCmd.Flags().String(clientSecretFlag, "", "Client Secret")
	rootCmd.Flags().String(authUrlFlag, "", "Auth URL")
	rootCmd.Flags().Bool(insecureSkipVerifyFlag, false, "Skip TLS verification")
	rootCmd.Flags().IntP(parallelFlag, "p", 1, "Number of parallel users")
	rootCmd.Flags().StringP(ledgerFlag, "l", "default", "Ledger to feed")
	rootCmd.Flags().Int64P(untilLogIDFlag, "u", 0, "Stop after this transaction ID")
	rootCmd.Flags().String(ledgerBucketFlag, "", "Ledger bucket")
	rootCmd.Flags().StringSlice(ledgerMetadataFlag, []string{}, "Ledger metadata")
	rootCmd.Flags().StringSlice(ledgerFeatureFlag, []string{}, "Ledger features")
	rootCmd.Flags().Duration(httpClientTimeoutFlag, 0, "HTTP client timeout (default: no timeout)")
	rootCmd.Flags().Bool(debugFlag, false, "Enable debug logging")

	rootCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}

func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

func run(cmd *cobra.Command, args []string) error {

	ledgerUrl := args[0]
	scriptLocation := args[1]

	fileContent, err := os.ReadFile(scriptLocation)
	if err != nil {
		return fmt.Errorf("failed to read file: %w", err)
	}

	vus, err := cmd.Flags().GetInt(parallelFlag)
	if err != nil {
		return fmt.Errorf("failed to get vu: %w", err)
	}

	targetedLedger, err := cmd.Flags().GetString(ledgerFlag)
	if err != nil {
		return fmt.Errorf("failed to get ledger: %w", err)
	}

	ledgerBucket, err := cmd.Flags().GetString(ledgerBucketFlag)
	if err != nil {
		return fmt.Errorf("failed to get ledger bucket: %w", err)
	}

	ledgerMetadata, err := extractSliceSliceFlag(cmd, ledgerMetadataFlag)
	if err != nil {
		return fmt.Errorf("failed to get ledger metadata: %w", err)
	}

	ledgerFeatures, err := extractSliceSliceFlag(cmd, ledgerFeatureFlag)
	if err != nil {
		return fmt.Errorf("failed to get ledger features: %w", err)
	}

	untilLogID, err := cmd.Flags().GetInt64(untilLogIDFlag)
	if err != nil {
		return fmt.Errorf("failed to get untilLogID: %w", err)
	}

	insecureSkipVerify, err := cmd.Flags().GetBool(insecureSkipVerifyFlag)
	if err != nil {
		return fmt.Errorf("failed to get insecureSkipVerify: %w", err)
	}

	httpClientTimeout, err := cmd.Flags().GetDuration(httpClientTimeoutFlag)
	if err != nil {
		return fmt.Errorf("failed to get http client timeout: %w", err)
	}

	debug, err := cmd.Flags().GetBool(debugFlag)
	if err != nil {
		return fmt.Errorf("failed to get debug: %w", err)
	}

	logger := logging.NewDefaultLogger(cmd.OutOrStdout(), debug, false, false)
	ctx := logging.ContextWithLogger(cmd.Context(), logger)

	httpClient := &http.Client{
		Timeout: httpClientTimeout,
		Transport: &http.Transport{
			MaxIdleConns:        vus,
			MaxConnsPerHost:     vus,
			MaxIdleConnsPerHost: vus,
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: insecureSkipVerify,
			},
		},
	}

	clientID, err := cmd.Flags().GetString(clientIDFlag)
	if err != nil {
		return fmt.Errorf("failed to get client id: %w", err)
	}
	if clientID != "" {
		clientSecret, err := cmd.Flags().GetString(clientSecretFlag)
		if err != nil {
			return fmt.Errorf("failed to get client secret: %w", err)
		}

		authUrl, err := cmd.Flags().GetString(authUrlFlag)
		if err != nil {
			return fmt.Errorf("failed to get auth url: %w", err)
		}

		httpClient = (&clientcredentials.Config{
			ClientID:     clientID,
			ClientSecret: clientSecret,
			TokenURL:     authUrl + "/oauth/token",
			Scopes:       []string{"ledger:read", "ledger:write"},
		}).
			Client(context.WithValue(ctx, oauth2.HTTPClient, httpClient))
	}

	client := ledgerclient.New(
		ledgerclient.WithServerURL(ledgerUrl),
		ledgerclient.WithClient(httpClient),
	)

	logging.FromContext(ctx).Infof("Creating ledger '%s' if not exists", targetedLedger)
	_, err = client.Ledger.V2.GetLedger(ctx, operations.V2GetLedgerRequest{
		Ledger: targetedLedger,
	})
	if err != nil {
		sdkError := &sdkerrors.V2ErrorResponse{}
		if !errors.As(err, &sdkError) || sdkError.ErrorCode != components.V2ErrorsEnumNotFound {
			return fmt.Errorf("failed to get ledger: %w", err)
		}
		_, err = client.Ledger.V2.CreateLedger(ctx, operations.V2CreateLedgerRequest{
			Ledger: targetedLedger,
			V2CreateLedgerRequest: components.V2CreateLedgerRequest{
				Bucket:   &ledgerBucket,
				Metadata: ledgerMetadata,
				Features: ledgerFeatures,
			},
		})
		if err != nil {
			if !errors.As(err, &sdkError) || (sdkError.ErrorCode != components.V2ErrorsEnumLedgerAlreadyExists &&
				sdkError.ErrorCode != components.V2ErrorsEnumValidation) {
				return fmt.Errorf("failed to create ledger: %w", err)
			}
		}
	}

	logger.Infof("Starting to generate data with %d vus", vus)

	return generate.
		NewGeneratorSet(vus, string(fileContent), targetedLedger, client, uint64(untilLogID)).
		Run(ctx)
}

func extractSliceSliceFlag(cmd *cobra.Command, flagName string) (map[string]string, error) {

	inputs, err := cmd.Flags().GetStringSlice(flagName)
	if err != nil {
		return nil, err
	}

	ret := make(map[string]string)

	for _, input := range inputs {
		parts := strings.SplitN(input, "=", 2)
		if len(parts) != 2 {
			return ret, fmt.Errorf("invalid metadata: %s", input)
		}

		ret[parts[0]] = parts[1]
	}

	return ret, nil
}
