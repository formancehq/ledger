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
	"golang.org/x/sync/errgroup"
	"net/http"
	"os"
	"strings"
)

var (
	rootCmd = &cobra.Command{
		Use:          "generator <ledger-url> <script-location>",
		Short:        "Generate data for a ledger. WARNING: This is an experimental tool.",
		RunE:         run,
		Args:         cobra.ExactArgs(2),
		SilenceUsage: true,
	}
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

	httpClient := &http.Client{
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
			Client(context.WithValue(cmd.Context(), oauth2.HTTPClient, httpClient))
	}

	client := ledgerclient.New(
		ledgerclient.WithServerURL(ledgerUrl),
		ledgerclient.WithClient(httpClient),
	)

	logging.FromContext(cmd.Context()).Infof("Creating ledger '%s' if not exists", targetedLedger)
	_, err = client.Ledger.V2.CreateLedger(cmd.Context(), operations.V2CreateLedgerRequest{
		Ledger: targetedLedger,
		V2CreateLedgerRequest: &components.V2CreateLedgerRequest{
			Bucket:   &ledgerBucket,
			Metadata: ledgerMetadata,
			Features: ledgerFeatures,
		},
	})
	if err != nil {
		sdkError := &sdkerrors.V2ErrorResponse{}
		if !errors.As(err, &sdkError) || (sdkError.ErrorCode != components.V2ErrorsEnumLedgerAlreadyExists &&
			sdkError.ErrorCode != components.V2ErrorsEnumValidation) {
			return fmt.Errorf("failed to create ledger: %w", err)
		}
	}

	parallelContext, cancel := context.WithCancel(cmd.Context())
	defer cancel()

	errGroup, ctx := errgroup.WithContext(parallelContext)

	logging.FromContext(cmd.Context()).Infof("Starting to generate data with %d vus", vus)

	for vu := 0; vu < vus; vu++ {
		generator, err := generate.NewGenerator(string(fileContent), generate.WithGlobals(map[string]any{
			"vu": vu,
		}))
		if err != nil {
			return fmt.Errorf("failed to create generator: %w", err)
		}

		errGroup.Go(func() error {
			defer cancel()

			iteration := 0

			for {
				logging.FromContext(ctx).Infof("Run iteration %d/%d", vu, iteration)
				action, err := generator.Next(vu)
				if err != nil {
					return fmt.Errorf("iteration %d/%d failed: %w", vu, iteration, err)
				}

				ret, err := action.Apply(ctx, client.Ledger.V2, targetedLedger)
				if err != nil {
					if errors.Is(err, context.Canceled) {
						return nil
					}
					return fmt.Errorf("iteration %d/%d failed: %w", vu, iteration, err)
				}
				if untilLogID != 0 && ret.GetLogID() >= untilLogID {
					return nil
				}
				iteration++
			}
		})
	}

	return errGroup.Wait()
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
