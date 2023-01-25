package cmd

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"runtime/debug"

	"github.com/formancehq/fctl/cmd/auth"
	"github.com/formancehq/fctl/cmd/cloud"
	"github.com/formancehq/fctl/cmd/ledger"
	"github.com/formancehq/fctl/cmd/orchestration"
	"github.com/formancehq/fctl/cmd/payments"
	"github.com/formancehq/fctl/cmd/profiles"
	"github.com/formancehq/fctl/cmd/search"
	"github.com/formancehq/fctl/cmd/stack"
	"github.com/formancehq/fctl/cmd/wallets"
	"github.com/formancehq/fctl/cmd/webhooks"
	fctl "github.com/formancehq/fctl/pkg"
	"github.com/formancehq/formance-sdk-go"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
)

const (
	MaxVersionShift = 2
)

func NewRootCommand() *cobra.Command {
	homedir, err := os.UserHomeDir()
	if err != nil {
		panic(err)
	}

	cmd := fctl.NewCommand("fctl",
		fctl.WithSilenceError(),
		fctl.WithShortDescription("Formance Control CLI"),
		fctl.WithSilenceUsage(),
		fctl.WithChildCommands(
			NewUICommand(),
			NewVersionCommand(),
			NewLoginCommand(),
			NewPromptCommand(),
			ledger.NewCommand(),
			payments.NewCommand(),
			profiles.NewCommand(),
			stack.NewCommand(),
			auth.NewCommand(),
			cloud.NewCommand(),
			search.NewCommand(),
			webhooks.NewCommand(),
			wallets.NewCommand(),
			orchestration.NewCommand(),
		),
		fctl.WithPersistentStringPFlag(fctl.ProfileFlag, "p", "", "config profile to use"),
		fctl.WithPersistentStringPFlag(fctl.FileFlag, "c", fmt.Sprintf("%s/.formance/fctl.config", homedir), "Debug mode"),
		fctl.WithPersistentBoolPFlag(fctl.DebugFlag, "d", false, "Debug mode"),
		fctl.WithPersistentBoolFlag(fctl.InsecureTlsFlag, false, "Insecure TLS"),
	)
	//cmd.PersistentPreRunE = func(cmd *cobra.Command, args []string) error {
	//
	//	cfg, err := fctl.GetConfig(cmd)
	//	if err != nil {
	//		return err
	//	}
	//
	//	profile := fctl.GetCurrentProfile(cmd, cfg)
	//	httpClient := fctl.GetHttpClient(cmd)
	//	configuration := membershipclient.NewConfiguration()
	//	configuration.HTTPClient = httpClient
	//	configuration.Servers[0].URL = profile.GetMembershipURI()
	//	client := membershipclient.NewAPIClient(configuration)
	//	serverInfo, _, err := client.DefaultApi.GetServerInfo(cmd.Context()).Execute()
	//	if err != nil {
	//		return err
	//	}
	//
	//	serverVersion, err := semver.NewVersion(serverInfo.Version)
	//	if err != nil {
	//		pterm.Warning.Printf("Server version is not semver, skip version checks: %s\r\n", err)
	//		return nil
	//	}
	//	fctlVersion, err := semver.NewVersion(Version)
	//	if err != nil {
	//		pterm.Warning.Printf("FCTL version is not semver, skip version checks: %s\r\n", err)
	//		return nil
	//	}
	//
	//	if serverVersion.Major() != fctlVersion.Major() {
	//		return errors.New("you use an incompatible version of FCTL, please upgrade\r\n")
	//	}
	//	if serverVersion.Minor()-fctlVersion.Minor() >= MaxVersionShift {
	//		return errors.New("")
	//	}
	//	return nil
	//}
	return cmd
}

func Execute() {
	defer func() {
		if e := recover(); e != nil {
			fctl.Error(os.Stderr, "%s", e)
			debug.PrintStack()
		}
	}()

	ctx, _ := signal.NotifyContext(context.TODO(), os.Interrupt)
	err := NewRootCommand().ExecuteContext(ctx)
	if err != nil {
		switch {
		case errors.Is(err, fctl.ErrMissingApproval):
			fctl.Error(os.Stderr, "Command aborted as you didn't approve.")
			os.Exit(1)
		case formance.ExtractOpenAPIErrorMessage(err) != nil:
			fctl.Error(os.Stderr, formance.ExtractOpenAPIErrorMessage(err).Error())
			os.Exit(2)
		default:
			fctl.Error(os.Stderr, err.Error())
			os.Exit(255)
		}
	}
}
