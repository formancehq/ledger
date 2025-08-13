package auth

import (
	"github.com/spf13/cobra"
	flag "github.com/spf13/pflag"
	"go.uber.org/fx"
)

const (
	AuthEnabledFlag              = "auth-enabled"
	AuthIssuerFlag               = "auth-issuer"
	AuthReadKeySetMaxRetriesFlag = "auth-read-key-set-max-retries"
	AuthCheckScopesFlag          = "auth-check-scopes"
	AuthServiceFlag              = "auth-service"
)

func AddFlags(flags *flag.FlagSet) {
	flags.Bool(AuthEnabledFlag, false, "Enable auth")
	flags.String(AuthIssuerFlag, "", "Issuer")
	flags.Int(AuthReadKeySetMaxRetriesFlag, 10, "ReadKeySetMaxRetries")
	flags.Bool(AuthCheckScopesFlag, false, "CheckScopes")
	flags.String(AuthServiceFlag, "", "Service")
}

func FXModuleFromFlags(cmd *cobra.Command) fx.Option {
	authEnabled, _ := cmd.Flags().GetBool(AuthEnabledFlag)
	authIssuer, _ := cmd.Flags().GetString(AuthIssuerFlag)
	authReadKeySetMaxRetries, _ := cmd.Flags().GetInt(AuthReadKeySetMaxRetriesFlag)
	authCheckScopes, _ := cmd.Flags().GetBool(AuthCheckScopesFlag)
	authService, _ := cmd.Flags().GetString(AuthServiceFlag)

	return Module(ModuleConfig{
		Enabled:              authEnabled,
		Issuer:               authIssuer,
		ReadKeySetMaxRetries: authReadKeySetMaxRetries,
		CheckScopes:          authCheckScopes,
		Service:              authService,
	})
}
