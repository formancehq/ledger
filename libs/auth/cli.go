package auth

import (
	flag "github.com/spf13/pflag"
	"github.com/spf13/viper"
	"go.uber.org/fx"
)

const (
	AuthEnabled                  = "auth-enabled"
	AuthIssuerFlag               = "auth-issuer"
	AuthReadKeySetMaxRetriesFlag = "auth-read-key-set-max-retries"
	AuthCheckScopesFlag          = "auth-check-scopes"
	AuthServiceFlag              = "auth-service"
)

func InitAuthFlags(flags *flag.FlagSet) {
	flags.Bool(AuthEnabled, false, "Enable auth")
	flags.String(AuthIssuerFlag, "", "Issuer")
	flags.Int(AuthReadKeySetMaxRetriesFlag, 10, "ReadKeySetMaxRetries")
	flags.Bool(AuthCheckScopesFlag, false, "CheckScopes")
	flags.String(AuthServiceFlag, "", "Service")
}

func CLIAuthModule(v *viper.Viper) fx.Option {
	return Module(ModuleConfig{
		Enabled:              v.GetBool(AuthEnabled),
		Issuer:               v.GetString(AuthIssuerFlag),
		ReadKeySetMaxRetries: v.GetInt(AuthReadKeySetMaxRetriesFlag),
		CheckScopes:          v.GetBool(AuthCheckScopesFlag),
		Service:              v.GetString(AuthServiceFlag),
	})
}
