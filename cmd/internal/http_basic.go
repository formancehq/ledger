package internal

import (
	"strings"

	"github.com/formancehq/go-libs/auth"
	"github.com/numary/ledger/pkg/api/routes"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

const (
	serverHttpBasicAuthFlag  = "server.http.basic_auth"
	authBasicEnabledFlag     = "auth-basic-enabled"
	authBasicCredentialsFlag = "auth-basic-credentials"
)

func HTTPBasicAuthMethod(v *viper.Viper) auth.Method {
	basicAuth := v.GetStringSlice(serverHttpBasicAuthFlag)
	if len(basicAuth) == 0 {
		basicAuth = v.GetStringSlice(authBasicCredentialsFlag)
	}
	if len(basicAuth) > 0 &&
		(!v.IsSet(authBasicEnabledFlag) || v.GetBool(authBasicEnabledFlag)) { // Keep compatibility, we disable the feature only if the flag is explicitely set to false
		credentials := auth.Credentials{}
		for _, kv := range basicAuth {
			parts := strings.SplitN(kv, ":", 2)
			credentials[parts[0]] = auth.Credential{
				Password: parts[1],
				Scopes:   routes.AllScopes,
			}
		}
		return auth.NewHTTPBasicMethod(credentials)
	}
	return nil
}

func InitHTTPBasicFlags(cmd *cobra.Command) {
	cmd.PersistentFlags().Bool(authBasicEnabledFlag, false, "Enable basic auth")
	cmd.PersistentFlags().StringSlice(authBasicCredentialsFlag, []string{}, "HTTP basic auth credentials (<username>:<password>)")
	cmd.PersistentFlags().String(serverHttpBasicAuthFlag, "", "Http basic auth")
}
