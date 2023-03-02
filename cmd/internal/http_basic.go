package internal

import (
	"net/http"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

const (
	serverHttpBasicAuthFlag  = "server.http.basic_auth"
	authBasicEnabledFlag     = "auth-basic-enabled"
	authBasicCredentialsFlag = "auth-basic-credentials"
)

type Credential struct {
	Password string
}
type Credentials map[string]Credential

func HTTPBasicAuthMethod(v *viper.Viper) func(http.Handler) http.Handler {
	basicAuth := v.GetStringSlice(serverHttpBasicAuthFlag)
	if len(basicAuth) == 0 {
		basicAuth = v.GetStringSlice(authBasicCredentialsFlag)
	}
	if len(basicAuth) > 0 &&
		(!v.IsSet(authBasicEnabledFlag) || v.GetBool(authBasicEnabledFlag)) { // Keep compatibility, we disable the feature only if the flag is explicitely set to false
		credentials := Credentials{}
		for _, kv := range basicAuth {
			parts := strings.SplitN(kv, ":", 2)
			credentials[parts[0]] = Credential{
				Password: parts[1],
			}
		}
		return func(handler http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				// TODO: Add integration test on basic auth
				username, password, ok := r.BasicAuth()
				if !ok {
					w.WriteHeader(http.StatusUnauthorized)
					return
				}

				credential, ok := credentials[username]
				if !ok || credential.Password != password {
					w.WriteHeader(http.StatusForbidden)
					return
				}

				handler.ServeHTTP(w, r)
			})
		}
	}
	return nil
}

func InitHTTPBasicFlags(cmd *cobra.Command) {
	cmd.PersistentFlags().Bool(authBasicEnabledFlag, false, "Enable basic auth")
	cmd.PersistentFlags().StringSlice(authBasicCredentialsFlag, []string{}, "HTTP basic auth credentials (<username>:<password>)")
	cmd.PersistentFlags().String(serverHttpBasicAuthFlag, "", "Http basic auth")
}
