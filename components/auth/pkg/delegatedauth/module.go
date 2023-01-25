package delegatedauth

import (
	"net/http"

	"github.com/zitadel/oidc/pkg/client/rp"
	"go.uber.org/fx"
)

func Module() fx.Option {
	return fx.Options(
		fx.Provide(func(cfg Config, httpClient *http.Client) (rp.RelyingParty, error) {
			return rp.NewRelyingPartyOIDC(cfg.Issuer, cfg.ClientID, cfg.ClientSecret, cfg.RedirectURL, []string{"openid email"},
				rp.WithHTTPClient(httpClient))
		}),
	)
}
