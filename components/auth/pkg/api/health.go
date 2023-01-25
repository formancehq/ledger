package api

import (
	"context"
	"net/http"

	"github.com/formancehq/go-libs/health"
	"github.com/zitadel/oidc/pkg/client"
	"github.com/zitadel/oidc/pkg/client/rp"
)

func delegatedOIDCServerAvailability(rp rp.RelyingParty) health.NamedCheck {
	return health.NewNamedCheck("Delegated OIDC server", health.CheckFn(func(ctx context.Context) error {
		_, err := client.Discover(rp.Issuer(), http.DefaultClient)
		return err
	}))
}
