package fctl

import (
	"github.com/spf13/cobra"
	"github.com/zitadel/oidc/pkg/client/rp"
)

func GetAuthRelyingParty(cmd *cobra.Command, membershipURI string) (rp.RelyingParty, error) {
	return rp.NewRelyingPartyOIDC(membershipURI, AuthClient, "",
		"", []string{"openid", "email", "offline_access", "supertoken"}, rp.WithHTTPClient(GetHttpClient(cmd)))
}
