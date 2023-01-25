package oidc

import (
	"time"

	"github.com/formancehq/auth/pkg/delegatedauth"
	"github.com/zitadel/oidc/pkg/client/rp"
	"github.com/zitadel/oidc/pkg/oidc"
	"github.com/zitadel/oidc/pkg/op"
)

type Client interface {
	GetID() string
	GetRedirectURIs() []string
	GetPostLogoutRedirectUris() []string
	IsPublic() bool
	GetScopes() []string
}

type ClientWithSecrets interface {
	Client
	ValidateSecret(secret string) error
	IsTrusted() bool
}

type clientFacade struct {
	Client       Client
	relyingParty rp.RelyingParty
}

func NewClientFacade(client Client, relyingParty rp.RelyingParty) *clientFacade {
	return &clientFacade{
		Client:       client,
		relyingParty: relyingParty,
	}
}

// GetID must return the client_id
func (c *clientFacade) GetID() string {
	return c.Client.GetID()
}

// RedirectURIs must return the registered redirect_uris for Code and Implicit Flow
func (c *clientFacade) RedirectURIs() []string {
	return c.Client.GetRedirectURIs()
}

// PostLogoutRedirectURIs must return the registered post_logout_redirect_uris for sign-outs
func (c *clientFacade) PostLogoutRedirectURIs() []string {
	return c.Client.GetPostLogoutRedirectUris()
}

// ApplicationType must return the type of the client (app, native, user agent)
func (c *clientFacade) ApplicationType() op.ApplicationType {
	return op.ApplicationTypeWeb
}

// AuthMethod must return the authentication method (client_secret_basic, client_secret_post, none, private_key_jwt)
func (c *clientFacade) AuthMethod() oidc.AuthMethod {
	authMethod := oidc.AuthMethodNone
	if !c.Client.IsPublic() {
		authMethod = oidc.AuthMethodBasic
	}
	return authMethod
}

// ResponseTypes must return all allowed response types (code, id_token token, id_token)
// these must match with the allowed grant types
func (c *clientFacade) ResponseTypes() []oidc.ResponseType {
	return []oidc.ResponseType{oidc.ResponseTypeCode}
}

// GrantTypes must return all allowed grant types (authorization_code, refresh_token, urn:ietf:params:oauth:grant-type:jwt-bearer)
func (c *clientFacade) GrantTypes() []oidc.GrantType {
	grantTypes := []oidc.GrantType{
		oidc.GrantTypeCode,
		oidc.GrantTypeRefreshToken,
	}
	if !c.Client.IsPublic() {
		grantTypes = append(grantTypes, oidc.GrantTypeClientCredentials)
	}
	return grantTypes
}

// LoginURL will be called to redirect the user (agent) to the login UI
// you could implement some logic here to redirect the users to different login UIs depending on the client
func (c *clientFacade) LoginURL(t op.LoginType, id string) string {
	return rp.AuthURL(delegatedauth.DelegatedState{
		AuthRequestID: id,
	}.EncodeAsUrlParam(), c.relyingParty)
}

// AccessTokenType must return the type of access token the client uses (Bearer (opaque) or JWT)
func (c *clientFacade) AccessTokenType() op.AccessTokenType {
	return op.AccessTokenTypeJWT
}

// IDTokenLifetime must return the lifetime of the client's id_tokens
func (c *clientFacade) IDTokenLifetime() time.Duration {
	return 1 * time.Hour
}

// DevMode enables the use of non-compliant configs such as redirect_uris (e.g. http schema for user agent client)
func (c *clientFacade) DevMode() bool {
	return false
}

// RestrictAdditionalIdTokenScopes allows specifying which custom scopes shall be asserted into the id_token
func (c *clientFacade) RestrictAdditionalIdTokenScopes() func(scopes []string) []string {
	return func(scopes []string) []string {
		return scopes
	}
}

// RestrictAdditionalAccessTokenScopes allows specifying which custom scopes shall be asserted into the JWT access_token
func (c *clientFacade) RestrictAdditionalAccessTokenScopes() func(scopes []string) []string {
	return func(scopes []string) []string {
		return scopes
	}
}

// IsScopeAllowed enables Client specific custom scopes validation
func (c *clientFacade) IsScopeAllowed(label string) bool {
	for _, scope := range c.Client.GetScopes() {
		if scope == label {
			return true
		}
	}
	return false
}

// IDTokenUserinfoClaimsAssertion allows specifying if claims of scope profile, email, phone and address are asserted into the id_token
// even if an access token if issued which violates the OIDC Core spec
// (5.4. Requesting Claims using Scope Values: https://openid.net/specs/openid-connect-core-1_0.html#ScopeClaims)
// some clients though require that e.g. email is always in the id_token when requested even if an access_token is issued
func (c *clientFacade) IDTokenUserinfoClaimsAssertion() bool {
	return false
}

// ClockSkew enables clients to instruct the OP to apply a clock skew on the various times and expirations
// (subtract from issued_at, add to expiration, ...)
func (c *clientFacade) ClockSkew() time.Duration {
	return 0
}
