package oidc

import (
	"context"
	"crypto/rsa"
	"errors"
	"fmt"
	"time"

	auth "github.com/formancehq/auth/pkg"
	"github.com/formancehq/auth/pkg/storage"
	"github.com/google/uuid"
	"github.com/zitadel/oidc/pkg/client/rp"
	"github.com/zitadel/oidc/pkg/oidc"
	"github.com/zitadel/oidc/pkg/op"
	"golang.org/x/text/language"
	"gopkg.in/square/go-jose.v2"
)

const (
	ExpirationToken2Legged = time.Hour
	ExpirationToken3Legged = 5 * time.Minute
)

type Storage interface {
	SaveAuthRequest(ctx context.Context, request auth.AuthRequest) error
	FindAuthRequest(ctx context.Context, id string) (*auth.AuthRequest, error)
	FindAuthRequestByCode(ctx context.Context, id string) (*auth.AuthRequest, error)
	UpdateAuthRequest(ctx context.Context, request auth.AuthRequest) error
	UpdateAuthRequestCode(ctx context.Context, id string, code string) error
	DeleteAuthRequest(ctx context.Context, id string) error

	SaveRefreshToken(ctx context.Context, token auth.RefreshToken) error
	FindRefreshToken(ctx context.Context, token string) (*auth.RefreshToken, error)
	DeleteRefreshToken(ctx context.Context, token string) error

	SaveAccessToken(ctx context.Context, token auth.AccessToken) error
	FindAccessToken(ctx context.Context, token string) (*auth.AccessToken, error)
	DeleteAccessToken(ctx context.Context, token string) error
	DeleteAccessTokensForUserAndClient(ctx context.Context, userID string, clientID string) error
	DeleteAccessTokensByRefreshToken(ctx context.Context, token string) error

	FindUser(ctx context.Context, id string) (*auth.User, error)
	FindUserBySubject(ctx context.Context, subject string) (*auth.User, error)
	SaveUser(ctx context.Context, user auth.User) error

	FindClient(ctx context.Context, id string) (*auth.Client, error)
	FindTransientScopes(ctx context.Context, id string) ([]auth.Scope, error)
	FindTransientScopesByLabel(ctx context.Context, label string) ([]auth.Scope, error)
}

type signingKey struct {
	ID        string
	Algorithm string
	Key       *rsa.PrivateKey
}

type Service struct {
	Keys map[string]*rsa.PublicKey
}

// getInfoFromRequest returns the clientID, authTime and amr depending on the op.TokenRequest type / implementation
func getInfoFromRequest(req op.TokenRequest) (clientID string, authTime time.Time, amr []string) {
	authReq, ok := req.(*auth.AuthRequest) //Code Flow (with scope offline_access)
	if ok {
		return authReq.ApplicationID, authReq.AuthTime, authReq.GetAMR()
	}
	refreshReq, ok := req.(*auth.RefreshTokenRequest) //Refresh Token Request
	if ok {
		return refreshReq.ApplicationID, refreshReq.AuthTime, refreshReq.AMR
	}
	return "", time.Time{}, nil
}

// Go workspaces force to use the same set of dependencies of other projects
// FCTL use a forked version og the oidc library
// We need to refine this forked version to make these methods optional
type storageFacade struct {
	Storage
	signingKey    signingKey
	relyingParty  rp.RelyingParty
	staticClients []auth.StaticClient
}

func (s *storageFacade) CreateDeviceCode(ctx context.Context, id string, scopes []string, interval, expiresIn int) (op.DeviceCode, error) {
	panic("not implemented")
}

func (s *storageFacade) DeviceCodeByUserCode(ctx context.Context, code string) (op.DeviceCode, error) {
	panic("not implemented")
}

func (s *storageFacade) DeviceCodeByDeviceCode(ctx context.Context, code string) (op.DeviceCode, error) {
	panic("not implemented")
}

func (s *storageFacade) DeleteDeviceCode(ctx context.Context, id string) error {
	panic("not implemented")
}

func (s *storageFacade) UpdateDeviceCodeLastCheck(ctx context.Context, code op.DeviceCode, now time.Time) error {
	panic("not implemented")
}

// CreateAuthRequest implements the op.Storage interface
// it will be called after parsing and validation of the authentication request
func (s *storageFacade) CreateAuthRequest(ctx context.Context, authReq *oidc.AuthRequest, userID string) (op.AuthRequest, error) {
	request := auth.AuthRequest{
		CreatedAt:     time.Now(),
		ApplicationID: authReq.ClientID,
		CallbackURI:   authReq.RedirectURI,
		TransferState: authReq.State,
		Prompt:        auth.PromptToInternal(authReq.Prompt),
		UiLocales:     auth.Array[language.Tag](authReq.UILocales),
		LoginHint:     authReq.LoginHint,
		MaxAuthAge:    auth.MaxAgeToInternal(authReq.MaxAge),
		Scopes:        auth.Array[string](authReq.Scopes),
		ResponseType:  authReq.ResponseType,
		Nonce:         authReq.Nonce,
		CodeChallenge: &auth.OIDCCodeChallenge{
			Challenge: authReq.CodeChallenge,
			Method:    string(authReq.CodeChallengeMethod),
		},
		ID: uuid.NewString(),
	}

	if err := s.Storage.SaveAuthRequest(ctx, request); err != nil {
		return nil, err
	}

	return &request, nil
}

// AuthRequestByCode implements the op.Storage interface
// it will be called after parsing and validation of the token request (in an authorization code flow)
func (s *storageFacade) AuthRequestByCode(ctx context.Context, code string) (op.AuthRequest, error) {
	return s.FindAuthRequestByCode(ctx, code)
}

// SaveAuthCode implements the op.Storage interface
// it will be called after the authentication has been successful and before redirecting the user agent to the redirect_uri
// (in an authorization code flow)
func (s *storageFacade) SaveAuthCode(ctx context.Context, id string, code string) error {
	return s.Storage.UpdateAuthRequestCode(ctx, id, code)
}

// CreateAccessToken implements the op.Storage interface
// it will be called for all requests able to return an access token (Authorization Code Flow, Implicit Flow, JWT Profile, ...)
func (s *storageFacade) CreateAccessToken(ctx context.Context, request op.TokenRequest) (string, time.Time, error) {
	var applicationID string
	//if authenticated for an app (auth code / implicit flow) we must save the client_id to the token
	authReq, ok := request.(*auth.AuthRequest)
	if ok {
		applicationID = authReq.ApplicationID
	}
	token, err := s.saveAccessToken(ctx, nil, applicationID, request.GetSubject(), request.GetAudience(), request.GetScopes())
	if err != nil {
		return "", time.Time{}, err
	}
	return token.ID, token.Expiration, nil
}

// CreateAccessAndRefreshTokens implements the op.Storage interface
// it will be called for all requests able to return an access and refresh token (Authorization Code Flow, Refresh Token Request)
func (s *storageFacade) CreateAccessAndRefreshTokens(ctx context.Context, request op.TokenRequest, currentRefreshToken string) (accessTokenID string, newRefreshToken string, expiration time.Time, err error) {
	//get the information depending on the request type / implementation
	applicationID, authTime, amr := getInfoFromRequest(request)

	//if currentRefreshToken is empty (Code Flow) we will have to create a new refresh token
	if currentRefreshToken == "" {
		refreshToken, err := s.createRefreshToken(ctx, applicationID, request.GetSubject(), request.GetAudience(), request.GetScopes(), amr, authTime)
		if err != nil {
			return "", "", time.Time{}, err
		}
		accessToken, err := s.saveAccessToken(ctx, refreshToken, applicationID, request.GetSubject(),
			request.GetAudience(), request.GetScopes())
		if err != nil {
			return "", "", time.Time{}, err
		}
		return accessToken.ID, refreshToken.ID, accessToken.Expiration, nil
	}

	//if we get here, the currentRefreshToken was not empty, so the call is a refresh token request
	//we therefore will have to check the currentRefreshToken and renew the refresh token
	refreshToken, err := s.renewRefreshToken(ctx, currentRefreshToken)
	if err != nil {
		return "", "", time.Time{}, err
	}
	accessToken, err := s.saveAccessToken(ctx, refreshToken, applicationID, request.GetSubject(), request.GetAudience(), request.GetScopes())
	if err != nil {
		return "", "", time.Time{}, err
	}
	return accessToken.ID, refreshToken.ID, accessToken.Expiration, nil
}

// TokenRequestByRefreshToken implements the op.Storage interface
// it will be called after parsing and validation of the refresh token request
func (s *storageFacade) TokenRequestByRefreshToken(ctx context.Context, refreshToken string) (op.RefreshTokenRequest, error) {
	token, err := s.Storage.FindRefreshToken(ctx, refreshToken)
	if err != nil {
		return nil, fmt.Errorf("invalid refresh_token")
	}
	return auth.NewRefreshTokenRequest(*token), nil
}

// TerminateSession implements the op.Storage interface
// it will be called after the user signed out, therefore the access and refresh token of the user of this client must be removed
func (s *storageFacade) TerminateSession(ctx context.Context, userID string, clientID string) error {
	return s.Storage.DeleteAccessTokensForUserAndClient(ctx, userID, clientID)
}

// RevokeToken implements the op.Storage interface
// it will be called after parsing and validation of the token revocation request
func (s *storageFacade) RevokeToken(ctx context.Context, tokenStr string, userID string, clientID string) *oidc.Error {

	accessToken, err := s.Storage.FindAccessToken(ctx, tokenStr)
	if storage.IgnoreNotFoundError(err) != nil {
		return oidc.ErrServerError().WithDescription(err.Error())
	}
	if err == nil {
		if accessToken.ApplicationID != clientID {
			return oidc.ErrInvalidClient().WithDescription("token was not issued for this client")
		}
		if err := s.Storage.DeleteAccessToken(ctx, tokenStr); err != nil {
			return oidc.ErrServerError().WithDescription(err.Error())
		}
		return nil
	}

	refreshToken, err := s.Storage.FindRefreshToken(ctx, tokenStr)
	if storage.IgnoreNotFoundError(err) != nil {
		return oidc.ErrServerError().WithDescription(err.Error())
	}
	if err == nil {
		if refreshToken.ApplicationID != clientID {
			return oidc.ErrInvalidClient().WithDescription("token was not issued for this client")
		}
		if err := s.Storage.DeleteRefreshToken(ctx, tokenStr); err != nil {
			return oidc.ErrServerError().WithDescription(err.Error())
		}
		if err := s.Storage.DeleteAccessTokensByRefreshToken(ctx, tokenStr); err != nil {
			return oidc.ErrServerError().WithDescription(err.Error())
		}
		return nil
	}
	return nil
}

// GetSigningKey implements the op.Storage interface
// it will be called when creating the OpenID Provider
func (s *storageFacade) GetSigningKey(ctx context.Context, keyCh chan<- jose.SigningKey) {
	//in this example the signing key is a static rsa.PrivateKey and the algorithm used is RS256
	//you would obviously have a more complex implementation and store / retrieve the key from your database as well
	//
	//the idea of the signing key channel is, that you can (with what ever mechanism) rotate your signing key and
	//switch the key of the signer via this channel
	keyCh <- jose.SigningKey{
		Algorithm: jose.SignatureAlgorithm(s.signingKey.Algorithm), //always tell the signer with algorithm to use
		Key: jose.JSONWebKey{
			KeyID: s.signingKey.ID, //always give the key an id so, that it will include it in the token header as `kid` claim
			Key:   s.signingKey.Key,
		},
	}
}

// GetKeySet implements the op.Storage interface
// it will be called to get the current (public) keys, among others for the keys_endpoint or for validating access_tokens on the userinfo_endpoint, ...
func (s *storageFacade) GetKeySet(ctx context.Context) (*jose.JSONWebKeySet, error) {
	//as mentioned above, this example only has a single signing key without key rotation,
	//so it will directly use its public key
	//
	//when using key rotation you typically would store the public keys alongside the private keys in your database
	//and give both of them an expiration date, with the public key having a longer lifetime (e.g. rotate private key every
	return &jose.JSONWebKeySet{Keys: []jose.JSONWebKey{
		{
			KeyID:     s.signingKey.ID,
			Algorithm: s.signingKey.Algorithm,
			Use:       oidc.KeyUseSignature,
			Key:       &s.signingKey.Key.PublicKey,
		}},
	}, nil
}

func (s *storageFacade) findClient(ctx context.Context, clientID string) (ClientWithSecrets, error) {
	var client *auth.Client
	for _, staticClient := range s.staticClients {
		if staticClient.Id == clientID {
			return &staticClient, nil
		}
	}
	if client == nil {
		var err error
		client, err = s.Storage.FindClient(ctx, clientID)
		if err != nil {
			return nil, err
		}
	}
	return client, nil
}

// GetClientByClientID implements the op.Storage interface
// it will be called whenever information (type, redirect_uris, ...) about the client behind the client_id is needed
func (s *storageFacade) GetClientByClientID(ctx context.Context, clientID string) (op.Client, error) {
	client, err := s.findClient(ctx, clientID)
	if err != nil {
		return nil, err
	}
	return NewClientFacade(client, s.relyingParty), nil
}

// AuthorizeClientIDSecret implements the op.Storage interface
// it will be called for validating the client_id, client_secret on token or introspection requests
func (s *storageFacade) AuthorizeClientIDSecret(ctx context.Context, clientID, clientSecret string) error {
	client, err := s.findClient(ctx, clientID)
	if err != nil {
		return err
	}
	return client.ValidateSecret(clientSecret)
}

// SetUserinfoFromScopes implements the op.Storage interface
// it will be called for the creation of an id_token, so we'll just pass it to the private function without any further check
func (s *storageFacade) SetUserinfoFromScopes(ctx context.Context, userinfo oidc.UserInfoSetter, userID, clientID string, scopes []string) error {
	return s.setUserinfo(ctx, userinfo, userID, clientID, scopes)
}

// SetUserinfoFromToken implements the op.Storage interface
// it will be called for the userinfo endpoint, so we read the token and pass the information from that to the private function
func (s *storageFacade) SetUserinfoFromToken(ctx context.Context, userinfo oidc.UserInfoSetter, tokenID, subject, origin string) error {
	token, err := s.Storage.FindAccessToken(ctx, tokenID)
	if err != nil {
		return err
	}
	return s.setUserinfo(ctx, userinfo, token.UserID, token.ApplicationID, token.Scopes)
}

// SetIntrospectionFromToken implements the op.Storage interface
// it will be called for the introspection endpoint, so we read the token and pass the information from that to the private function
func (s *storageFacade) SetIntrospectionFromToken(ctx context.Context, introspection oidc.IntrospectionResponse, tokenID, subject, clientID string) error {
	token, err := s.Storage.FindAccessToken(ctx, tokenID)
	if err != nil {
		return err
	}
	ok := false
	for _, aud := range token.Audience {
		if aud == clientID {
			ok = true
			break
		}
	}
	if !ok {
		client, err := s.findClient(ctx, clientID)
		if err != nil {
			return err
		}
		ok = client.IsTrusted()
	}

	if !ok {
		return fmt.Errorf("token is not valid for this client")
	}

	if err := s.setUserinfo(ctx, introspection, subject, clientID, token.Scopes); err != nil {
		return err
	}
	introspection.SetScopes(token.Scopes)
	introspection.SetClientID(token.ApplicationID)
	return nil
}

// GetPrivateClaimsFromScopes implements the op.Storage interface
// it will be called for the creation of a JWT access token to assert claims for custom scopes
func (s *storageFacade) GetPrivateClaimsFromScopes(ctx context.Context, userID, clientID string, scopes []string) (claims map[string]interface{}, err error) {
	return claims, nil
}

// GetKeyByIDAndUserID implements the op.Storage interface
// it will be called to validate the signatures of a JWT (JWT Profile Grant and Authentication)
func (s *storageFacade) GetKeyByIDAndUserID(ctx context.Context, keyID, userID string) (*jose.JSONWebKey, error) {
	return nil, errors.New("not supported")
}

// ValidateJWTProfileScopes implements the op.Storage interface
// it will be called to validate the scopes of a JWT Profile Authorization Grant request
func (s *storageFacade) ValidateJWTProfileScopes(ctx context.Context, userID string, scopes []string) ([]string, error) {
	allowedScopes := make([]string, 0)
	for _, scope := range scopes {
		if scope == oidc.ScopeOpenID || scope == oidc.ScopeEmail {
			allowedScopes = append(allowedScopes, scope)
		}
	}
	return allowedScopes, nil
}

// Health implements the op.Storage interface
func (s *storageFacade) Health(ctx context.Context) error {
	return nil
}

// createRefreshToken will store a refresh_token in-memory based on the provided information
func (s *storageFacade) createRefreshToken(ctx context.Context, applicationID string, subject string,
	audience []string, scopes []string, amr []string, authTime time.Time) (*auth.RefreshToken, error) {
	token := auth.RefreshToken{
		ID:            uuid.NewString(),
		AuthTime:      authTime,
		AMR:           amr,
		ApplicationID: applicationID,
		UserID:        subject,
		Audience:      audience,
		Expiration:    time.Now().Add(5 * time.Hour),
		Scopes:        scopes,
	}
	if err := s.Storage.SaveRefreshToken(ctx, token); err != nil {
		return nil, err
	}
	return &token, nil
}

// renewRefreshToken checks the provided refresh_token and creates a new one based on the current
func (s *storageFacade) renewRefreshToken(ctx context.Context, currentRefreshToken string) (*auth.RefreshToken, error) {
	refreshToken, err := s.Storage.FindRefreshToken(ctx, currentRefreshToken)
	if err != nil {
		return nil, err
	}
	//deletes the refresh token and all access tokens which were issued based on this refresh token
	if err := s.Storage.DeleteRefreshToken(ctx, currentRefreshToken); err != nil {
		return nil, err
	}
	if err := s.Storage.DeleteAccessTokensByRefreshToken(ctx, currentRefreshToken); err != nil {
		return nil, err
	}
	//creates a new refresh token based on the current one
	refreshToken.ID = uuid.NewString()

	if err := s.SaveRefreshToken(ctx, *refreshToken); err != nil {
		return nil, err
	}

	return refreshToken, nil
}

// accessToken will store an access_token in-memory based on the provided information
func (s *storageFacade) saveAccessToken(ctx context.Context, refreshToken *auth.RefreshToken, applicationId, subject string, audience, scopes []string) (*auth.AccessToken, error) {

	expiration := ExpirationToken2Legged
	if subject != "" {
		expiration = ExpirationToken3Legged
	}

	token := auth.AccessToken{
		ID:            uuid.NewString(),
		ApplicationID: applicationId,
		UserID:        subject,
		Audience:      audience,
		Expiration:    time.Now().Add(expiration),
		Scopes:        scopes,
		RefreshTokenID: func() string {
			if refreshToken == nil {
				return ""
			}
			return refreshToken.ID
		}(),
	}
	if err := s.Storage.SaveAccessToken(ctx, token); err != nil {
		return nil, err
	}
	return &token, nil
}

// setUserinfo sets the info based on the user, scopes and if necessary the clientID
func (s *storageFacade) setUserinfo(ctx context.Context, userInfo oidc.UserInfoSetter, userID, clientID string, scopes []string) (err error) {

	user, err := s.Storage.FindUser(ctx, userID)
	if err != nil {
		return err
	}

	for _, scope := range scopes {
		switch scope {
		case oidc.ScopeOpenID:
			userInfo.SetSubject(userID)
		case oidc.ScopeEmail:
			userInfo.SetEmail(user.Email, true) // TODO: Get the information
		case oidc.ScopeProfile:
			// TODO: Support that
		case oidc.ScopePhone:
			// TODO: Support that ?
		}
	}
	return nil
}

func (i *storageFacade) AuthRequestByID(ctx context.Context, id string) (op.AuthRequest, error) {
	return i.FindAuthRequest(ctx, id)
}

func (s *storageFacade) ClientCredentialsTokenRequest(ctx context.Context, clientID string, scopes []string) (op.TokenRequest, error) {

	client, err := s.findClient(ctx, clientID)
	if err != nil {
		return nil, err
	}

	allowedScopes := auth.Array[string]{}
	verifiedScopes := make(map[string]any)
	scopesToCheck := client.GetScopes()

l:
	for _, scope := range scopes {
		for {
			if len(scopesToCheck) == 0 {
				break l
			}
			clientScope := scopesToCheck[0]
			if len(scopesToCheck) == 1 {
				scopesToCheck = make([]string, 0)
			} else {
				scopesToCheck = scopesToCheck[1:]
			}
			if clientScope == scope {
				allowedScopes = append(allowedScopes, scope)
				continue l
			}
			verifiedScopes[clientScope] = struct{}{}

			scopes, err := s.Storage.FindTransientScopesByLabel(ctx, clientScope)
			if err != nil {
				return nil, err
			}

			scopeLabels := make([]string, 0)
			for _, scope := range scopes {
				scopeLabels = append(scopeLabels, scope.Label)
			}

			scopesToCheck = append(scopesToCheck, scopeLabels...)
		}
	}
	return &auth.AuthRequest{
		ID:            uuid.NewString(),
		CreatedAt:     time.Now(),
		ApplicationID: clientID,
		Scopes:        allowedScopes,
	}, nil
}

var _ op.Storage = (*storageFacade)(nil)

func NewStorageFacade(storage Storage, rp rp.RelyingParty, privateKey *rsa.PrivateKey, staticClients ...auth.StaticClient) *storageFacade {
	return &storageFacade{
		Storage: storage,
		signingKey: signingKey{
			ID:        "id",
			Algorithm: "RS256",
			Key:       privateKey,
		},
		relyingParty:  rp,
		staticClients: staticClients,
	}
}
