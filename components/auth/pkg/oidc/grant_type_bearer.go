package oidc

import (
	"context"
	"fmt"
	"net/http"
	"time"

	httphelper "github.com/zitadel/oidc/pkg/http"
	"github.com/zitadel/oidc/pkg/oidc"
	"github.com/zitadel/oidc/pkg/op"
	"gopkg.in/square/go-jose.v2"
)

type openIDKeySet struct {
	jose.JSONWebKeySet
}

// VerifySignature implements the oidc.KeySet interface
// providing an implementation for the keys stored in the OP Storage interface
func (o *openIDKeySet) VerifySignature(ctx context.Context, jws *jose.JSONWebSignature) ([]byte, error) {
	keyID, alg := oidc.GetKeyIDAndAlg(jws)

	var (
		key jose.JSONWebKey
		err error
	)
	key, err = oidc.FindMatchingKey(keyID, oidc.KeyUseSignature, alg, o.JSONWebKeySet.Keys...)
	if err != nil {
		return nil, fmt.Errorf("invalid signature: %w", err)
	}

	return jws.Verify(&key)
}

func VerifyJWTAssertion(ctx context.Context, assertion string, v JWTProfileVerifier) (*oidc.JWTTokenRequest, error) {
	request := new(oidc.JWTTokenRequest)

	_, err := oidc.ParseToken(assertion, request)
	if err != nil {
		return nil, err
	}

	if err := oidc.CheckAudience(request, v.Issuer()); err != nil {
		return nil, err
	}

	if err := oidc.CheckExpiration(request, v.Offset()); err != nil {
		return nil, err
	}

	accessTokenVerifier := op.NewAccessTokenVerifier(v.DelegatedIssuer(), &openIDKeySet{
		v.JSONWebKeySet(),
	})
	if _, err := op.VerifyAccessToken(ctx, assertion, accessTokenVerifier); err != nil {
		return nil, err
	}

	return request, nil
}

type JWTProfileVerifier interface {
	oidc.Verifier
	DelegatedIssuer() string
	JSONWebKeySet() jose.JSONWebKeySet
}

type JWTAuthorizationGrantExchanger interface {
	op.Exchanger
	JWTProfileVerifier() JWTProfileVerifier
}

func grantTypeBearer(p JWTAuthorizationGrantExchanger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		profileRequest, err := op.ParseJWTProfileGrantRequest(r, p.Decoder())
		if err != nil {
			op.RequestError(w, r, err)
			return
		}

		clientID, _, ok := r.BasicAuth()
		if !ok {
			op.RequestError(w, r, oidc.ErrInvalidClient().WithDescription("missing authentication").WithParent(err))
			return
		}

		client, err := p.Storage().GetClientByClientID(r.Context(), clientID)
		if err != nil {
			op.RequestError(w, r, err)
			return
		}

		tokenRequest, err := VerifyJWTAssertion(r.Context(), profileRequest.Assertion, p.JWTProfileVerifier())
		if err != nil {
			op.RequestError(w, r, err)
			return
		}

		tokenRequest.Scopes, err = p.Storage().ValidateJWTProfileScopes(r.Context(), tokenRequest.Issuer, profileRequest.Scope)
		if err != nil {
			op.RequestError(w, r, err)
			return
		}
		resp, err := CreateJWTTokenResponse(r.Context(), tokenRequest, p, client)
		if err != nil {
			op.RequestError(w, r, err)
			return
		}
		httphelper.MarshalJSON(w, resp)
	}
}

func CreateJWTTokenResponse(ctx context.Context, tokenRequest *oidc.JWTTokenRequest, creator op.TokenCreator, client op.Client) (*oidc.AccessTokenResponse, error) {
	id, exp, err := creator.Storage().CreateAccessToken(ctx, tokenRequest)
	if err != nil {
		return nil, err
	}

	tokenRequest.Audience = oidc.Audience{}
	accessToken, err := op.CreateJWT(ctx, creator.Issuer(), tokenRequest, exp, id,
		creator.Signer(), client, creator.Storage())
	if err != nil {
		return nil, err
	}

	return &oidc.AccessTokenResponse{
		AccessToken: accessToken,
		TokenType:   oidc.BearerToken,
		ExpiresIn:   uint64(exp.Sub(time.Now().UTC()).Seconds()),
	}, nil
}
