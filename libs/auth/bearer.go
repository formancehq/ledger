package auth

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"github.com/formancehq/stack/libs/go-libs/oauth2/oauth2introspect"
	"github.com/golang-jwt/jwt"
	"github.com/pkg/errors"
)

type validator interface {
	Validate(ctx context.Context, token string) error
}
type validatorFn func(ctx context.Context, token string) error

func (fn validatorFn) Validate(ctx context.Context, token string) error {
	return fn(ctx, token)
}

var NoOpValidator = validatorFn(func(ctx context.Context, token string) error {
	return nil
})

type AudienceValidator interface {
	Validate(context.Context, string) bool
}
type AudienceValidatorFn func(context.Context, string) bool

func (fn AudienceValidatorFn) Validate(ctx context.Context, v string) bool {
	return fn(ctx, v)
}

var NoAudienceValidation = AudienceValidatorFn(func(ctx context.Context, v string) bool {
	return true
})

func AudienceIn(audiences ...string) AudienceValidatorFn {
	return func(ctx context.Context, s string) bool {
		for _, a := range audiences {
			if s == a {
				return true
			}
		}
		return false
	}
}

type introspectionValidator struct {
	introspecter      *oauth2introspect.Introspecter
	audiencesWildcard bool
	audienceValidator AudienceValidator
}

func (v *introspectionValidator) Validate(ctx context.Context, token string) error {
	active, err := v.introspecter.Introspect(ctx, token)
	if err != nil {
		return err
	}
	if !active {
		return errors.New("invalid token")
	}

	if v.audiencesWildcard {
		return nil
	}

	claims := jwt.MapClaims{}
	_, _, err = (&jwt.Parser{}).ParseUnverified(token, &claims)
	if err != nil {
		return err
	}

	tokenAudience := claims["aud"]
	if tokenAudience == nil {
		return errors.New("no audience provided in token")
	}
	switch aud := tokenAudience.(type) {
	case string:
		if !v.audienceValidator.Validate(ctx, aud) {
			return errors.New("audience mismatch")
		}
	case []any:
		match := false
		for _, aud := range aud {
			if v.audienceValidator.Validate(ctx, aud.(string)) {
				match = true
				break
			}
		}
		if !match {
			return errors.New("audience mismatch")
		}
	default:
		return fmt.Errorf("invalid audience property type, got %T", tokenAudience)
	}
	return nil
}

func NewIntrospectionValidator(introspecter *oauth2introspect.Introspecter, audiencesWildcard bool, audienceValidator AudienceValidator) *introspectionValidator {
	return &introspectionValidator{
		introspecter:      introspecter,
		audiencesWildcard: audiencesWildcard,
		audienceValidator: audienceValidator,
	}
}

type oauth2Agent struct {
	claims jwt.MapClaims
}

func (o oauth2Agent) GetScopes() []string {
	scopeClaim, ok := o.claims["scope"]
	if !ok {
		return []string{}
	}
	scopeClaimAsString, ok := scopeClaim.(string)
	if !ok {
		return []string{}
	}
	return strings.Split(scopeClaimAsString, " ")
}

type Oauth2BearerMethod struct {
	validator validator
}

func (h Oauth2BearerMethod) IsMatching(c *http.Request) bool {
	return strings.HasPrefix(
		strings.ToLower(c.Header.Get("Authorization")),
		"bearer",
	)
}

func (h *Oauth2BearerMethod) Check(c *http.Request) (Agent, error) {
	token := c.Header.Get("Authorization")[len("bearer "):]
	err := h.validator.Validate(c.Context(), token)
	if err != nil {
		return nil, err
	}
	claims := jwt.MapClaims{}
	_, _, err = new(jwt.Parser).ParseUnverified(token, &claims)
	if err != nil {
		return nil, err
	}
	return &oauth2Agent{
		claims: claims,
	}, nil
}

var _ Method = &Oauth2BearerMethod{}

func NewHttpBearerMethod(validator validator) *Oauth2BearerMethod {
	return &Oauth2BearerMethod{
		validator: validator,
	}
}
