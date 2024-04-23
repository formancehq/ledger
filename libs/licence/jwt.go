package licence

import (
	"context"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/lestrrat-go/jwx/jwk"
	"github.com/pkg/errors"
)

func (l *Licence) getKey(issuer, kid string) (interface{}, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	set, err := jwk.Fetch(ctx, issuer)
	if err != nil {
		return nil, err
	}

	key, ok := set.LookupKeyID(kid)
	if !ok {
		return nil, errors.Wrap(jwt.ErrInvalidKey, "key not found")
	}

	var rawKey interface{}
	if err := key.Raw(&rawKey); err != nil {
		return nil, errors.Wrap(err, "failed to get raw key")
	}

	return rawKey, nil
}

func (l *Licence) validate() error {
	parser := jwt.NewParser(
		jwt.WithAudience(l.serviceName),
		jwt.WithExpirationRequired(),
		jwt.WithSubject(l.clusterID),
		jwt.WithIssuer(l.expectedIssuer),
	)

	token, err := parser.Parse(l.jwtToken, func(token *jwt.Token) (interface{}, error) {
		kid, ok := token.Header["kid"].(string)
		if !ok {
			return nil, errors.Wrap(jwt.ErrTokenInvalidId, "missing kid")
		}

		issuer, ok := token.Claims.(jwt.MapClaims)["iss"].(string)
		if !ok {
			return nil, errors.Wrap(jwt.ErrTokenInvalidIssuer, "missing issuer")
		}

		return l.getKey(issuer, kid)
	})
	if err != nil {
		return err
	}

	if !token.Valid {
		return errors.New("token is not valid")
	}

	return nil
}
