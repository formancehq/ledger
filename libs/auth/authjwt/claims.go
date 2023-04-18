package authjwt

import (
	"net/http"

	"github.com/golang-jwt/jwt"
	"github.com/pkg/errors"
)

type ClaimLedger struct {
	Name string `json:"name"`
	Slug string `json:"slug"`
}

type ClaimOrganization struct {
	Name    string        `json:"name"`
	ID      string        `json:"id"`
	Ledgers []ClaimLedger `json:"ledgers"`
}

type ClaimStruct struct {
	IDUser                 string              `json:"idUser"`
	IDOrganization         string              `json:"idOrganization"`
	Email                  string              `json:"email"`
	StytchID               string              `json:"stytchId"`
	AuthenticationStrategy string              `json:"authenticationStrategy"`
	Organizations          []ClaimOrganization `json:"organizations"`
}

func (c ClaimStruct) Valid() error {
	panic("implement me")
}

func ClaimsFromString(token string) (*ClaimStruct, error) {
	claims := &ClaimStruct{}
	_, _, err := new(jwt.Parser).ParseUnverified(token, claims)
	if err != nil {
		return nil, errors.Wrap(err, "parsing jwt token")
	}
	return claims, nil
}

func ClaimsFromHeader(header string) (*ClaimStruct, error) {
	prefix := len("bearer ")
	if len(header) < prefix {
		return nil, errors.New("malformed header")
	}
	token := header[prefix:]
	return ClaimsFromString(token)
}

func ClaimsFromRequest(req *http.Request) (*ClaimStruct, error) {
	return ClaimsFromHeader(req.Header.Get("Authorization"))
}
