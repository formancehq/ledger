package authjwt

import (
	"net/http"
	"strings"

	"github.com/golang-jwt/jwt"
	"github.com/pkg/errors"
)

var (
	ErrAuthorizationHeaderNotFound = errors.New("missing_authorization_header")
	ErrAccessDenied                = errors.New("access_denied")
)

// Deprecated: Use oauth2introspect package
func CheckTokenWithAuth(client *http.Client, authBaseUrl string, req *http.Request) error {
	token := req.Header.Get("Authorization")
	if !strings.HasPrefix(strings.ToLower(token), "bearer ") {
		return ErrAuthorizationHeaderNotFound
	}
	authUrl := authBaseUrl + "/authenticate/check"
	authRequest, err := http.NewRequest("GET", authUrl, nil)
	if err != nil {
		return errors.Wrap(err, "building request")
	}
	authRequest = authRequest.WithContext(req.Context())
	authRequest.Header.Add("Authorization", token)
	response, err := client.Do(authRequest)
	if err != nil {
		return errors.Wrap(err, "doing request")
	}
	if response.StatusCode != 200 {
		return ErrAccessDenied
	}
	return nil
}

// Deprecated: Use audience claim
func CheckLedgerAccess(req *http.Request, name string) error {
	jwtString := req.Header.Get("Authorization")
	if !strings.HasPrefix(strings.ToLower(jwtString), "bearer ") {
		return ErrAuthorizationHeaderNotFound
	}
	tokenString := jwtString[len("bearer "):]

	payload, _, err := new(jwt.Parser).ParseUnverified(tokenString, &ClaimStruct{})
	if err != nil {
		return errors.Wrap(err, "parsing jwt token")
	}
	for _, s := range payload.Claims.(*ClaimStruct).Organizations {
		for _, l := range s.Ledgers {
			if l.Slug == name {
				return nil
			}
		}
	}
	return ErrAccessDenied
}

// Deprecated: Use audience claim
func CheckOrganizationAccess(req *http.Request, id string) error {
	jwtString := req.Header.Get("Authorization")
	if !strings.HasPrefix(strings.ToLower(jwtString), "bearer ") {
		return ErrAuthorizationHeaderNotFound
	}
	tokenString := jwtString[len("bearer "):]

	payload, _, err := new(jwt.Parser).ParseUnverified(tokenString, &ClaimStruct{})
	if err != nil {
		return errors.Wrap(err, "parsing jwt token")
	}
	for _, s := range payload.Claims.(*ClaimStruct).Organizations {
		if s.ID == id {
			return nil
		}
	}
	return ErrAccessDenied
}
