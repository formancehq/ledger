package auth

import (
	"net/http"
	"strings"

	"github.com/pkg/errors"
)

type Credential struct {
	Scopes   []string
	Password string
}

func (c Credential) GetScopes() []string {
	return c.Scopes
}

type Credentials map[string]Credential

type HttpBasicMethod struct {
	credentials Credentials
}

func (h HttpBasicMethod) IsMatching(c *http.Request) bool {
	return strings.HasPrefix(
		strings.ToLower(c.Header.Get("Authorization")),
		"basic",
	)
}

func (h HttpBasicMethod) Check(c *http.Request) (Agent, error) {
	username, password, ok := c.BasicAuth()
	if !ok {
		return nil, errors.New("malformed basic")
	}
	if username == "" {
		return nil, errors.New("malformed basic")
	}
	credential, ok := h.credentials[username]
	if !ok {
		return nil, errors.New("invalid credentials")
	}
	if credential.Password != password {
		return nil, errors.New("invalid credentials")
	}
	return credential, nil
}

func NewHTTPBasicMethod(credentials Credentials) *HttpBasicMethod {
	return &HttpBasicMethod{
		credentials: credentials,
	}
}

var _ Method = &HttpBasicMethod{}
