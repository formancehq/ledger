package auth

import (
	"slices"
	"strings"
)

// Scope represents an authorization scope for the ledger service.
type Scope string

var (
	ScopeRead  Scope = "ledger:read"
	ScopeWrite Scope = "ledger:write"
	ScopeAdmin Scope = "ledger:admin"
)

// WithService returns the scope as a string, including the service prefix.
func (s Scope) WithService(service string) string {
	// Default scope uses "ledger" prefix. If the service name differs,
	// replace the prefix.
	if service == "" || service == "ledger" {
		return string(s)
	}
	_, suffix, _ := strings.Cut(string(s), ":")
	return service + ":" + suffix
}

// HasScope checks if a slice of scope strings contains the given scope.
func HasScope(scopes []string, scope Scope, service string) bool {
	return slices.Contains(scopes, scope.WithService(service))
}
