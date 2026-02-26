package auth

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestHasScope(t *testing.T) {
	t.Parallel()

	scopes := []string{"ledger:read", "ledger:write"}

	assert.True(t, HasScope(scopes, ScopeRead, "ledger"))
	assert.True(t, HasScope(scopes, ScopeWrite, "ledger"))
	assert.False(t, HasScope(scopes, ScopeAdmin, "ledger"))
	assert.False(t, HasScope(scopes, ScopeRead, "other"))
}

func TestHasScopeCustomService(t *testing.T) {
	t.Parallel()

	scopes := []string{"myservice:read"}
	assert.True(t, HasScope(scopes, ScopeRead, "myservice"))
	assert.False(t, HasScope(scopes, ScopeRead, "ledger"))
}

func TestScopeWithService(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "ledger:read", ScopeRead.WithService(""))
	assert.Equal(t, "ledger:read", ScopeRead.WithService("ledger"))
	assert.Equal(t, "custom:read", ScopeRead.WithService("custom"))
	assert.Equal(t, "custom:write", ScopeWrite.WithService("custom"))
	assert.Equal(t, "custom:admin", ScopeAdmin.WithService("custom"))
}
