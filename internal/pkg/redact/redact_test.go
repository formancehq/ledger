package redact

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSecretIsIdempotent(t *testing.T) {
	t.Parallel()

	for _, value := range []string{"", "plaintext-secret", SecretSet, SecretNone} {
		t.Run(value, func(t *testing.T) {
			t.Parallel()

			once := Secret(value)
			assert.Equal(t, once, Secret(once))
		})
	}
}

func TestDSNRedactsCredentials(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    string
		secrets  []string
		expected string
	}{
		{
			name:     "userinfo password",
			input:    "clickhouse://operator:supersecret@host:9000/ledger",
			secrets:  []string{"supersecret"},
			expected: "clickhouse://operator:****@host:9000/ledger",
		},
		{
			name:     "query password",
			input:    "clickhouse://host:9000/ledger?username=operator&password=supersecret",
			secrets:  []string{"supersecret"},
			expected: "clickhouse://host:9000/ledger?password=****&username=operator",
		},
		{
			name:     "userinfo username and query password",
			input:    "clickhouse://operator@host:9000/ledger?password=supersecret",
			secrets:  []string{"supersecret"},
			expected: "clickhouse://operator@host:9000/ledger?password=****",
		},
		{
			name:     "userinfo and query passwords",
			input:    "clickhouse://operator:userinfo-secret@host:9000/ledger?password=query-secret",
			secrets:  []string{"userinfo-secret", "query-secret"},
			expected: "clickhouse://operator:****@host:9000/ledger?password=****",
		},
		{
			name:     "at sign inside query password",
			input:    "clickhouse://host:9000/ledger?password=p%40ss",
			secrets:  []string{"p%40ss", "p@ss"},
			expected: "clickhouse://host:9000/ledger?password=****",
		},
		{
			name:     "explicit empty password",
			input:    "clickhouse://operator:@host:9000/ledger?password=",
			expected: "clickhouse://operator:@host:9000/ledger?password=",
		},
		{
			name:     "other credential query keys",
			input:    "clickhouse://host:9000/ledger?access-token=a&api_key=b&client-secret=c&token=d",
			secrets:  []string{"=a", "=b", "=c", "=d"},
			expected: "clickhouse://host:9000/ledger?access-token=****&api_key=****&client-secret=****&token=****",
		},
		{
			name:     "malformed URL fails closed",
			input:    "clickhouse://operator:secret%zz@host:9000/ledger",
			secrets:  []string{"secret"},
			expected: SecretSet,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			got := DSN(test.input)
			assert.Equal(t, test.expected, got)
			for _, secret := range test.secrets {
				assert.NotContains(t, got, secret)
			}
			assert.Equal(t, got, DSN(got))
		})
	}
}

func TestURLUserinfoRedactsPasswordsAndTokens(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    string
		secret   string
		expected string
	}{
		{
			name:     "username and password",
			input:    "nats://operator:password-secret@host:4222",
			secret:   "password-secret",
			expected: "nats://operator:****@host:4222",
		},
		{
			name:     "token",
			input:    "nats://token-secret@host:4222",
			secret:   "token-secret",
			expected: "nats://****@host:4222",
		},
		{
			name:     "HTTP basic auth",
			input:    "https://operator:http-secret@example.com/hooks",
			secret:   "http-secret",
			expected: "https://operator:****@example.com/hooks",
		},
		{
			name:     "at sign in path is not userinfo",
			input:    "https://example.com/hooks@v2",
			expected: "https://example.com/hooks@v2",
		},
		{
			name:     "explicit empty password",
			input:    "nats://operator:@host:4222",
			expected: "nats://operator:@host:4222",
		},
		{
			name:     "malformed URL fails closed",
			input:    "nats://token%zz@host:4222",
			secret:   "token",
			expected: SecretSet,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			got := URLUserinfo(test.input)
			assert.Equal(t, test.expected, got)
			if test.secret != "" {
				assert.NotContains(t, got, test.secret)
			}
			assert.Equal(t, got, URLUserinfo(got))
		})
	}
}
