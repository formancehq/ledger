package cmdutil

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWrapText(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		text      string
		maxWidth  int
		separator string
		expected  []string
	}{
		{
			name:      "short text no wrap",
			text:      "users:bank",
			maxWidth:  20,
			separator: ":",
			expected:  []string{"users:bank"},
		},
		{
			name:      "exact fit",
			text:      "users:bank",
			maxWidth:  10,
			separator: ":",
			expected:  []string{"users:bank"},
		},
		{
			name:      "wrap at separator",
			text:      "users:bank:payments:eu:france",
			maxWidth:  20,
			separator: ":",
			expected:  []string{"users:bank:payments:", "eu:france"},
		},
		{
			name:      "multiple wraps",
			text:      "users:bank:payments:eu:france:paris:12345678",
			maxWidth:  20,
			separator: ":",
			expected:  []string{"users:bank:payments:", "eu:france:paris:", "12345678"},
		},
		{
			name:      "hard break no separator found",
			text:      "abcdefghijklmnopqrstuvwxyz",
			maxWidth:  10,
			separator: ":",
			expected:  []string{"abcdefghij", "klmnopqrst", "uvwxyz"},
		},
		{
			name:      "empty separator hard break",
			text:      "abcdefghijklmno",
			maxWidth:  10,
			separator: "",
			expected:  []string{"abcdefghij", "klmno"},
		},
		{
			name:      "zero max width returns as-is",
			text:      "anything",
			maxWidth:  0,
			separator: ":",
			expected:  []string{"anything"},
		},
		{
			name:      "empty text",
			text:      "",
			maxWidth:  10,
			separator: ":",
			expected:  []string{""},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.expected, WrapText(tt.text, tt.maxWidth, tt.separator))
		})
	}
}

func TestObfuscateDSN(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "postgres with password",
			input:    "postgres://user:secret@host:5432/db",
			expected: "postgres://user:****@host:5432/db",
		},
		{
			name:     "postgresql scheme",
			input:    "postgresql://admin:p4ssw0rd@myhost:5432/mydb?sslmode=require",
			expected: "postgresql://admin:****@myhost:5432/mydb?sslmode=require",
		},
		{
			name:     "clickhouse with password",
			input:    "clickhouse://default:secret@ch-host:9000/events",
			expected: "clickhouse://default:****@ch-host:9000/events",
		},
		{
			name:     "password with special chars",
			input:    "postgres://formance:YCA[sRR-~X]Pqdv|Ms3?hzc0u#f_@host:5432/db",
			expected: "postgres://formance:****@host:5432/db",
		},
		{
			name:     "no password",
			input:    "postgres://user@host:5432/db",
			expected: "postgres://user@host:5432/db",
		},
		{
			name:     "no scheme",
			input:    "host=localhost user=admin password=secret dbname=mydb",
			expected: "host=localhost user=admin password=secret dbname=mydb",
		},
		{
			name:     "no credentials",
			input:    "postgres://host:5432/db",
			expected: "postgres://host:5432/db",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.expected, ObfuscateDSN(tt.input))
		})
	}
}

func TestObfuscateClickHouseDSN(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "query password",
			input:    "clickhouse://ch-host:9000/events?username=default&password=secret",
			expected: "clickhouse://ch-host:9000/events?password=****&username=default",
		},
		{
			name:     "userinfo and query password",
			input:    "clickhouse://default:userinfo-secret@ch-host:9000/events?password=query-secret",
			expected: "clickhouse://default:****@ch-host:9000/events?password=****",
		},
		{
			name:     "unrelated query parameter",
			input:    "clickhouse://ch-host:9000/events?secure=true&token=public-routing-value",
			expected: "clickhouse://ch-host:9000/events?secure=true&token=public-routing-value",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.expected, ObfuscateClickHouseDSN(tt.input))
		})
	}
}

func TestObfuscateURLPassword(t *testing.T) {
	t.Parallel()

	assert.Equal(t,
		"https://operator:****@example.com/hooks",
		ObfuscateURLPassword("https://operator:secret@example.com/hooks"),
	)
	assert.Equal(t,
		"https://operator@example.com/hooks",
		ObfuscateURLPassword("https://operator@example.com/hooks"),
	)
}

func TestObfuscateURLUserinfo(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "username and password",
			input:    "nats://operator:secret@host:4222",
			expected: "nats://operator:****@host:4222",
		},
		{
			name:     "token-only userinfo",
			input:    "nats://secret-token@host:4222",
			expected: "nats://****@host:4222",
		},
		{
			name:     "escaped password",
			input:    "https://operator:p%40ss@example.com/hooks",
			expected: "https://operator:****@example.com/hooks",
		},
		{
			name:     "at sign in path",
			input:    "https://example.com/hooks@v2",
			expected: "https://example.com/hooks@v2",
		},
		{
			name:     "no credentials",
			input:    "nats://host:4222",
			expected: "nats://host:4222",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.expected, ObfuscateURLUserinfo(tt.input))
		})
	}
}
