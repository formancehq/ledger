package cmdutil

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

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
