package v2

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEncodeDSNPassword(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "no special chars",
			input:    "postgres://user:simple@host:5432/db",
			expected: "postgres://user:simple@host:5432/db",
		},
		{
			name:     "password with pipe and question mark",
			input:    "postgres://formance:YCA[sRR-~X]Pqdv|Ms3?hzc0u#f_@host:5432/db",
			expected: "postgres://formance:YCA%5BsRR-~X%5DPqdv%7CMs3%3Fhzc0u%23f_@host:5432/db",
		},
		{
			name:     "keyword value format unchanged",
			input:    "host=localhost user=admin password=secret dbname=mydb",
			expected: "host=localhost user=admin password=secret dbname=mydb",
		},
		{
			name:     "no password",
			input:    "postgres://user@host:5432/db",
			expected: "postgres://user@host:5432/db",
		},
		{
			name:     "no credentials",
			input:    "postgres://host:5432/db",
			expected: "postgres://host:5432/db",
		},
		{
			name:     "postgresql scheme",
			input:    "postgresql://user:p@ss|word@host:5432/db",
			expected: "postgresql://user:p%40ss%7Cword@host:5432/db",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.expected, encodeDSNPassword(tt.input))
		})
	}
}
