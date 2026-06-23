package cmdutil

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestIntegrityResult exercises the verdict mapping directly: any integrity
// error count must produce a non-nil error so the CLI exits non-zero and a
// chain such as `restore validate && restore finalize` stops before finalizing.
func TestIntegrityResult(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		subject    string
		errorCount int
		wantErr    string
	}{
		{name: "no errors is valid", subject: "backup validation", errorCount: 0, wantErr: ""},
		{name: "one error fails", subject: "backup validation", errorCount: 1, wantErr: "backup validation failed: 1 integrity error(s)"},
		{name: "many errors fail", subject: "store validation", errorCount: 5, wantErr: "store validation failed: 5 integrity error(s)"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := IntegrityResult(tt.subject, tt.errorCount)
			if tt.wantErr != "" {
				require.EqualError(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
		})
	}
}
