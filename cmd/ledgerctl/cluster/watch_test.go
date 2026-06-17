package cluster

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const intervalErr = "--interval must be greater than 0"

// TestValidateInterval exercises the --interval guard directly. time.NewTicker
// panics on a non-positive duration, so the guard must reject 0 and negative
// values with a clean error while letting any positive value through.
func TestValidateInterval(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		interval time.Duration
		wantErr  bool
	}{
		{name: "zero is rejected", interval: 0, wantErr: true},
		{name: "negative is rejected", interval: -time.Second, wantErr: true},
		{name: "positive is accepted", interval: 2 * time.Second, wantErr: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := validateInterval(tt.interval)
			if tt.wantErr {
				require.ErrorContains(t, err, intervalErr)

				return
			}

			require.NoError(t, err)
		})
	}
}

// TestWatchRejectsNonPositiveInterval drives the real cobra command to prove the
// guard is wired into RunE (and the flag is named "--interval") so the CLI
// returns a clean error instead of panicking in time.NewTicker. A non-positive
// interval fails before GetClusterClient, so this never touches TLS or the OS
// keychain.
func TestWatchRejectsNonPositiveInterval(t *testing.T) {
	t.Parallel()

	cmd := NewWatchCommand()
	cmd.SetArgs([]string{"--interval", "0"})
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true

	err := cmd.Execute()
	require.ErrorContains(t, err, intervalErr)
}
