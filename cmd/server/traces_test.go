package server

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRunCommandRejectsRemovedTraceSamplingFlags(t *testing.T) {
	t.Parallel()

	for _, flag := range []string{"--trace-sampling-enabled=true", "--trace-sampling-success-ratio=0.1"} {
		t.Run(flag, func(t *testing.T) {
			t.Parallel()
			cmd := NewRunCommand()
			require.ErrorContains(t, cmd.ParseFlags([]string{flag}), "unknown flag")
		})
	}
}
