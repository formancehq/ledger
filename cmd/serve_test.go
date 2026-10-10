package cmd

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewServeCommand_MaxPageSizeDefault(t *testing.T) {
	cmd := NewServeCommand()
	flag := cmd.Flags().Lookup(MaxPageSizeFlag)
	require.NotNil(t, flag)
	require.Equal(t, "1000", flag.DefValue)
}
