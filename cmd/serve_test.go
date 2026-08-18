package cmd

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestServeCommandTransactionListFlagDefaults pins the transactions-list hedge to
// opt-in. The hedge costs a second connection and a second running query when it
// fires, and only workloads hitting the sparse-wallet plan benefit, so a
// deployment that passes no flag must keep the plain behaviour.
func TestServeCommandTransactionListFlagDefaults(t *testing.T) {
	t.Parallel()

	flags := NewServeCommand().Flags()

	enabled, err := flags.GetBool(TxListAdaptiveFallbackFlag)
	require.NoError(t, err)
	require.False(t, enabled,
		"--%s must default to false: enabling it by default turns the hedge on for every deployment",
		TxListAdaptiveFallbackFlag)

	// The tuning values are populated regardless, so switching a deployment on is
	// a single boolean rather than three values to rediscover.
	delay, err := flags.GetInt64(TxListChaserDelayMsFlag)
	require.NoError(t, err)
	require.Equal(t, int64(5_000), delay)

	timeout, err := flags.GetInt64(TxListChaserTimeoutMsFlag)
	require.NoError(t, err)
	require.Equal(t, int64(40_000), timeout)
}
