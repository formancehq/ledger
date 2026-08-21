package internal

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPITScopeCasesCoverEveryAxisAndTransformMode(t *testing.T) {
	t.Parallel()

	testCases := PITScopeCases()
	require.Len(t, testCases, 8)

	names := make(map[string]struct{}, len(testCases))
	for _, testCase := range testCases {
		require.NotNil(t, testCase.Selector)
		require.NotNil(t, testCase.Selector.GetAt())
		names[testCase.Name] = struct{}{}
	}
	require.Len(t, names, len(testCases))
}

func TestPITScopeReservedAddressIsOutsideFixtureAccounts(t *testing.T) {
	t.Parallel()

	require.Equal(t, "pitscope:never-created", PITScopeReservedAddress)
	require.Equal(t, "pitscope-oracle", PITScopeLedgerName())
}

func TestResolvedPITScopeTargetsDropsSpareOrdinals(t *testing.T) {
	t.Parallel()

	resolved := ResolvedPITScopeTargets(PerNodeConns{
		&PerNodeConn{Addr: "ledger-0:8888", NodeID: 1},
		&PerNodeConn{Addr: "ledger-3:8888"},
		nil,
		&PerNodeConn{Addr: "ledger-2:8888", NodeID: 3},
	})
	require.Equal(t, PerNodeConns{
		&PerNodeConn{Addr: "ledger-0:8888", NodeID: 1},
		&PerNodeConn{Addr: "ledger-2:8888", NodeID: 3},
	}, resolved)
}
