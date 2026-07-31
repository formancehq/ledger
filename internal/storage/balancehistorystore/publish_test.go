package balancehistorystore

import (
	"math"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBuildRunRecordsFromGroupsRejectsOversizedIdentityComponentsDeterministically(t *testing.T) {
	t.Parallel()

	oversized := strings.Repeat("x", math.MaxUint16+1)
	testCases := []struct {
		name     string
		identity recordIdentity
		wantErr  string
	}{
		{
			name: "account",
			identity: recordIdentity{
				Axis:      AxisEffective,
				Scope:     scopeVolume,
				LedgerID:  1,
				Account:   oversized,
				AssetBase: "USD",
			},
			wantErr: "encoding balance history catalog key: history account key component exceeds 65535 bytes",
		},
		{
			name: "asset base",
			identity: recordIdentity{
				Axis:      AxisEffective,
				Scope:     scopeVolume,
				LedgerID:  1,
				Account:   "assets:cash",
				AssetBase: oversized,
			},
			wantErr: "encoding balance history catalog key: history key component exceeds 65535 bytes",
		},
		{
			name: "color",
			identity: recordIdentity{
				Axis:      AxisEffective,
				Scope:     scopeVolume,
				LedgerID:  1,
				Account:   "assets:cash",
				AssetBase: "USD",
				Color:     oversized,
			},
			wantErr: "encoding balance history catalog key: history key component exceeds 65535 bytes",
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			for range 10 {
				assertRunRecordBuildError(t, map[recordIdentity][]timedDelta{
					testCase.identity: nil,
				}, testCase.wantErr)
			}
		})
	}

	t.Run("mixed invalid identities", func(t *testing.T) {
		t.Parallel()

		groups := map[recordIdentity][]timedDelta{
			{
				Axis:      AxisEffective,
				Scope:     scopeVolume,
				LedgerID:  1,
				Account:   oversized,
				AssetBase: "USD",
			}: nil,
			{
				Axis:      AxisEffective,
				Scope:     scopeVolume,
				LedgerID:  2,
				Account:   "assets:cash",
				AssetBase: oversized,
			}: nil,
			{
				Axis:      AxisEffective,
				Scope:     scopeVolume,
				LedgerID:  3,
				Account:   "assets:cash",
				AssetBase: "USD",
				Color:     oversized,
			}: nil,
		}

		for range 100 {
			assertRunRecordBuildError(t, groups, "encoding balance history catalog key: history account key component exceeds 65535 bytes")
		}
	})
}

func assertRunRecordBuildError(t *testing.T, groups map[recordIdentity][]timedDelta, wantErr string) {
	t.Helper()

	records, dataCount, identityCount, checksum, err := buildRunRecordsFromGroups(1, groups)
	require.EqualError(t, err, wantErr)
	require.Nil(t, records)
	require.Zero(t, dataCount)
	require.Zero(t, identityCount)
	require.Zero(t, checksum)
}
