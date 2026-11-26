package connectionurl

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseShape(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, raw string
		valid     bool
	}{
		{"connector", "postgres://alice:canary@db/ledger", true},
		{"future scheme", "custom+source://alice:canary@db/ledger", true},
		{"uppercase scheme", "POSTGRES://db/ledger", true},
		{"empty host", "custom:///ledger", true},
		{"port only", "custom://:65535/ledger", true},
		{"port out of range", "custom://:65536/ledger", false},
		{"multi host boundary ports", "custom://one:0,two:65535/ledger", true},
		{"multi host first port out of range", "custom://one:65536,two:9000/ledger", false},
		{"multi host middle port out of range", "custom://one:9000,two:65536,three:9000/ledger", false},
		{"multi host last port out of range", "custom://one:9000,two:65536/ledger", false},
		{"multi host invalid first port", "custom://one:bad,two:9000/ledger", false},
		{"IPv6 zone", "custom://[fe80::1%25en0]:65535/ledger", true},
		{"host largest port", "custom://db:65535/ledger", true},
		{"host port out of range", "custom://db:65536/ledger", false},
		{"IPv6 largest port", "custom://[::1]:65535/ledger", true},
		{"IPv6 port out of range", "custom://[::1]:65536/ledger", false},
		{"missing scheme", "://db/ledger", false},
		{"invalid scheme", "1source://db/ledger", false},
		{"delimiter only in query", "postgres:/ledger?option=https://control", false},
		{"delimiter only in path", "custom:/ledger/https://control", false},
		{"delimiter only in fragment", "custom:/ledger#https://control", false},
		{"opaque", "postgres:password=canary", false},
		{"keyword", "host=db password=canary", false},
		{"invalid escape", "custom://alice:canary@db/%zz", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			parsed, ok := ParseShape(tc.raw, true)
			require.Equal(t, tc.valid, ok)
			if ok {
				require.NotEmpty(t, parsed.Scheme)
			} else {
				require.Nil(t, parsed)
			}
		})
	}
}

func TestParseRetainsConnectorPolicy(t *testing.T) {
	t.Parallel()
	_, ok := Parse("custom://db/ledger", true, "postgres", "postgresql")
	require.False(t, ok, "generic shape must not widen projection's scheme policy")
	_, ok = Parse("postgres:///ledger", false, "postgres")
	require.False(t, ok, "callers can still require a hostname")
}
