package membership

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidatePeerAddress(t *testing.T) {
	t.Parallel()

	for _, addr := range []string{
		"127.0.0.1:7777",
		"ledger-1.ledger.svc.cluster.local:8888",
		"[::1]:7777",
		"node-2:65535",
		":7777",
		"localhost:7777",
		"ledger_1.internal.:7777",
		"node.:7777",
		"[fe80::1%eth0]:7777",
	} {
		t.Run("valid "+addr, func(t *testing.T) {
			t.Parallel()
			require.NoError(t, ValidatePeerAddress("raft_address", addr))
		})
	}

	for _, tc := range []struct {
		addr string
		want string
	}{
		{addr: "", want: "raft_address is required"},
		{addr: "not an address", want: "must be host:port"},
		{addr: "node-2", want: "must be host:port"},
		{addr: "not an:7777", want: "invalid host"},
		{addr: "bad\x00host:7777", want: "invalid host"},
		{addr: "bad\thost:7777", want: "invalid host"},
		{addr: "bad/host:7777", want: "invalid host"},
		{addr: "-node:7777", want: "invalid host"},
		{addr: "node-:7777", want: "invalid host"},
		{addr: "node..svc:7777", want: "invalid host"},
		{addr: "node..:7777", want: "invalid host"},
		{addr: ".:7777", want: "invalid host"},
		{addr: "1234:7777", want: "invalid host"},
		{addr: strings.Repeat("a", 64) + ":7777", want: "invalid host"},
		{addr: strings.Repeat("a.", 127) + "ab:7777", want: "invalid host"},
		{addr: "node-2:", want: "port must be a number between 1 and 65535"},
		{addr: "node-2:0", want: "port must be a number between 1 and 65535"},
		{addr: "node-2:65536", want: "port must be a number between 1 and 65535"},
		{addr: "node-2:http", want: "port must be a number between 1 and 65535"},
	} {
		t.Run("invalid "+tc.addr, func(t *testing.T) {
			t.Parallel()
			err := ValidatePeerAddress("raft_address", tc.addr)
			require.ErrorContains(t, err, "raft_address")
			require.ErrorContains(t, err, tc.want)
		})
	}
}
