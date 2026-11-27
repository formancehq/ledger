package server

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"slices"
	"strings"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/go-libs/v5/pkg/service"
)

func TestSliceFlagPrecedenceThroughServiceExecute(t *testing.T) {
	const helper = "LEDGER_SLICE_PRECEDENCE_HELPER"
	if os.Getenv(helper) == "1" {
		root := NewServiceRootCommand()
		run, _, err := root.Find([]string{"run"})
		require.NoError(t, err)
		run.RunE = func(cmd *cobra.Command, _ []string) error {
			cfg, err := LoadConfig(context.Background(), cmd)
			if err != nil {
				return err
			}

			return json.NewEncoder(os.Stdout).Encode(struct {
				Issuers   []string `json:"issuers"`
				Reception []int    `json:"reception"`
				Send      []int    `json:"send"`
				NodeID    uint64   `json:"nodeId"`
			}{cfg.AuthConfig.TrustedIssuers(), cfg.TransportConfig.Reception, cfg.TransportConfig.Send, cfg.RaftConfig.NodeID})
		}
		root.SetArgs(os.Args[slices.Index(os.Args, "--")+1:])
		service.Execute(root)

		return
	}

	t.Parallel()
	for _, test := range []struct {
		name  string
		flags []string
		want  string
	}{
		{"CLI replaces environment", []string{"--node-id=7", "--auth-issuers=https://cli.example", "--raft-transport-reception-queues=1,2,3", "--raft-transport-send-queues=4,5,6"}, `{"issuers":["https://cli.example"],"reception":[1,2,3],"send":[4,5,6],"nodeId":7}`},
		{"repeated CLI issuer appends", []string{"--node-id=7", "--auth-issuers=https://first.example", "--auth-issuers=https://second.example"}, `{"issuers":["https://first.example","https://second.example"],"reception":[7,8,9],"send":[10,11,12],"nodeId":7}`},
		{"environment only", nil, `{"issuers":["https://env.example"],"reception":[7,8,9],"send":[10,11,12],"nodeId":99}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			args := []string{"-test.run=^TestSliceFlagPrecedenceThroughServiceExecute$", "--", "run", "--cluster-id=precedence-test"}
			args = append(args, test.flags...)
			cmd := exec.CommandContext(t.Context(), os.Args[0], args...)
			cmd.Env = append(os.Environ(), helper+"=1", "AUTH_ISSUERS=https://env.example", "RAFT_TRANSPORT_RECEPTION_QUEUES=7,8,9", "RAFT_TRANSPORT_SEND_QUEUES=10,11,12", "NODE_ID=99")
			output, err := cmd.CombinedOutput()
			require.NoError(t, err, string(output))
			require.JSONEq(t, test.want, strings.SplitN(string(output), "\n", 2)[0])
		})
	}
}
