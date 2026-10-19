package controller

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

func TestRemoveNodeSkipsRemoveWhenMembershipPostconditionAlreadyHolds(t *testing.T) {
	t.Parallel()

	var calls int
	exec := ledgerctlExec(func(
		_ context.Context,
		_ *rest.Config,
		_ kubernetes.Interface,
		_, _, _ string,
		command []string,
	) (*execResult, error) {
		calls++
		require.Contains(t, command[2], "'cluster' 'status' '--json'")

		return &execResult{Stdout: `{"state":"Leader","nodes":[{"id":"1"}]}`}, nil
	})

	err := removeNodeWithExec(
		context.Background(), nil, nil,
		"ns", "ledger-0", "ledger", "ledger-0:3068", "disabled", 3, false,
		exec,
	)
	require.NoError(t, err)
	require.Equal(t, 1, calls, "an absent node must not trigger remove-node")
}

func TestForceRemoveNodeUsesLeaderLocalMembershipCheck(t *testing.T) {
	t.Parallel()

	var calls int
	exec := ledgerctlExec(func(
		_ context.Context,
		_ *rest.Config,
		_ kubernetes.Interface,
		_, _, _ string,
		command []string,
	) (*execResult, error) {
		calls++
		shellCommand := command[2]

		switch calls {
		case 1:
			require.Contains(t, shellCommand, "'cluster' 'status' '--node-id' '1' '--json'")

			return &execResult{Stdout: `{"state":"Leader","nodes":[{"id":"1"},{"id":"3"}]}`}, nil
		case 2:
			require.Contains(t, shellCommand, "'cluster' 'remove-node' '3' '--force'")

			return &execResult{}, nil
		default:
			t.Fatalf("unexpected ledgerctl call %d: %s", calls, shellCommand)

			return nil, nil
		}
	})

	err := removeNodeWithExec(
		context.Background(), nil, nil,
		"ns", "ledger-0", "ledger", "ledger-0:3068", "disabled", 3, true,
		exec,
	)
	require.NoError(t, err)
	require.Equal(t, 2, calls)
}

func TestRemoveNodeAcceptsAbsentPostconditionAfterOpaqueError(t *testing.T) {
	t.Parallel()

	var (
		calls       int
		removeCalls int
	)
	exec := ledgerctlExec(func(
		_ context.Context,
		_ *rest.Config,
		_ kubernetes.Interface,
		_, _, _ string,
		command []string,
	) (*execResult, error) {
		calls++
		shellCommand := command[2]

		switch calls {
		case 1:
			require.Contains(t, shellCommand, "'cluster' 'status' '--json'")

			return &execResult{Stdout: `{"state":"Leader","nodes":[{"id":"1"},{"id":"3"}]}`}, nil
		case 2:
			require.Contains(t, shellCommand, "'cluster' 'remove-node' '3'")
			removeCalls++

			return &execResult{Stderr: "unknown server error (correlation ID: opaque)"}, errors.New("exec failed")
		case 3:
			require.Contains(t, shellCommand, "'cluster' 'status' '--json'")

			return &execResult{Stdout: `{"state":"Leader","nodes":[{"id":"1"}]}`}, nil
		default:
			t.Fatalf("unexpected ledgerctl call %d: %s", calls, shellCommand)

			return nil, nil
		}
	})

	err := removeNodeWithExec(
		context.Background(), nil, nil,
		"ns", "ledger-0", "ledger", "ledger-0:3068", "disabled", 3, false,
		exec,
	)
	require.NoError(t, err)
	require.Equal(t, 3, calls)
	require.Equal(t, 1, removeCalls, "the operator must not retry remove-node after membership confirms absence")
}

func TestRemoveNodeReturnsOpaqueErrorWhenNodeIsStillPresent(t *testing.T) {
	t.Parallel()

	var calls int
	exec := ledgerctlExec(func(
		_ context.Context,
		_ *rest.Config,
		_ kubernetes.Interface,
		_, _, _ string,
		command []string,
	) (*execResult, error) {
		calls++
		if strings.Contains(command[2], "'remove-node'") {
			return &execResult{Stderr: "unknown server error"}, errors.New("opaque removal failure")
		}

		return &execResult{Stdout: `{"state":"Leader","nodes":[{"id":"1"},{"id":"3"}]}`}, nil
	})

	err := removeNodeWithExec(
		context.Background(), nil, nil,
		"ns", "ledger-0", "ledger", "ledger-0:3068", "disabled", 3, false,
		exec,
	)
	require.ErrorContains(t, err, "opaque removal failure")
	require.Equal(t, 3, calls)
}

func TestRaftNodePresentRejectsNonLeaderStatus(t *testing.T) {
	t.Parallel()

	exec := ledgerctlExec(func(
		_ context.Context,
		_ *rest.Config,
		_ kubernetes.Interface,
		_, _, _ string,
		_ []string,
	) (*execResult, error) {
		return &execResult{Stdout: `{"state":"Follower","nodes":[]}`}, nil
	})

	present, err := raftNodePresent(
		context.Background(), nil, nil,
		"ns", "ledger-0", "ledger", "ledger-0:3068", "disabled", 3, false,
		exec,
	)
	require.False(t, present)
	require.ErrorContains(t, err, "non-leader")
}

func TestRemoveNodePreservesStatusIdentity(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name       string
		status     string
		wantRemove bool
	}{
		{name: "small member present", status: `{"state":"Leader","nodes":[{"id":"1"},{"id":"3"}]}`, wantRemove: true},
		{name: "distinct colliding members", status: `{"state":"Leader","nodes":[{"id":"1"},{"id":"3"},{"id":"4294967297"},{"id":"4294967299"}]}`, wantRemove: true},
		{name: "high member does not alias target", status: `{"state":"Leader","nodes":[{"id":"1"},{"id":"4294967299"}]}`},
		{name: "high bit only and maximum members", status: `{"state":"Leader","nodes":[{"id":"1"},{"id":"4294967296"},{"id":"18446744073709551615"}]}`},
		{name: "numeric small member present", status: `{"state":"Leader","nodes":[{"id":1},{"id":3}]}`, wantRemove: true},
		{name: "numeric distinct colliding members", status: `{"state":"Leader","nodes":[{"id":1},{"id":3},{"id":4294967297},{"id":4294967299}]}`, wantRemove: true},
		{name: "numeric high member does not alias target", status: `{"state":"Leader","nodes":[{"id":1},{"id":4294967299}]}`},
		{name: "numeric high bit only and maximum members", status: `{"state":"Leader","nodes":[{"id":1},{"id":4294967296},{"id":18446744073709551615}]}`},
		{name: "mixed numeric and string forms", status: `{"state":"Leader","nodes":[{"id":1},{"id":"3"},{"id":4294967299}]}`, wantRemove: true},
		{name: "current CLI hybrid output", status: `{"state":"Leader","nodes":[{"id":1},{"id":3},{"id":"4294967299"}]}`, wantRemove: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var calls []string
			exec := ledgerctlExec(func(_ context.Context, _ *rest.Config, _ kubernetes.Interface,
				_, _, _ string, command []string,
			) (*execResult, error) {
				calls = append(calls, command[2])
				if len(calls) == 1 {
					require.Contains(t, command[2], "'cluster' 'status' '--json'")

					return &execResult{Stdout: tc.status}, nil
				}
				require.Contains(t, command[2], "'cluster' 'remove-node' '3'")

				return &execResult{}, nil
			})
			err := removeNodeWithExec(context.Background(), nil, nil,
				"ns", "ledger-0", "ledger", "ledger-0:3068", "disabled", 3, false, exec)
			require.NoError(t, err)
			if tc.wantRemove {
				require.Len(t, calls, 2)
			} else {
				require.Len(t, calls, 1, "another full-width member must not trigger removal of node 3")
			}
		})
	}
}

func TestRemoveNodeRejectsInvalidStatusIdentity(t *testing.T) {
	t.Parallel()
	for _, id := range []string{`18446744073709551616`, `"18446744073709551616"`, `-1`, `"-1"`, `"+3"`, `"invalid"`, `3.0`, `"3.0"`, `3e0`, `"3e0"`, `null`, `true`, `""`, `" 3"`} {
		t.Run(id, func(t *testing.T) {
			t.Parallel()
			var calls int
			exec := ledgerctlExec(func(_ context.Context, _ *rest.Config, _ kubernetes.Interface,
				_, _, _ string, command []string,
			) (*execResult, error) {
				calls++
				require.Contains(t, command[2], "'cluster' 'status' '--json'")

				return &execResult{Stdout: `{"state":"Leader","nodes":[{"id":"1"},{"id":` + id + `}]}`}, nil
			})
			err := removeNodeWithExec(context.Background(), nil, nil,
				"ns", "ledger-0", "ledger", "ledger-0:3068", "disabled", 3, false, exec)
			require.ErrorContains(t, err, "decoding ledgerctl cluster status JSON")
			require.Equal(t, 1, calls, "invalid status must stop before removing a node")
		})
	}
}

func TestRaftStatusNodeIDPreservesExactUint64(t *testing.T) {
	t.Parallel()
	for _, value := range []uint64{0, 1, 3, 4294967299, 9007199254740993, ^uint64(0)} {
		for _, quoted := range []bool{false, true} {
			encoded := strconv.FormatUint(value, 10)
			if quoted {
				encoded = strconv.Quote(encoded)
			}
			t.Run(encoded, func(t *testing.T) {
				t.Parallel()
				var id raftStatusNodeID
				require.NoError(t, json.Unmarshal([]byte(encoded), &id))
				require.Equal(t, value, uint64(id))
			})
		}
	}
}
