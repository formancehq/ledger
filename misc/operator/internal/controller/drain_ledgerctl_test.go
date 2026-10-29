package controller

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
)

func TestLedgerctlDrainReadsLocalDurableCursors(t *testing.T) {
	t.Parallel()
	c := &ledgerctlDrainCluster{
		cluster: &ledgerv1alpha1.Cluster{},
		exec: func(_ context.Context, _ *rest.Config, _ kubernetes.Interface, _, pod, _ string, command []string) (*execResult, error) {
			line := strings.Join(command, " ")
			switch {
			case strings.Contains(line, "'begin-drain'"):
				return &execResult{Stdout: `{"cutoffIndex":"42","members":["1","2"]}`}, nil
			case strings.Contains(line, "'drain-status'"):
				if pod != podName("", 1) {
					return nil, fmt.Errorf("node 2 queried through %s", pod)
				}

				return &execResult{Stdout: `{"localNode":"2","supportsDrain":true,"drainEpoch":"e1","drainIndex":"42","lastPersistedIndex":"42","syncStatus":"normal"}`}, nil
			default:
				return &execResult{Stdout: `{"state":"Leader","drainEpoch":"e1","drainIndex":"42","nodes":[{"id":"1"},{"id":"2"}]}`}, nil
			}
		},
	}
	decision, err := c.BeginDrain(t.Context(), "e1")
	require.NoError(t, err)
	require.Equal(t, drainDecision{Epoch: "e1", Index: 42, Members: []uint64{1, 2}}, decision)
	current, err := c.CurrentDrain(t.Context())
	require.NoError(t, err)
	require.Equal(t, decision, current)
	status, err := c.NodeStatus(t.Context(), 2)
	require.NoError(t, err)
	require.Equal(t, uint64(42), status.LastPersistedIndex)
	require.Equal(t, uint64(2), status.NodeID)
}

func TestLedgerctlDrainEpochBeginningWithDash(t *testing.T) {
	t.Parallel()
	c := &ledgerctlDrainCluster{
		cluster: &ledgerv1alpha1.Cluster{},
		exec: func(_ context.Context, _ *rest.Config, _ kubernetes.Interface, _, _, _ string, command []string) (*execResult, error) {
			require.Equal(t, []string{"/bin/sh", "-c"}, command[:2])
			require.Contains(t, command[2], "'cluster' 'begin-drain' '--json'")
			require.Contains(t, command[2], "'--' '-shutdown'")
			return &execResult{Stdout: `{"cutoffIndex":"42","members":["1"]}`}, nil
		},
	}
	_, err := c.BeginDrain(t.Context(), "-shutdown")
	require.NoError(t, err)
}

func TestLedgerctlDrainRejectsWrongLocalNode(t *testing.T) {
	t.Parallel()
	c := &ledgerctlDrainCluster{
		cluster: &ledgerv1alpha1.Cluster{},
		exec: func(context.Context, *rest.Config, kubernetes.Interface, string, string, string, []string) (*execResult, error) {
			return &execResult{Stdout: `{"localNode":"1","supportsDrain":true,"drainIndex":"42","lastPersistedIndex":"42"}`}, nil
		},
	}
	_, err := c.NodeStatus(t.Context(), 2)
	require.ErrorContains(t, err, "reported invalid node")
}

func TestLedgerctlDrainRejectsMemberWithoutCapability(t *testing.T) {
	t.Parallel()
	c := &ledgerctlDrainCluster{
		cluster: &ledgerv1alpha1.Cluster{},
		exec: func(context.Context, *rest.Config, kubernetes.Interface, string, string, string, []string) (*execResult, error) {
			return &execResult{Stdout: `{"localNode":"2","supportsDrain":false,"drainIndex":"42","lastPersistedIndex":"42"}`}, nil
		},
	}
	_, err := c.NodeStatus(t.Context(), 2)
	require.ErrorContains(t, err, "has not confirmed drain support")
}
