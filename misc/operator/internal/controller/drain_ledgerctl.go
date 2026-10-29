package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
)

// ledgerctlDrainCluster addresses each Raft member through its own Pod.
type ledgerctlDrainCluster struct {
	cluster   *ledgerv1alpha1.Cluster
	tlsMode   string
	config    *rest.Config
	clientset kubernetes.Interface
	exec      ledgerctlExec
}

type ledgerctlDrainState struct {
	State      string `json:"state"`
	LocalNode  string `json:"localNode"`
	DrainEpoch string `json:"drainEpoch"`
	DrainIndex string `json:"drainIndex"`
	Nodes      []struct {
		ID string `json:"id"`
	} `json:"nodes"`
}

type ledgerctlBeginDrainResponse struct {
	CutoffIndex string   `json:"cutoffIndex"`
	Members     []string `json:"members"`
}

type ledgerctlEndDrainResponse struct {
	ReleaseIndex string   `json:"releaseIndex"`
	Members      []string `json:"members"`
}

type ledgerctlLocalDrainStatus struct {
	LocalNode          string `json:"localNode"`
	SupportsDrain      bool   `json:"supportsDrain"`
	DrainEpoch         string `json:"drainEpoch"`
	DrainIndex         string `json:"drainIndex"`
	ReleaseIndex       string `json:"releaseIndex"`
	SupportsRelease    bool   `json:"supportsRelease"`
	LastPersistedIndex string `json:"lastPersistedIndex"`
	SyncStatus         string `json:"syncStatus"`
}

func (c *ledgerctlDrainCluster) run(ctx context.Context, ordinal int, args ...string) (string, error) {
	cluster := c.cluster
	serverAddr := podSelfServerAddr(headlessServiceName(cluster.Name), cluster.Spec.GrpcPort)
	command := ledgerctlCommand(serverAddr, c.tlsMode, args...)
	result, err := c.exec(ctx, c.config, c.clientset, cluster.Namespace, podName(cluster.Name, ordinal), ledgerContainer, command)
	if err != nil {
		return "", fmt.Errorf("ledgerctl %s on pod %d: %w", args[0], ordinal, err)
	}
	if result == nil {
		return "", errors.New("ledgerctl returned no result")
	}

	return result.Stdout, nil
}

func (c *ledgerctlDrainCluster) BeginDrain(ctx context.Context, epoch string) (drainDecision, error) {
	output, err := c.run(ctx, 0, "cluster", "begin-drain", "--json", "--", epoch)
	if err != nil {
		return drainDecision{}, err
	}
	var response ledgerctlBeginDrainResponse
	if err := json.Unmarshal([]byte(output), &response); err != nil {
		return drainDecision{}, fmt.Errorf("decoding begin-drain response: %w", err)
	}
	index, err := strconv.ParseUint(response.CutoffIndex, 10, 64)
	if err != nil {
		return drainDecision{}, fmt.Errorf("decoding drain cutoff: %w", err)
	}
	members, err := parseDrainMembers(response.Members)
	if err != nil {
		return drainDecision{}, err
	}

	return drainDecision{Epoch: epoch, Index: index, Members: members}, nil
}

func (c *ledgerctlDrainCluster) EndDrain(ctx context.Context, epoch string) (drainReleaseDecision, error) {
	output, err := c.run(ctx, 0, "cluster", "end-drain", "--json", "--", epoch)
	if err != nil {
		return drainReleaseDecision{}, err
	}
	var response ledgerctlEndDrainResponse
	if err := json.Unmarshal([]byte(output), &response); err != nil {
		return drainReleaseDecision{}, fmt.Errorf("decoding end-drain response: %w", err)
	}
	index, err := strconv.ParseUint(response.ReleaseIndex, 10, 64)
	if err != nil {
		return drainReleaseDecision{}, fmt.Errorf("decoding drain release index: %w", err)
	}
	members, err := parseDrainMembers(response.Members)
	if err != nil {
		return drainReleaseDecision{}, err
	}

	return drainReleaseDecision{Epoch: epoch, Index: index, Members: members}, nil
}

func (c *ledgerctlDrainCluster) CurrentDrain(ctx context.Context) (drainDecision, error) {
	replicas := int32(3)
	if c.cluster.Spec.Replicas != nil {
		replicas = *c.cluster.Spec.Replicas
	}
	var state ledgerctlDrainState
	var lastErr error
	for ordinal := range int(replicas) {
		output, err := c.run(ctx, ordinal, "cluster", "status", "--json")
		if err != nil {
			if ctx.Err() != nil {
				return drainDecision{}, ctx.Err()
			}
			lastErr = err

			continue
		}
		if err := json.Unmarshal([]byte(output), &state); err != nil {
			return drainDecision{}, fmt.Errorf("decoding cluster status on pod %d: %w", ordinal, err)
		}
		if strings.EqualFold(state.State, "leader") {
			break
		}
	}
	if !strings.EqualFold(state.State, "leader") {
		if lastErr == nil {
			return drainDecision{}, errors.New("cluster status did not come from leader")
		}

		return drainDecision{}, fmt.Errorf("no leader status available: %w", lastErr)
	}
	index, err := parseOptionalUint64(state.DrainIndex)
	if err != nil {
		return drainDecision{}, fmt.Errorf("decoding current drain index: %w", err)
	}
	ids := make([]string, 0, len(state.Nodes))
	for _, member := range state.Nodes {
		ids = append(ids, member.ID)
	}
	members, err := parseDrainMembers(ids)
	if err != nil {
		return drainDecision{}, err
	}

	return drainDecision{Epoch: state.DrainEpoch, Index: index, Members: members}, nil
}

func (c *ledgerctlDrainCluster) NodeStatus(ctx context.Context, id uint64) (drainNodeStatus, error) {
	if id == 0 || id > math.MaxInt32 {
		return drainNodeStatus{}, fmt.Errorf("invalid operator node ID %d", id)
	}
	ordinal := int(id - 1)
	output, err := c.run(ctx, ordinal, "cluster", "drain-status", "--json")
	if err != nil {
		return drainNodeStatus{}, err
	}
	var state ledgerctlLocalDrainStatus
	if err := json.Unmarshal([]byte(output), &state); err != nil {
		return drainNodeStatus{}, fmt.Errorf("decoding node %d status: %w", id, err)
	}
	localID, err := strconv.ParseUint(state.LocalNode, 10, 64)
	if err != nil || localID != id {
		return drainNodeStatus{}, fmt.Errorf("pod %d reported invalid node %q", ordinal, state.LocalNode)
	}
	if !state.SupportsDrain {
		return drainNodeStatus{}, fmt.Errorf("node %d has not confirmed drain support", id)
	}
	index, err := parseOptionalUint64(state.DrainIndex)
	if err != nil {
		return drainNodeStatus{}, fmt.Errorf("decoding node %d drain index: %w", id, err)
	}
	persisted, err := strconv.ParseUint(state.LastPersistedIndex, 10, 64)
	if err != nil {
		return drainNodeStatus{}, fmt.Errorf("decoding node %d persisted index: %w", id, err)
	}
	releaseIndex, err := parseOptionalUint64(state.ReleaseIndex)
	if err != nil {
		return drainNodeStatus{}, fmt.Errorf("decoding node %d release index: %w", id, err)
	}

	return drainNodeStatus{NodeID: id, DrainEpoch: state.DrainEpoch, DrainIndex: index,
		ReleaseIndex: releaseIndex, SupportsRelease: state.SupportsRelease, LastPersistedIndex: persisted, SyncStatus: state.SyncStatus}, nil
}

func parseDrainMembers(raw []string) ([]uint64, error) {
	members := make([]uint64, 0, len(raw))
	for _, value := range raw {
		id, err := strconv.ParseUint(value, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("decoding member node ID %q: %w", value, err)
		}
		members = append(members, id)
	}

	return members, nil
}

func parseOptionalUint64(value string) (uint64, error) {
	if value == "" {
		return 0, nil
	}

	return strconv.ParseUint(value, 10, 64)
}
