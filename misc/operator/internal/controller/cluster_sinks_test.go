package controller

import (
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
)

func TestDesiredEventSinksNormalizesDefaultsAndOrdering(t *testing.T) {
	t.Parallel()

	batchSize := int32(20)
	batchDelay := int64(25)
	desired := desiredEventSinks(&ledgerv1alpha1.EventSinksSpec{
		NATS: []ledgerv1alpha1.NATSEventSinkSpec{
			{
				Name:         "zeta",
				URL:          "nats://nats:4222",
				Topic:        "ledger.events",
				Format:       "protobuf",
				BatchSize:    &batchSize,
				BatchDelayMs: &batchDelay,
				EventTypes:   []string{"REVERTED_TRANSACTION", "COMMITTED_TRANSACTION"},
			},
			{Name: "alpha", URL: "nats://nats:4222", Topic: "ledger.audit"},
		},
	})

	require.Len(t, desired, 2)
	assert.Equal(t, "alpha", desired[0].name)
	assert.Equal(t, "json", desired[0].format)
	assert.Zero(t, desired[0].batchSize)
	assert.Zero(t, desired[0].batchDelayMS)
	assert.Equal(t, []string{"COMMITTED_TRANSACTION", "REVERTED_TRANSACTION"}, desired[1].eventTypes)
}

func TestParseActualEventSinks(t *testing.T) {
	t.Parallel()

	actual, err := parseActualEventSinks(`{
  "sinks": [
    {
      "name": "primary",
      "nats": {"url": "nats://nats:4222", "topic": "ledger.events"},
      "format": "json",
      "batchSize": 10,
      "batchDelayMs": "50",
      "eventTypes": ["REVERTED_TRANSACTION", "COMMITTED_TRANSACTION"]
    },
    {
      "name": "external",
      "http": {"endpoint": "https://example.com", "secret": "(set)"},
      "format": "json",
      "batchSize": 0,
      "batchDelayMs": "0",
      "eventTypes": []
    }
  ],
  "sinkStatuses": []
}`)
	require.NoError(t, err)
	require.Len(t, actual, 2)

	assert.Equal(t, "nats", actual["primary"].kind)
	assert.Equal(t, "nats://nats:4222", actual["primary"].nats.url)
	assert.Equal(t, int64(50), actual["primary"].nats.batchDelayMS)
	assert.Equal(t, []string{"COMMITTED_TRANSACTION", "REVERTED_TRANSACTION"}, actual["primary"].nats.eventTypes)
	assert.Equal(t, "http", actual["external"].kind)
}

func TestDiffEventSinksScopesOwnership(t *testing.T) {
	t.Parallel()

	desiredPrimary := managedNATSSink{
		name: "primary", url: "nats://nats:4222", topic: "ledger.events", format: "json",
	}
	desiredNew := managedNATSSink{
		name: "new", url: "nats://nats:4222", topic: "ledger.new", format: "json",
	}

	tests := []struct {
		name    string
		desired []managedNATSSink
		actual  map[string]actualEventSink
		applied []string
		want    eventSinkDiff
	}{
		{
			name:    "create missing desired sink",
			desired: []managedNATSSink{desiredNew},
			actual:  map[string]actualEventSink{},
			want:    eventSinkDiff{toCreate: []managedNATSSink{desiredNew}},
		},
		{
			name:    "matching external sink conflicts without adoption",
			desired: []managedNATSSink{desiredPrimary},
			actual: map[string]actualEventSink{
				"primary": {kind: "nats", nats: desiredPrimary},
			},
			want: eventSinkDiff{conflict: []string{"primary"}},
		},
		{
			name:    "mismatched external sink conflicts",
			desired: []managedNATSSink{desiredPrimary},
			actual: map[string]actualEventSink{
				"primary": {kind: "http"},
			},
			want: eventSinkDiff{conflict: []string{"primary"}},
		},
		{
			name:    "mismatched owned sink is removed for two-pass update",
			desired: []managedNATSSink{desiredPrimary},
			actual: map[string]actualEventSink{
				"primary": {kind: "nats", nats: managedNATSSink{name: "primary", url: "nats://old:4222", topic: "ledger.events", format: "json"}},
			},
			applied: []string{"primary"},
			want:    eventSinkDiff{toDrop: []string{"primary"}},
		},
		{
			name: "managed empty removes only owned sink",
			actual: map[string]actualEventSink{
				"owned":    {kind: "nats"},
				"external": {kind: "http"},
			},
			applied: []string{"owned"},
			want:    eventSinkDiff{toDrop: []string{"owned"}},
		},
		{
			name:    "missing no-longer-desired sink relinquishes stale ownership",
			actual:  map[string]actualEventSink{},
			applied: []string{"gone"},
			want:    eventSinkDiff{toDrop: []string{"gone"}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, diffEventSinks(tt.desired, tt.actual, tt.applied))
		})
	}
}

func TestReconcileEventSinksCreatesAndRecordsOwnership(t *testing.T) {
	t.Parallel()

	cluster := clusterWithNATSSinks("primary")
	var calls [][]string
	exec := func(args ...string) (string, error) {
		calls = append(calls, slices.Clone(args))
		if slices.Equal(args, []string{"events", "list", "--json"}) {
			return `{"sinks":[],"sinkStatuses":[]}`, nil
		}

		return "", nil
	}

	synced, err := reconcileEventSinks(cluster, exec, nil)
	require.NoError(t, err)
	assert.False(t, synced)
	assert.Equal(t, []string{"primary"}, cluster.Status.AppliedSinks)
	require.Len(t, calls, 2)
	assert.Equal(t, []string{"events", "add-sink", "--name", "primary", "--nats-url", "nats://nats:4222", "--nats-topic", "ledger.events", "--format", "json", "--batch-size", "0", "--batch-delay-ms", "0"}, calls[1])
}

func TestReconcileEventSinksDoesNotCreateBeforeOwnershipIsPersisted(t *testing.T) {
	t.Parallel()

	cluster := clusterWithNATSSinks("primary")
	callCount := 0
	exec := func(args ...string) (string, error) {
		callCount++

		return `{"sinks":[]}`, nil
	}

	synced, err := reconcileEventSinks(cluster, exec, func() error {
		return errors.New("injected status failure")
	})
	assert.False(t, synced)
	require.ErrorContains(t, err, "injected status failure")
	assert.Equal(t, 1, callCount, "add-sink must not run before ownership is durable")
	assert.Empty(t, cluster.Status.AppliedSinks)
}

func TestReconcileEventSinksPersistsPartialProgress(t *testing.T) {
	t.Parallel()

	cluster := clusterWithNATSSinks("alpha", "beta")
	exec := func(args ...string) (string, error) {
		if slices.Equal(args, []string{"events", "list", "--json"}) {
			return `{"sinks":[],"sinkStatuses":[]}`, nil
		}
		if slices.Contains(args, "beta") {
			return "", errors.New("injected add failure")
		}

		return "", nil
	}

	synced, err := reconcileEventSinks(cluster, exec, nil)
	assert.False(t, synced)
	require.ErrorContains(t, err, "injected add failure")
	assert.Equal(t, []string{"alpha", "beta"}, cluster.Status.AppliedSinks)
}

func TestReconcileEventSinksRecoversOwnershipAfterAmbiguousAdd(t *testing.T) {
	t.Parallel()

	cluster := clusterWithNATSSinks("primary")
	added := false
	exec := func(args ...string) (string, error) {
		if slices.Equal(args, []string{"events", "list", "--json"}) {
			if added {
				return `{"sinks":[{"name":"primary","nats":{"url":"nats://nats:4222","topic":"ledger.events"},"format":"json","batchSize":0,"batchDelayMs":"0","eventTypes":[]}]}`, nil
			}

			return `{"sinks":[]}`, nil
		}

		added = true

		return "", errors.New("response lost after commit")
	}

	synced, err := reconcileEventSinks(cluster, exec, nil)
	assert.False(t, synced)
	require.ErrorContains(t, err, "response lost after commit")
	assert.Equal(t, []string{"primary"}, cluster.Status.AppliedSinks)

	synced, err = reconcileEventSinks(cluster, exec, nil)
	require.NoError(t, err)
	assert.True(t, synced)
	assert.Equal(t, []string{"primary"}, cluster.Status.AppliedSinks)
}

func TestReconcileEventSinksKeepsOwnershipWhenRemoveTransportFails(t *testing.T) {
	t.Parallel()

	cluster := clusterWithNATSSinks()
	cluster.Status.AppliedSinks = []string{"primary"}
	listCalls := 0
	exec := func(args ...string) (string, error) {
		if slices.Equal(args, []string{"events", "list", "--json"}) {
			listCalls++

			return `{"sinks":[{"name":"primary","nats":{"url":"nats://nats:4222","topic":"ledger.events"},"format":"json","batchSize":0,"batchDelayMs":"0","eventTypes":[]}]}`, nil
		}

		return "", errors.New(`pods "ledger-0" not found`)
	}

	synced, err := reconcileEventSinks(cluster, exec, nil)
	assert.False(t, synced)
	require.ErrorContains(t, err, `pods "ledger-0" not found`)
	assert.Equal(t, 2, listCalls)
	assert.Equal(t, []string{"primary"}, cluster.Status.AppliedSinks)
}

func TestReconcileEventSinksUpdatesOwnedSinkInTwoPasses(t *testing.T) {
	t.Parallel()

	cluster := clusterWithNATSSinks("primary")
	cluster.Status.AppliedSinks = []string{"primary"}
	actualState := "old"
	var mutations [][]string
	exec := func(args ...string) (string, error) {
		if slices.Equal(args, []string{"events", "list", "--json"}) {
			switch actualState {
			case "old":
				return `{"sinks":[{"name":"primary","nats":{"url":"nats://old:4222","topic":"ledger.events"},"format":"json","batchSize":0,"batchDelayMs":"0","eventTypes":[]}]}`, nil
			case "missing":
				return `{"sinks":[]}`, nil
			default:
				return "", errors.New("unexpected test state")
			}
		}

		mutations = append(mutations, slices.Clone(args))
		if slices.Equal(args, []string{"events", "remove-sink", "--name", "primary"}) {
			actualState = "missing"
		}

		return "", nil
	}

	synced, err := reconcileEventSinks(cluster, exec, nil)
	require.NoError(t, err)
	assert.False(t, synced)
	assert.Empty(t, cluster.Status.AppliedSinks)

	synced, err = reconcileEventSinks(cluster, exec, nil)
	require.NoError(t, err)
	assert.False(t, synced)
	assert.Equal(t, []string{"primary"}, cluster.Status.AppliedSinks)
	require.Len(t, mutations, 2)
	assert.Equal(t, []string{"events", "remove-sink", "--name", "primary"}, mutations[0])
	assert.Equal(t, []string{"events", "add-sink", "--name", "primary", "--nats-url", "nats://nats:4222", "--nats-topic", "ledger.events", "--format", "json", "--batch-size", "0", "--batch-delay-ms", "0"}, mutations[1])
}

func TestReconcileEventSinksRejectsExternalNameConflictWithoutMutation(t *testing.T) {
	t.Parallel()

	cluster := clusterWithNATSSinks("primary")
	callCount := 0
	exec := func(args ...string) (string, error) {
		callCount++

		return `{"sinks":[{"name":"primary","format":"json","batchSize":0,"batchDelayMs":"0","eventTypes":[],"http":{"endpoint":"https://example.com","secret":""}}]}`, nil
	}

	synced, err := reconcileEventSinks(cluster, exec, nil)
	assert.False(t, synced)
	require.ErrorContains(t, err, "not operator-owned")
	assert.Equal(t, 1, callCount)
	assert.Empty(t, cluster.Status.AppliedSinks)
}

func TestReconcileEventSinksDoesNotAdoptMatchingExternalSink(t *testing.T) {
	t.Parallel()

	cluster := clusterWithNATSSinks("primary")
	callCount := 0
	exec := func(args ...string) (string, error) {
		callCount++

		return `{"sinks":[{"name":"primary","nats":{"url":"nats://nats:4222","topic":"ledger.events"},"format":"json","batchSize":0,"batchDelayMs":"0","eventTypes":[]}]}`, nil
	}

	synced, err := reconcileEventSinks(cluster, exec, nil)
	assert.False(t, synced)
	require.ErrorContains(t, err, "not operator-owned")
	assert.Equal(t, 1, callCount)
	assert.Empty(t, cluster.Status.AppliedSinks)
}

func TestNextAppliedSinksIsDeterministic(t *testing.T) {
	t.Parallel()

	assert.Equal(t, []string{"alpha", "gamma"}, nextAppliedSinks(
		[]string{"beta", "alpha"},
		[]string{"gamma", "alpha"},
		[]string{"beta"},
	))
}

func clusterWithNATSSinks(names ...string) *ledgerv1alpha1.Cluster {
	cluster := &ledgerv1alpha1.Cluster{Spec: ledgerv1alpha1.ClusterSpec{Sinks: &ledgerv1alpha1.EventSinksSpec{}}}
	for _, name := range names {
		cluster.Spec.Sinks.NATS = append(cluster.Spec.Sinks.NATS, ledgerv1alpha1.NATSEventSinkSpec{
			Name: name, URL: "nats://nats:4222", Topic: "ledger.events", Format: "json",
		})
	}

	return cluster
}

func TestAddNATSSinkArgsSortsEventTypes(t *testing.T) {
	t.Parallel()

	args := addNATSSinkArgs(managedNATSSink{
		name: "primary", url: "nats://nats:4222", topic: "ledger.events", format: "json",
		eventTypes: []string{"COMMITTED_TRANSACTION", "REVERTED_TRANSACTION"},
	})

	assert.True(t, strings.HasSuffix(strings.Join(args, " "), "--event-types COMMITTED_TRANSACTION,REVERTED_TRANSACTION"))
}
