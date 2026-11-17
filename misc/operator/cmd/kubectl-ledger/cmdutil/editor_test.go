package cmdutil

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestComputeDiff(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		original map[string]any
		modified map[string]any
		want     []Change
	}{
		{
			name:     "unchanged",
			original: map[string]any{"replicas": float64(3), "debug": false},
			modified: map[string]any{"replicas": float64(3), "debug": false},
		},
		{
			name:     "added and removed values",
			original: map[string]any{"debug": true},
			modified: map[string]any{"replicas": float64(5)},
			want: []Change{
				{Path: "spec.debug", Old: true},
				{Path: "spec.replicas", New: float64(5)},
			},
		},
		{
			name:     "nested change",
			original: map[string]any{"raft": map[string]any{"electionTick": float64(10), "heartbeatTick": float64(1)}},
			modified: map[string]any{"raft": map[string]any{"electionTick": float64(20), "heartbeatTick": float64(1)}},
			want:     []Change{{Path: "spec.raft.electionTick", Old: float64(10), New: float64(20)}},
		},
		{
			name:     "added nested object",
			modified: map[string]any{"raft": map[string]any{"electionTick": float64(10)}},
			want:     []Change{{Path: "spec.raft.electionTick", New: float64(10)}},
		},
		{
			name:     "removed nested object",
			original: map[string]any{"raft": map[string]any{"electionTick": float64(10)}},
			want:     []Change{{Path: "spec.raft.electionTick", Old: float64(10)}},
		},
		{
			name:     "changed list",
			original: map[string]any{"args": []any{"first"}},
			modified: map[string]any{"args": []any{"first", "second"}},
			want:     []Change{{Path: "spec.args", Old: []any{"first"}, New: []any{"first", "second"}}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			require.ElementsMatch(t, test.want, ComputeDiff(test.original, test.modified, "spec"))
		})
	}
}

func TestFormatDiffValues(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		value  any
		leaf   string
		change string
	}{
		{name: "unset", value: nil, leaf: "<nil>", change: "<not set>"},
		{name: "integer", value: float64(3), leaf: "3", change: "3"},
		{name: "fraction", value: 1.5, leaf: "1.5", change: "1.5"},
		{name: "boolean", value: false, leaf: "false", change: "false"},
		{name: "string", value: "ledger", leaf: "ledger", change: "ledger"},
		{name: "list", value: []any{"first", "second"}, leaf: "[first second]", change: "[first second]"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, test.leaf, FormatLeafValue(test.value))
			require.Equal(t, test.change, FormatChangeValue(test.value))
		})
	}
}
