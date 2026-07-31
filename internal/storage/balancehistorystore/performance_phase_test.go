package balancehistorystore_test

import (
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	perfPhaseHotUnfiltered  = "hot-unfiltered"
	perfPhaseHotFiltered    = "hot-filtered"
	perfPhaseHotGrouped     = "hot-grouped"
	perfPhaseHotShapes      = "hot-shapes"
	perfPhaseCompaction     = "compaction"
	perfPhaseReplicaDigest  = "replica-digest"
	perfPhaseColdUnfiltered = "cold-unfiltered"
	perfPhaseColdFiltered   = "cold-filtered"
	perfPhaseColdGrouped    = "cold-grouped"
	perfPhaseCardinality    = "cardinality"
	perfPhaseBackdating     = "backdating"
	perfPhaseWrite          = "write"
)

var (
	perfPhaseOrder = []string{
		perfPhaseHotUnfiltered,
		perfPhaseHotFiltered,
		perfPhaseHotGrouped,
		perfPhaseHotShapes,
		perfPhaseCompaction,
		perfPhaseReplicaDigest,
		perfPhaseColdUnfiltered,
		perfPhaseColdFiltered,
		perfPhaseColdGrouped,
		perfPhaseCardinality,
		perfPhaseBackdating,
		perfPhaseWrite,
	}
)

type perfPhaseSelection struct {
	all      bool
	selected map[string]struct{}
}

func parsePerfPhaseSelection(raw string) (perfPhaseSelection, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" || raw == "all" {
		return perfPhaseSelection{all: true}, nil
	}

	selection := perfPhaseSelection{selected: make(map[string]struct{})}
	for value := range strings.SplitSeq(raw, ",") {
		phase := strings.TrimSpace(value)
		if phase == "" || !slices.Contains(perfPhaseOrder, phase) {
			return perfPhaseSelection{}, fmt.Errorf(
				"unknown PIT performance phase %q; expected all or one of %s",
				phase,
				strings.Join(perfPhaseOrder, ", "),
			)
		}
		selection.selected[phase] = struct{}{}
	}

	return selection, nil
}

func (s perfPhaseSelection) Includes(phase string) bool {
	if s.all {
		return true
	}
	_, ok := s.selected[phase]

	return ok
}

func (s perfPhaseSelection) IncludesAny(phases ...string) bool {
	return slices.ContainsFunc(phases, s.Includes)
}

func (s perfPhaseSelection) Names() []string {
	if s.all {
		return []string{"all"}
	}

	names := make([]string, 0, len(s.selected))
	for _, phase := range perfPhaseOrder {
		if s.Includes(phase) {
			names = append(names, phase)
		}
	}

	return names
}

func TestParsePerfPhaseSelection(t *testing.T) {
	t.Parallel()

	all, err := parsePerfPhaseSelection("")
	require.NoError(t, err)
	require.True(t, all.Includes(perfPhaseHotGrouped))
	require.True(t, all.Includes(perfPhaseWrite))
	require.Equal(t, []string{"all"}, all.Names())

	selected, err := parsePerfPhaseSelection("write,hot-grouped,write")
	require.NoError(t, err)
	require.True(t, selected.Includes(perfPhaseHotGrouped))
	require.True(t, selected.Includes(perfPhaseWrite))
	require.False(t, selected.Includes(perfPhaseColdGrouped))
	require.Equal(t, []string{perfPhaseHotGrouped, perfPhaseWrite}, selected.Names())
}

func TestParsePerfPhaseSelectionRejectsUnknownPhase(t *testing.T) {
	t.Parallel()

	_, err := parsePerfPhaseSelection("hot-grouped,unknown")
	require.ErrorContains(t, err, "unknown PIT performance phase")
}
