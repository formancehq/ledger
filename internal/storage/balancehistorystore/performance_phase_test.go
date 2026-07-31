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

	perfCaseEffective1D  = "effective-1d"
	perfCaseEffective6Mo = "effective-6mo"
	perfCaseEffective2Y  = "effective-2y"
	perfCaseInsertion1D  = "insertion-1d"
	perfCaseInsertion6Mo = "insertion-6mo"
	perfCaseInsertion2Y  = "insertion-2y"
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
	perfCaseOrder = []string{
		perfCaseEffective1D,
		perfCaseEffective6Mo,
		perfCaseEffective2Y,
		perfCaseInsertion1D,
		perfCaseInsertion6Mo,
		perfCaseInsertion2Y,
	}
)

type perfSelection struct {
	all      bool
	selected map[string]struct{}
	order    []string
}

func parsePerfPhaseSelection(raw string) (perfSelection, error) {
	return parsePerfSelection(raw, "phase", perfPhaseOrder)
}

func parsePerfCaseSelection(raw string) (perfSelection, error) {
	return parsePerfSelection(raw, "case", perfCaseOrder)
}

func parsePerfSelection(raw, kind string, order []string) (perfSelection, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" || raw == "all" {
		return perfSelection{all: true, order: order}, nil
	}

	selection := perfSelection{selected: make(map[string]struct{}), order: order}
	for value := range strings.SplitSeq(raw, ",") {
		name := strings.TrimSpace(value)
		if name == "" || !slices.Contains(order, name) {
			return perfSelection{}, fmt.Errorf(
				"unknown PIT performance %s %q; expected all or one of %s",
				kind,
				name,
				strings.Join(order, ", "),
			)
		}
		selection.selected[name] = struct{}{}
	}

	return selection, nil
}

func (s perfSelection) Includes(name string) bool {
	if s.all {
		return true
	}
	_, ok := s.selected[name]

	return ok
}

func (s perfSelection) IncludesAny(names ...string) bool {
	return slices.ContainsFunc(names, s.Includes)
}

func (s perfSelection) Names() []string {
	if s.all {
		return []string{"all"}
	}

	names := make([]string, 0, len(s.selected))
	for _, name := range s.order {
		if s.Includes(name) {
			names = append(names, name)
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

func TestParsePerfCaseSelection(t *testing.T) {
	t.Parallel()

	all, err := parsePerfCaseSelection("")
	require.NoError(t, err)
	require.True(t, all.Includes("effective-1d"))
	require.Equal(t, []string{"all"}, all.Names())

	selected, err := parsePerfCaseSelection("insertion-2y,effective-6mo")
	require.NoError(t, err)
	require.True(t, selected.Includes("effective-6mo"))
	require.True(t, selected.Includes("insertion-2y"))
	require.False(t, selected.Includes("effective-1d"))
	require.Equal(t, []string{"effective-6mo", "insertion-2y"}, selected.Names())
}

func TestParsePerfCaseSelectionRejectsUnknownCase(t *testing.T) {
	t.Parallel()

	_, err := parsePerfCaseSelection("effective-1d,unknown")
	require.ErrorContains(t, err, "unknown PIT performance case")
}
