package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFixpointRerunsOnlyComponentInvalidatedByNormalization(t *testing.T) {
	t.Parallel()

	repository := newFixpointRepository(t)
	components := []component{
		{Name: "consumer", Inputs: exactPath("consumer.in"), Outputs: exactPath("consumer.out"), Config: noPath, Complete: true},
		{Name: "producer", Inputs: exactPath("producer.in"), Outputs: exactPath("producer.out"), Config: noPath, Complete: true},
	}
	counts := map[string]int{}
	r := runner{repo: repository, toolRoot: repository, components: components}
	r.execute = func(item component) error {
		counts[item.Name]++
		switch item.Name {
		case "producer":
			return os.WriteFile(filepath.Join(repository, "consumer.in"), []byte("normalized\n"), 0o644)
		case "consumer":
			return os.WriteFile(filepath.Join(repository, "consumer.out"), []byte("generated\n"), 0o644)
		default:
			return nil
		}
	}
	plan := fixpointPlan(t, r, map[string][]string{"producer": {"input_changed:producer.in"}})
	require.NoError(t, r.runFixpoint(plan))
	require.Equal(t, map[string]int{"producer": 1, "consumer": 1}, counts)
}

func TestCleanFirstPassDoesNotReplayComponent(t *testing.T) {
	t.Parallel()

	repository := newFixpointRepository(t)
	item := component{Name: "clean", Inputs: exactPath("producer.in"), Outputs: noPath, Config: noPath, Complete: true}
	runs := 0
	r := runner{repo: repository, toolRoot: repository, components: []component{item}}
	r.execute = func(component) error {
		runs++

		return nil
	}
	plan := fixpointPlan(t, r, map[string][]string{"clean": {"input_changed:producer.in"}})
	require.NoError(t, r.runFixpoint(plan))
	require.Equal(t, 1, runs, "an exact clean pass is already the fixpoint proof")
}

func TestUnmappedNormalizerEffectForcesFullFallbackPass(t *testing.T) {
	t.Parallel()

	repository := newFixpointRepository(t)
	components := []component{
		{Name: "first", Inputs: exactPath("producer.in"), Outputs: noPath, Config: noPath, Complete: true},
		{Name: "second", Inputs: exactPath("consumer.in"), Outputs: noPath, Config: noPath, Complete: true},
	}
	counts := map[string]int{}
	r := runner{repo: repository, toolRoot: repository, components: components}
	r.execute = func(item component) error {
		counts[item.Name]++
		if item.Name == "first" && counts[item.Name] == 1 {
			return os.WriteFile(filepath.Join(repository, "unmapped.output"), []byte("unexpected\n"), 0o644)
		}

		return nil
	}
	plan := fixpointPlan(t, r, map[string][]string{"first": {"input_changed:producer.in"}})
	require.NoError(t, r.runFixpoint(plan))
	require.Equal(t, map[string]int{"first": 2, "second": 1}, counts,
		"an unknown relationship must run the complete component set")
}

func TestWorkspaceChangeAfterSelectionFailsClosed(t *testing.T) {
	t.Parallel()

	repository := newFixpointRepository(t)
	item := component{Name: "clean", Inputs: exactPath("producer.in"), Outputs: noPath, Config: noPath, Complete: true}
	r := runner{repo: repository, toolRoot: repository, components: []component{item}}
	selectedFingerprint, err := r.workspaceFingerprint()
	require.NoError(t, err)
	writeFixtureFile(t, repository, "late-change.go", "package late\n")
	plan := selection{
		CandidateFingerprint: selectedFingerprint,
		Selected:             map[string][]string{"clean": {"input_changed:producer.in"}},
	}
	err = r.runFixpoint(plan)
	require.EqualError(t, err, "candidate workspace changed after component selection")
}

func newFixpointRepository(t *testing.T) string {
	t.Helper()

	repository := t.TempDir()
	runGit(t, repository, "init", "-q")
	runGit(t, repository, "config", "user.name", "Fixpoint Test")
	runGit(t, repository, "config", "user.email", "fixpoint@example.com")
	writeFixtureFile(t, repository, "producer.in", "source\n")
	writeFixtureFile(t, repository, "consumer.in", "old\n")
	runGit(t, repository, "add", ".")
	runGit(t, repository, "commit", "-qm", "base")

	return repository
}

func exactPath(want string) func(string) bool {
	return func(path string) bool { return path == want }
}

func fixpointPlan(t *testing.T, r runner, selected map[string][]string) selection {
	t.Helper()
	fingerprint, err := r.workspaceFingerprint()
	require.NoError(t, err)

	return selection{CandidateFingerprint: fingerprint, Selected: selected}
}
