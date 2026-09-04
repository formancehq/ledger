package rootguard

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/scripts/internal/testenv"
)

func TestMain(testingMain *testing.M) {
	if err := testenv.SanitizeProcess(); err != nil {
		panic(err)
	}
	if err := os.Setenv("GIT_CONFIG_GLOBAL", os.DevNull); err != nil {
		panic(err)
	}
	if err := os.Setenv("GIT_CONFIG_SYSTEM", os.DevNull); err != nil {
		panic(err)
	}
	os.Exit(testingMain.Run())
}

func TestSnapshotDetectsProtectedStateChanges(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		mutate func(*testing.T, string)
	}{
		{name: "HEAD", mutate: func(t *testing.T, root string) {
			require.NoError(t, os.WriteFile(filepath.Join(root, "committed.txt"), []byte("committed\n"), 0o644))
			runGit(t, root, "add", "committed.txt")
			runGit(t, root, "commit", "-m", "advance HEAD")
		}},
		{name: "branch", mutate: func(t *testing.T, root string) {
			runGit(t, root, "switch", "-c", "other-branch")
		}},
		{name: "staged tracked content", mutate: func(t *testing.T, root string) {
			require.NoError(t, os.WriteFile(filepath.Join(root, "base.txt"), []byte("staged\n"), 0o644))
			runGit(t, root, "add", "base.txt")
		}},
		{name: "unstaged tracked content", mutate: func(t *testing.T, root string) {
			require.NoError(t, os.WriteFile(filepath.Join(root, "base.txt"), []byte("unstaged\n"), 0o644))
		}},
		{name: "non-ignored untracked content", mutate: func(t *testing.T, root string) {
			require.NoError(t, os.WriteFile(filepath.Join(root, "untracked.txt"), []byte("untracked\n"), 0o644))
		}},
		{name: "intent-to-add index entry", mutate: func(t *testing.T, root string) {
			require.NoError(t, os.WriteFile(filepath.Join(root, "intent-to-add"), nil, 0o644))
			runGit(t, root, "add", "--intent-to-add", "intent-to-add")
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			root := newTestRepository(t)
			before := capture(t, root)
			test.mutate(t, root)
			after := capture(t, root)
			require.Error(t, Compare(before, after))
		})
	}
}

func TestSnapshotIgnoresSharedCacheChurn(t *testing.T) {
	t.Parallel()

	root := newTestRepository(t)
	ignored := filepath.Join(root, "ignored", "cache")
	require.NoError(t, os.WriteFile(ignored, []byte("before"), 0o644))
	before := capture(t, root)
	require.NoError(t, os.WriteFile(ignored, []byte("after"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(root, "ignored", "new"), []byte("new"), 0o644))
	after := capture(t, root)
	require.NoError(t, Compare(before, after))
}

func TestSnapshotFailsClosedOnUntrackedEnumerationAndReadErrors(t *testing.T) {
	t.Parallel()

	t.Run("enumeration", func(t *testing.T) {
		root := newTestRepository(t)
		worker := newTestSnapshotter()
		worker.git = func(directory string, arguments ...string) ([]byte, error) {
			if slices.Equal(arguments, []string{"ls-files", "--others", "--exclude-standard", "-z"}) {
				return nil, errors.New("injected enumeration failure")
			}

			return gitOutput(directory, arguments...)
		}
		_, err := worker.capture(root)
		require.ErrorContains(t, err, "injected enumeration failure")
	})

	for _, test := range []struct {
		name     string
		failOpen bool
	}{{name: "stat"}, {name: "read", failOpen: true}} {
		t.Run(test.name, func(t *testing.T) {
			root := newTestRepository(t)
			require.NoError(t, os.WriteFile(filepath.Join(root, "untracked"), []byte("content"), 0o644))
			worker := newTestSnapshotter()
			baseOpenRoot := worker.openRoot
			worker.openRoot = func(path string) (rootedFilesystem, error) {
				opened, err := baseOpenRoot(path)
				if err != nil || path != root {
					return opened, err
				}

				return errorFilesystem{rootedFilesystem: opened, path: "untracked", failOpen: test.failOpen}, nil
			}
			_, err := worker.capture(root)
			require.ErrorContains(t, err, "injected "+test.name+" failure")
		})
	}
}

func TestSnapshotUsesSixGitProcessesAndSkipsIgnoredEntries(t *testing.T) {
	t.Parallel()

	root := newTestRepository(t)
	for index := range 200 {
		path := filepath.Join(root, "ignored", fmt.Sprintf("file-%03d", index))
		require.NoError(t, os.WriteFile(path, []byte("content"), 0o644))
	}
	require.NoError(t, os.WriteFile(filepath.Join(root, "untracked"), []byte("visible"), 0o644))

	snapshot := capture(t, root)
	require.Equal(t, 1, snapshot.Metrics.UntrackedEntries)
	require.Equal(t, 1, snapshot.Metrics.UntrackedRegularFiles)
	require.Equal(t, int64(7), snapshot.Metrics.UntrackedLogicalBytes)
	require.Equal(t, GitProcessesPerSnapshot(), snapshot.Metrics.GitProcesses)
	require.Equal(t, 6, snapshot.Metrics.GitProcesses)
}

func BenchmarkCapture(b *testing.B) {
	root := os.Getenv("ROOTGUARD_BENCH_ROOT")
	if root == "" {
		b.Skip("set ROOTGUARD_BENCH_ROOT to benchmark a representative trusted root")
	}
	snapshot, err := Capture(root)
	require.NoError(b, err)
	b.ReportMetric(float64(snapshot.Metrics.UntrackedEntries), "untracked-entries")
	b.ReportMetric(float64(snapshot.Metrics.GitProcesses), "git-processes/snapshot")
	b.ReportMetric(0, "ignored-entries")
	b.ResetTimer()
	for range b.N {
		_, err := Capture(root)
		require.NoError(b, err)
	}
}

type errorFilesystem struct {
	rootedFilesystem

	path     string
	failOpen bool
}

func (filesystem errorFilesystem) Lstat(name string) (os.FileInfo, error) {
	if name == filesystem.path && !filesystem.failOpen {
		return nil, errors.New("injected stat failure")
	}

	return filesystem.rootedFilesystem.Lstat(name)
}

func (filesystem errorFilesystem) Open(name string) (rootedFile, error) {
	if name == filesystem.path && filesystem.failOpen {
		return nil, errors.New("injected read failure")
	}

	return filesystem.rootedFilesystem.Open(name)
}

func newTestSnapshotter() snapshotter {
	return snapshotter{
		git: gitOutput,
		openRoot: func(path string) (rootedFilesystem, error) {
			opened, err := os.OpenRoot(path)
			if err != nil {
				return nil, err
			}

			return osRoot{root: opened}, nil
		},
	}
}

func newTestRepository(t *testing.T) string {
	t.Helper()
	root := filepath.Join(t.TempDir(), "root")
	require.NoError(t, os.Mkdir(root, 0o755))
	runGit(t, root, "init")
	runGit(t, root, "config", "user.name", "Root Guard Test")
	runGit(t, root, "config", "user.email", "rootguard@example.com")
	require.NoError(t, os.WriteFile(filepath.Join(root, ".gitignore"), []byte("ignored/\n"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(root, "base.txt"), []byte("base\n"), 0o644))
	require.NoError(t, os.Mkdir(filepath.Join(root, "ignored"), 0o755))
	runGit(t, root, "add", ".gitignore", "base.txt")
	runGit(t, root, "commit", "-m", "base")

	return root
}

func capture(t *testing.T, root string) Snapshot {
	t.Helper()
	snapshot, err := Capture(root)
	require.NoError(t, err)

	return snapshot
}

func runGit(t *testing.T, directory string, arguments ...string) {
	t.Helper()
	command := testenv.Command(t, "git", arguments...)
	command.Dir = directory
	output, err := command.CombinedOutput()
	require.NoError(t, err, "git %s:\n%s", strings.Join(arguments, " "), output)
}
