package rootguard

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMain(testingMain *testing.M) {
	if err := os.Setenv("GIT_CONFIG_GLOBAL", os.DevNull); err != nil {
		panic(err)
	}
	if err := os.Setenv("GIT_CONFIG_SYSTEM", os.DevNull); err != nil {
		panic(err)
	}
	os.Exit(testingMain.Run())
}

func TestSnapshotDetectsGitAndWorkspaceStateChanges(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		mutate func(*testing.T, string)
	}{
		{
			name: "HEAD",
			mutate: func(t *testing.T, root string) {
				require.NoError(t, os.WriteFile(filepath.Join(root, "committed.txt"), []byte("committed\n"), 0o644))
				runGit(t, root, "add", "committed.txt")
				runGit(t, root, "commit", "-m", "advance HEAD")
			},
		},
		{
			name: "branch",
			mutate: func(t *testing.T, root string) {
				runGit(t, root, "switch", "-c", "other-branch")
			},
		},
		{
			name: "staged diff",
			mutate: func(t *testing.T, root string) {
				require.NoError(t, os.WriteFile(filepath.Join(root, "base.txt"), []byte("staged\n"), 0o644))
				runGit(t, root, "add", "base.txt")
			},
		},
		{
			name: "unstaged diff",
			mutate: func(t *testing.T, root string) {
				require.NoError(t, os.WriteFile(filepath.Join(root, "base.txt"), []byte("unstaged\n"), 0o644))
			},
		},
		{
			name: "non-ignored untracked file",
			mutate: func(t *testing.T, root string) {
				require.NoError(t, os.WriteFile(filepath.Join(root, "untracked.txt"), []byte("untracked\n"), 0o644))
			},
		},
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

func TestSnapshotDetectsAdversarialIgnoredPathChanges(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		prepare func(*testing.T, string) string
		mutate  func(*testing.T, string, string)
	}{
		{
			name: "same-size content with restored mtime",
			prepare: func(t *testing.T, root string) string {
				path := filepath.Join(root, "ignored", "same-size")
				require.NoError(t, os.WriteFile(path, []byte("before"), 0o644))

				return path
			},
			mutate: func(t *testing.T, _ string, path string) {
				info, err := os.Stat(path)
				require.NoError(t, err)
				require.NoError(t, os.WriteFile(path, []byte("after!"), 0o644))
				require.NoError(t, os.Chtimes(path, info.ModTime(), info.ModTime()))
			},
		},
		{
			name:    "addition",
			prepare: func(*testing.T, string) string { return "" },
			mutate: func(t *testing.T, root, _ string) {
				require.NoError(t, os.WriteFile(filepath.Join(root, "ignored", "added"), []byte("added"), 0o644))
			},
		},
		{
			name: "deletion",
			prepare: func(t *testing.T, root string) string {
				path := filepath.Join(root, "ignored", "deleted")
				require.NoError(t, os.WriteFile(path, []byte("deleted"), 0o644))

				return path
			},
			mutate: func(t *testing.T, _ string, path string) {
				require.NoError(t, os.Remove(path))
			},
		},
		{
			name: "rename",
			prepare: func(t *testing.T, root string) string {
				path := filepath.Join(root, "ignored", "old-name")
				require.NoError(t, os.WriteFile(path, []byte("renamed"), 0o644))

				return path
			},
			mutate: func(t *testing.T, root, path string) {
				require.NoError(t, os.Rename(path, filepath.Join(root, "ignored", "new-name")))
			},
		},
		{
			name: "regular file to symlink",
			prepare: func(t *testing.T, root string) string {
				path := filepath.Join(root, "ignored", "node")
				require.NoError(t, os.WriteFile(path, []byte("target"), 0o644))

				return path
			},
			mutate: func(t *testing.T, _ string, path string) {
				require.NoError(t, os.Remove(path))
				require.NoError(t, os.Symlink("target", path))
			},
		},
		{
			name: "symlink target",
			prepare: func(t *testing.T, root string) string {
				path := filepath.Join(root, "ignored", "link")
				require.NoError(t, os.Symlink("before", path))

				return path
			},
			mutate: func(t *testing.T, _ string, path string) {
				require.NoError(t, os.Remove(path))
				require.NoError(t, os.Symlink("after", path))
			},
		},
		{
			name: "directory to regular file",
			prepare: func(t *testing.T, root string) string {
				path := filepath.Join(root, "ignored", "directory")
				require.NoError(t, os.Mkdir(path, 0o755))
				runGit(t, path, "init")

				return path
			},
			mutate: func(t *testing.T, _ string, path string) {
				require.NoError(t, os.RemoveAll(path))
				require.NoError(t, os.WriteFile(path, []byte("file"), 0o644))
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			root := newTestRepository(t)
			path := test.prepare(t, root)
			before := capture(t, root)
			test.mutate(t, root, path)
			after := capture(t, root)
			require.ErrorContains(t, Compare(before, after), "root workspace content changed")
		})
	}
}

func TestSnapshotHandlesUnusualFilenameBytes(t *testing.T) {
	t.Parallel()

	root := newTestRepository(t)
	path := filepath.Join(root, "ignored", "space and\nnewline")
	require.NoError(t, os.WriteFile(path, []byte("before"), 0o644))
	before := capture(t, root)
	require.NoError(t, os.WriteFile(path, []byte("after!"), 0o644))
	after := capture(t, root)
	require.ErrorContains(t, Compare(before, after), "root workspace content changed")
}

func TestSnapshotHandlesNonUTF8FilenameBytesWhereSupported(t *testing.T) {
	t.Parallel()

	if runtime.GOOS != "linux" {
		t.Skip("os.Root rejects non-UTF-8 path bytes on this platform")
	}
	root := newTestRepository(t)
	path := filepath.Join(root, "ignored", string([]byte{'n', 'o', 'n', '-', 0xff, '-', 'u', 't', 'f', '8'}))
	require.NoError(t, os.WriteFile(path, []byte("before"), 0o644))
	before := capture(t, root)
	require.NoError(t, os.WriteFile(path, []byte("after!"), 0o644))
	after := capture(t, root)
	require.ErrorContains(t, Compare(before, after), "root workspace content changed")
}

func TestSnapshotPreservesIgnoredNestedRepositoryBoundary(t *testing.T) {
	t.Parallel()

	root := newTestRepository(t)
	nested := filepath.Join(root, "ignored", "nested-repository")
	require.NoError(t, os.Mkdir(nested, 0o755))
	runGit(t, nested, "init")
	require.NoError(t, os.WriteFile(filepath.Join(nested, "inside"), []byte("before"), 0o644))

	before := capture(t, root)
	require.NoError(t, os.WriteFile(filepath.Join(nested, "inside"), []byte("after!"), 0o644))
	after := capture(t, root)
	require.NoError(t, Compare(before, after), "the parent guard intentionally treats an ignored nested repository as a separate root")
}

func TestSnapshotDetectsTrustedIgnoreConfigurationMutation(t *testing.T) {
	t.Parallel()

	root := newTestRepository(t)
	before := capture(t, root)
	excludePath := filepath.Join(root, ".git", "info", "exclude")
	require.NoError(t, os.WriteFile(excludePath, []byte("# semantically inert mutation\n"), 0o644))
	after := capture(t, root)
	require.ErrorContains(t, Compare(before, after), "root workspace content changed")
}

func TestSnapshotFailsClosedOnEnumerationStatAndReadErrors(t *testing.T) {
	t.Parallel()

	t.Run("enumeration", func(t *testing.T) {
		root := newTestRepository(t)
		worker := newTestSnapshotter()
		worker.git = func(directory string, arguments ...string) ([]byte, error) {
			if slices.Equal(arguments, []string{"ls-files", "--others", "--ignored", "--exclude-standard", "-z"}) {
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
	}{
		{name: "stat"},
		{name: "read", failOpen: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			root := newTestRepository(t)
			path := filepath.Join(root, "ignored", "unreadable")
			require.NoError(t, os.WriteFile(path, []byte("content"), 0o644))
			worker := newTestSnapshotter()
			baseOpenRoot := worker.openRoot
			worker.openRoot = func(path string) (rootedFilesystem, error) {
				opened, err := baseOpenRoot(path)
				if err != nil || path != root {
					return opened, err
				}

				return errorFilesystem{
					rootedFilesystem: opened,
					path:             filepath.Join("ignored", "unreadable"),
					failOpen:         test.failOpen,
				}, nil
			}
			_, err := worker.capture(root)
			require.ErrorContains(t, err, "injected "+test.name+" failure")
		})
	}
}

func TestSnapshotUsesBoundedGitProcessesIndependentOfEntryCount(t *testing.T) {
	t.Parallel()

	root := newTestRepository(t)
	for index := range 200 {
		path := filepath.Join(root, "ignored", fmt.Sprintf("file-%03d", index))
		require.NoError(t, os.WriteFile(path, []byte("content"), 0o644))
	}

	snapshot := capture(t, root)
	require.Equal(t, 200, snapshot.Metrics.Entries)
	require.Equal(t, 200, snapshot.Metrics.RegularFiles)
	require.Equal(t, int64(1400), snapshot.Metrics.LogicalBytes)
	require.Equal(t, 200, snapshot.Metrics.IgnoredEntries)
	require.Equal(t, 200, snapshot.Metrics.IgnoredRegularFiles)
	require.Equal(t, int64(1400), snapshot.Metrics.IgnoredLogicalBytes)
	require.Equal(t, GitProcessesPerSnapshot(), snapshot.Metrics.GitProcesses)
	require.Equal(t, 9, snapshot.Metrics.GitProcesses)
}

func BenchmarkCapture(b *testing.B) {
	root := os.Getenv("ROOTGUARD_BENCH_ROOT")
	if root == "" {
		b.Skip("set ROOTGUARD_BENCH_ROOT to benchmark a representative trusted root")
	}
	snapshot, err := Capture(root)
	require.NoError(b, err)
	b.ReportMetric(float64(snapshot.Metrics.IgnoredEntries), "ignored-entries")
	b.ReportMetric(float64(snapshot.Metrics.IgnoredRegularFiles), "ignored-regular-files")
	b.ReportMetric(float64(snapshot.Metrics.IgnoredLogicalBytes), "ignored-logical-bytes")
	b.ReportMetric(float64(snapshot.Metrics.GitProcesses), "git-processes/snapshot")
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
	command := exec.Command("git", arguments...)
	command.Dir = directory
	output, err := command.CombinedOutput()
	require.NoError(t, err, "git %s:\n%s", strings.Join(arguments, " "), output)
}
