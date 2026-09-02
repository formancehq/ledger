// Package rootguard captures content-aware snapshots of a trusted Git worktree.
package rootguard

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
)

const gitProcessesPerSnapshot = 9

// Metrics describes the full-content worktree scope captured in a snapshot.
type Metrics struct {
	GitProcesses          int
	Entries               int
	RegularFiles          int
	LogicalBytes          int64
	UntrackedEntries      int
	UntrackedRegularFiles int
	UntrackedLogicalBytes int64
	IgnoredEntries        int
	IgnoredRegularFiles   int
	IgnoredLogicalBytes   int64
}

// Snapshot is the trusted state compared at guard observation boundaries.
type Snapshot struct {
	Head                 string
	Branch               string
	Status               []byte
	WorkspaceFingerprint string
	Metrics              Metrics
}

type gitCommand func(directory string, arguments ...string) ([]byte, error)

type rootedFile interface {
	io.Reader
	Stat() (os.FileInfo, error)
	Close() error
}

type rootedFilesystem interface {
	Lstat(name string) (os.FileInfo, error)
	Open(name string) (rootedFile, error)
	Readlink(name string) (string, error)
	Close() error
}

type osRoot struct {
	root *os.Root
}

func (root osRoot) Lstat(name string) (os.FileInfo, error) {
	return root.root.Lstat(name)
}

func (root osRoot) Open(name string) (rootedFile, error) {
	return root.root.Open(name)
}

func (root osRoot) Readlink(name string) (string, error) {
	return root.root.Readlink(name)
}

func (root osRoot) Close() error {
	return root.root.Close()
}

type snapshotter struct {
	git      gitCommand
	openRoot func(string) (rootedFilesystem, error)
	metrics  Metrics
}

// Capture takes one deterministic, full-content snapshot of root.
func Capture(root string) (Snapshot, error) {
	worker := snapshotter{
		git: gitOutput,
		openRoot: func(path string) (rootedFilesystem, error) {
			opened, err := os.OpenRoot(path)
			if err != nil {
				return nil, err
			}

			return osRoot{root: opened}, nil
		},
	}

	return worker.capture(root)
}

// Compare verifies that two observation boundaries describe the same trusted
// worktree state.
func Compare(expected, current Snapshot) error {
	if current.Head != expected.Head {
		return fmt.Errorf("root HEAD changed: got %s, expected %s", current.Head, expected.Head)
	}
	if current.Branch != expected.Branch {
		return fmt.Errorf("root branch changed: got %s, expected %s", current.Branch, expected.Branch)
	}
	if !bytes.Equal(current.Status, expected.Status) {
		return errors.New("root status changed")
	}
	if current.WorkspaceFingerprint != expected.WorkspaceFingerprint {
		return errors.New("root workspace content changed")
	}

	return nil
}

// StatusSHA256 returns a display-safe digest of the NUL-delimited porcelain
// status captured in the snapshot.
func (snapshot Snapshot) StatusSHA256() string {
	digest := sha256.Sum256(snapshot.Status)

	return hex.EncodeToString(digest[:])
}

func (worker *snapshotter) capture(root string) (Snapshot, error) {
	if !filepath.IsAbs(root) {
		return Snapshot{}, errors.New("trusted root path is not absolute")
	}
	rootFS, err := worker.openRoot(root)
	if err != nil {
		return Snapshot{}, fmt.Errorf("opening trusted root: %w", err)
	}
	defer func() {
		_ = rootFS.Close() // Capture errors take precedence over best-effort descriptor cleanup.
	}()

	head, err := worker.runGit(root, "rev-parse", "HEAD")
	if err != nil {
		return Snapshot{}, fmt.Errorf("capturing ROOT_HEAD: %w", err)
	}
	branch, err := worker.runGit(root, "rev-parse", "--abbrev-ref", "HEAD")
	if err != nil {
		return Snapshot{}, fmt.Errorf("capturing ROOT_BRANCH: %w", err)
	}
	status, err := worker.runGit(root, "status", "--porcelain=v1", "-z", "--untracked-files=all")
	if err != nil {
		return Snapshot{}, fmt.Errorf("capturing ROOT_STATUS: %w", err)
	}
	stagedDiff, err := worker.runGit(root, "diff", "--cached", "--binary", "--full-index", "HEAD", "--")
	if err != nil {
		return Snapshot{}, fmt.Errorf("reading staged root diff: %w", err)
	}
	unstagedDiff, err := worker.runGit(root, "diff", "--binary", "--full-index", "--")
	if err != nil {
		return Snapshot{}, fmt.Errorf("reading unstaged root diff: %w", err)
	}
	untracked, err := worker.runGit(root, "ls-files", "--others", "--exclude-standard", "-z")
	if err != nil {
		return Snapshot{}, fmt.Errorf("listing untracked root paths: %w", err)
	}
	ignored, err := worker.runGit(root, "ls-files", "--others", "--ignored", "--exclude-standard", "-z")
	if err != nil {
		return Snapshot{}, fmt.Errorf("listing ignored root paths: %w", err)
	}
	infoExclude, err := worker.runGit(root, "rev-parse", "--path-format=absolute", "--git-path", "info/exclude")
	if err != nil {
		return Snapshot{}, fmt.Errorf("resolving repository exclude file: %w", err)
	}
	globalExcludes, err := worker.runOptionalGit(root, "config", "--null", "--path", "--get-all", "core.excludesFile")
	if err != nil {
		return Snapshot{}, fmt.Errorf("resolving configured exclude files: %w", err)
	}

	hasher := sha256.New()
	writeHashField(hasher, []byte("ledger-rootguard-v1"))
	writeHashField(hasher, bytes.TrimSpace(head))
	writeHashField(hasher, stagedDiff)
	writeHashField(hasher, unstagedDiff)
	if err := worker.hashGitPaths(hasher, rootFS, "untracked", untracked); err != nil {
		return Snapshot{}, err
	}
	if err := worker.hashGitPaths(hasher, rootFS, "ignored", ignored); err != nil {
		return Snapshot{}, err
	}
	writeHashField(hasher, []byte("ignore-configuration"))
	infoExcludePath := strings.TrimSuffix(string(infoExclude), "\n")
	if err := worker.hashExternalPath(hasher, root, infoExcludePath); err != nil {
		return Snapshot{}, fmt.Errorf("hashing repository exclude file: %w", err)
	}
	configuredPaths, err := splitNUL(globalExcludes)
	if err != nil {
		return Snapshot{}, fmt.Errorf("parsing configured exclude files: %w", err)
	}
	slices.SortFunc(configuredPaths, bytes.Compare)
	for _, configuredPath := range configuredPaths {
		if err := worker.hashExternalPath(hasher, root, string(configuredPath)); err != nil {
			return Snapshot{}, fmt.Errorf("hashing configured exclude file %q: %w", configuredPath, err)
		}
	}

	return Snapshot{
		Head:                 strings.TrimSpace(string(head)),
		Branch:               strings.TrimSpace(string(branch)),
		Status:               status,
		WorkspaceFingerprint: hex.EncodeToString(hasher.Sum(nil)),
		Metrics:              worker.metrics,
	}, nil
}

func (worker *snapshotter) hashGitPaths(hasher hash.Hash, root rootedFilesystem, class string, output []byte) error {
	paths, err := splitNUL(output)
	if err != nil {
		return fmt.Errorf("parsing %s root paths: %w", class, err)
	}
	slices.SortFunc(paths, bytes.Compare)
	writeHashField(hasher, []byte(class))
	for _, rawPath := range paths {
		path := filepath.FromSlash(string(rawPath))
		if !filepath.IsLocal(path) || path == "." {
			return fmt.Errorf("%s path is not local: %q", class, rawPath)
		}
		if err := worker.hashPath(hasher, root, path, rawPath, class); err != nil {
			return fmt.Errorf("hashing %s path %q: %w", class, rawPath, err)
		}
	}

	return nil
}

func (worker *snapshotter) hashExternalPath(hasher hash.Hash, repositoryRoot, path string) error {
	writeHashField(hasher, []byte(path))
	if path == "" {
		writeHashField(hasher, []byte("missing"))

		return nil
	}
	if !filepath.IsAbs(path) {
		path = filepath.Join(repositoryRoot, path)
	}
	directory, name := filepath.Split(filepath.Clean(path))
	if !filepath.IsLocal(name) || name == "." {
		return fmt.Errorf("exclude path has invalid final component: %q", path)
	}
	root, err := worker.openRoot(directory)
	if errors.Is(err, os.ErrNotExist) {
		writeHashField(hasher, []byte("missing"))

		return nil
	}
	if err != nil {
		return err
	}
	defer func() {
		_ = root.Close() // Hashing errors take precedence over best-effort descriptor cleanup.
	}()

	return worker.hashPath(hasher, root, name, []byte(name), "")
}

func (worker *snapshotter) hashPath(
	hasher hash.Hash,
	root rootedFilesystem,
	path string,
	rawPath []byte,
	metricsClass string,
) error {
	info, err := root.Lstat(path)
	if errors.Is(err, os.ErrNotExist) && metricsClass == "" {
		writeHashField(hasher, []byte("missing"))

		return nil
	}
	if err != nil {
		return fmt.Errorf("reading path metadata: %w", err)
	}
	writeHashField(hasher, rawPath)
	writeHashField(hasher, []byte(info.Mode().String()))
	if metricsClass != "" {
		worker.metrics.Entries++
		switch metricsClass {
		case "untracked":
			worker.metrics.UntrackedEntries++
		case "ignored":
			worker.metrics.IgnoredEntries++
		}
	}

	switch {
	case info.Mode()&os.ModeSymlink != 0:
		writeHashField(hasher, []byte("symlink"))
		target, err := root.Readlink(path)
		if err != nil {
			return fmt.Errorf("reading symlink target: %w", err)
		}
		writeHashField(hasher, []byte(target))
	case info.Mode().IsRegular():
		writeHashField(hasher, []byte("regular"))
		file, err := root.Open(path)
		if err != nil {
			return fmt.Errorf("opening regular file: %w", err)
		}
		contentHasher := sha256.New()
		logicalBytes, copyErr := io.Copy(contentHasher, file)
		openedInfo, statErr := file.Stat()
		closeErr := file.Close()
		if copyErr != nil {
			return fmt.Errorf("reading regular file: %w", copyErr)
		}
		if statErr != nil {
			return fmt.Errorf("checking regular file after read: %w", statErr)
		}
		if closeErr != nil {
			return fmt.Errorf("closing regular file after read: %w", closeErr)
		}
		if !openedInfo.Mode().IsRegular() || !os.SameFile(info, openedInfo) || openedInfo.Mode() != info.Mode() || openedInfo.Size() != logicalBytes {
			return errors.New("regular file changed type or identity while being read")
		}
		writeHashField(hasher, contentHasher.Sum(nil))
		if metricsClass != "" {
			worker.metrics.RegularFiles++
			worker.metrics.LogicalBytes += logicalBytes
			switch metricsClass {
			case "untracked":
				worker.metrics.UntrackedRegularFiles++
				worker.metrics.UntrackedLogicalBytes += logicalBytes
			case "ignored":
				worker.metrics.IgnoredRegularFiles++
				worker.metrics.IgnoredLogicalBytes += logicalBytes
			}
		}
	case info.IsDir():
		writeHashField(hasher, []byte("directory"))
	default:
		writeHashField(hasher, []byte("special"))
	}

	return nil
}

func (worker *snapshotter) runGit(directory string, arguments ...string) ([]byte, error) {
	worker.metrics.GitProcesses++

	return worker.git(directory, arguments...)
}

func (worker *snapshotter) runOptionalGit(directory string, arguments ...string) ([]byte, error) {
	output, err := worker.runGit(directory, arguments...)
	if err == nil {
		return output, nil
	}
	var exitError *exec.ExitError
	if errors.As(err, &exitError) && exitError.ExitCode() == 1 {
		return nil, nil
	}

	return nil, err
}

func splitNUL(output []byte) ([][]byte, error) {
	if len(output) == 0 {
		return nil, nil
	}
	if output[len(output)-1] != 0 {
		return nil, errors.New("NUL-delimited Git output lacks final delimiter")
	}
	parts := bytes.Split(output[:len(output)-1], []byte{0})
	for _, part := range parts {
		if len(part) == 0 {
			return nil, errors.New("NUL-delimited Git output contains an empty path")
		}
	}

	return parts, nil
}

func writeHashField(hasher hash.Hash, value []byte) {
	var length [8]byte
	binary.BigEndian.PutUint64(length[:], uint64(len(value)))
	// hash.Hash.Write is specified to never return an error.
	_, _ = hasher.Write(length[:])
	_, _ = hasher.Write(value)
}

func gitOutput(directory string, arguments ...string) ([]byte, error) {
	command := exec.Command("git", arguments...)
	command.Dir = directory
	output, err := command.Output()
	if err != nil {
		var exitError *exec.ExitError
		if errors.As(err, &exitError) {
			return nil, fmt.Errorf("git %s: %w: %s", strings.Join(arguments, " "), err, strings.TrimSpace(string(exitError.Stderr)))
		}

		return nil, fmt.Errorf("git %s: %w", strings.Join(arguments, " "), err)
	}

	return output, nil
}

// GitProcessesPerSnapshot returns the fixed upper bound used by Capture.
func GitProcessesPerSnapshot() int {
	return gitProcessesPerSnapshot
}
