// Package rootguard captures the cooperative safety boundary around a Git worktree.
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

const gitProcessesPerSnapshot = 6

// Metrics describes the non-ignored untracked content captured in a snapshot.
type Metrics struct {
	GitProcesses          int
	UntrackedEntries      int
	UntrackedRegularFiles int
	UntrackedLogicalBytes int64
}

// Snapshot is the trusted state compared at the outer workflow boundaries.
type Snapshot struct {
	Head                 string
	Branch               string
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

// Capture takes one deterministic snapshot without enumerating ignored paths.
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

// Compare verifies that two boundaries describe the same protected state.
func Compare(expected, current Snapshot) error {
	if current.Head != expected.Head {
		return fmt.Errorf("root HEAD changed: got %s, expected %s", current.Head, expected.Head)
	}
	if current.Branch != expected.Branch {
		return fmt.Errorf("root branch changed: got %s, expected %s", current.Branch, expected.Branch)
	}
	if current.WorkspaceFingerprint != expected.WorkspaceFingerprint {
		return errors.New("root workspace content changed")
	}

	return nil
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
		return Snapshot{}, fmt.Errorf("capturing root status: %w", err)
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

	hasher := sha256.New()
	writeHashField(hasher, []byte("ledger-rootguard-v2"))
	writeHashField(hasher, status)
	writeHashField(hasher, stagedDiff)
	writeHashField(hasher, unstagedDiff)
	if err := worker.hashUntrackedPaths(hasher, rootFS, untracked); err != nil {
		return Snapshot{}, err
	}

	return Snapshot{
		Head:                 strings.TrimSpace(string(head)),
		Branch:               strings.TrimSpace(string(branch)),
		WorkspaceFingerprint: hex.EncodeToString(hasher.Sum(nil)),
		Metrics:              worker.metrics,
	}, nil
}

func (worker *snapshotter) hashUntrackedPaths(hasher hash.Hash, root rootedFilesystem, output []byte) error {
	paths, err := splitNUL(output)
	if err != nil {
		return fmt.Errorf("parsing untracked root paths: %w", err)
	}
	slices.SortFunc(paths, bytes.Compare)
	writeHashField(hasher, []byte("untracked"))
	for _, rawPath := range paths {
		path := filepath.FromSlash(string(rawPath))
		if !filepath.IsLocal(path) || path == "." {
			return fmt.Errorf("untracked path is not local: %q", rawPath)
		}
		if err := worker.hashPath(hasher, root, path, rawPath); err != nil {
			return fmt.Errorf("hashing untracked path %q: %w", rawPath, err)
		}
	}

	return nil
}

func (worker *snapshotter) hashPath(hasher hash.Hash, root rootedFilesystem, path string, rawPath []byte) error {
	info, err := root.Lstat(path)
	if err != nil {
		return fmt.Errorf("reading path metadata: %w", err)
	}
	worker.metrics.UntrackedEntries++
	writeHashField(hasher, rawPath)
	writeHashField(hasher, []byte(info.Mode().String()))

	switch {
	case info.Mode()&os.ModeSymlink != 0:
		target, err := root.Readlink(path)
		if err != nil {
			return fmt.Errorf("reading symlink target: %w", err)
		}
		writeHashField(hasher, []byte(target))
	case info.Mode().IsRegular():
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
		worker.metrics.UntrackedRegularFiles++
		worker.metrics.UntrackedLogicalBytes += logicalBytes
	default:
		return errors.New("unsupported untracked file type")
	}

	return nil
}

func (worker *snapshotter) runGit(directory string, arguments ...string) ([]byte, error) {
	worker.metrics.GitProcesses++

	return worker.git(directory, arguments...)
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

// GitProcessesPerSnapshot returns the fixed number of Git commands used by Capture.
func GitProcessesPerSnapshot() int {
	return gitProcessesPerSnapshot
}
