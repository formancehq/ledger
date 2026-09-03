package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	"github.com/formancehq/ledger/v3/scripts/internal/rootguard"
)

const validationReceiptVersion = "ledger-validation-receipt-v1"

type validationResult string

const (
	validationExecuted         validationResult = "VALIDATION_EXECUTED"
	validationReusedExactState validationResult = "VALIDATION_REUSED_EXACT_STATE"
)

type validationReceiptKey struct {
	Version                    string
	BaseRef                    string
	BaseSHA                    string
	CandidateWorktree          string
	CandidateHead              string
	CandidateFingerprint       string
	CandidateRootFingerprint   string
	TrustedRootCheckout        string
	TrustedRootFingerprint     string
	TrustedToolWorktree        string
	TrustedToolHead            string
	TrustedToolFingerprint     string
	ValidationCommand          string
	ValidationGatesCommand     string
	SelectedGatesFingerprint   string
	ValidationEnvironment      string
	ValidationRunDirectory     string
	ValidationRunFingerprint   string
	ReviewStateDirectory       string
	WorktreeBindingFile        string
	WorktreeBindingFingerprint string
	GitGuardFingerprint        string
}

type validationReceipt struct {
	key    validationReceiptKey
	digest string
	gates  string
}

// validationReceiptCache belongs to one review-loop process. It is deliberately
// not serializable and is never shared with candidate adoption or publication.
type validationReceiptCache struct {
	enabled      bool
	base         reviewBase
	toolRoot     string
	toolSnapshot rootguard.Snapshot
	gatesCommand string
	receipt      *validationReceipt
}

type protectedValidationIdentityError struct {
	err error
}

func (err protectedValidationIdentityError) Error() string {
	return err.err.Error()
}

func (err protectedValidationIdentityError) Unwrap() error {
	return err.err
}

func newValidationReceiptCache(
	runner *boundCommandRunner,
	base reviewBase,
	toolRoot string,
	gatesCommand string,
) (*validationReceiptCache, error) {
	trimmedToolRoot := strings.TrimSpace(toolRoot)
	trimmedGatesCommand := strings.TrimSpace(gatesCommand)
	if trimmedToolRoot == "" && trimmedGatesCommand == "" {
		return &validationReceiptCache{base: base}, nil
	}
	if trimmedToolRoot == "" || trimmedGatesCommand == "" {
		return nil, errors.New("--validation-tool-root and --validation-gates-cmd must be supplied together")
	}
	resolvedToolRoot, err := resolveExistingDirectory(trimmedToolRoot)
	if err != nil {
		return nil, fmt.Errorf("resolving trusted validation tool worktree: %w", err)
	}
	if resolvedToolRoot == runner.candidateWorktree {
		return nil, errors.New("trusted validation tool worktree must differ from the candidate worktree")
	}
	candidateCommonDir, err := gitCommonDirectory(runner.candidateWorktree)
	if err != nil {
		return nil, err
	}
	toolCommonDir, err := gitCommonDirectory(resolvedToolRoot)
	if err != nil {
		return nil, fmt.Errorf("reading trusted validation tool Git identity: %w", err)
	}
	if toolCommonDir != candidateCommonDir {
		return nil, errors.New("trusted validation tool worktree does not belong to the candidate Git worktree set")
	}
	toolSnapshot, err := rootguard.Capture(resolvedToolRoot)
	if err != nil {
		return nil, fmt.Errorf("capturing initial trusted validation tool identity: %w", err)
	}
	if toolSnapshot.Head != base.SHA {
		return nil, fmt.Errorf(
			"trusted validation tool HEAD is %s, expected base %s",
			toolSnapshot.Head,
			base.SHA,
		)
	}

	return &validationReceiptCache{
		enabled:      true,
		base:         base,
		toolRoot:     resolvedToolRoot,
		toolSnapshot: toolSnapshot,
		gatesCommand: trimmedGatesCommand,
	}, nil
}

func (cache *validationReceiptCache) executeAndRecord(
	runner *boundCommandRunner,
	command string,
	environment map[string]string,
	runStateDir string,
) error {
	// A fixer is an explicit invalidation boundary even if it happens to restore
	// byte-identical candidate content.
	cache.receipt = nil
	fmt.Printf("%s reason=post_fix\n", validationExecuted)
	if err := cache.runValidation(runner, command, environment); err != nil {
		return err
	}
	if !cache.enabled {
		return nil
	}
	key, gates, err := cache.captureKey(runner, command, environment, runStateDir)
	if err != nil {
		var protectedError protectedValidationIdentityError
		if errors.As(err, &protectedError) {
			return err
		}
		fmt.Printf("VALIDATION_RECEIPT_MISMATCH reason=receipt_identity_unavailable detail=%q\n", err)

		return nil
	}
	cache.receipt = &validationReceipt{
		key:    key,
		digest: validationReceiptDigest(key),
		gates:  gates,
	}
	fmt.Printf("VALIDATION_RECEIPT_STORED key=%s gates=%s\n", cache.receipt.digest, gates)

	return nil
}

func (cache *validationReceiptCache) reuseOrExecute(
	runner *boundCommandRunner,
	command string,
	environment map[string]string,
	runStateDir string,
) (validationResult, error) {
	if !cache.enabled {
		fmt.Printf("%s reason=receipt_disabled\n", validationExecuted)

		return validationExecuted, cache.runValidation(runner, command, environment)
	}
	if cache.receipt == nil {
		fmt.Printf("%s reason=no_successful_receipt\n", validationExecuted)

		return validationExecuted, cache.runValidation(runner, command, environment)
	}

	key, gates, err := cache.captureKey(runner, command, environment, runStateDir)
	if err != nil {
		var protectedError protectedValidationIdentityError
		if errors.As(err, &protectedError) {
			return validationExecuted, err
		}
		fmt.Printf("VALIDATION_RECEIPT_MISMATCH reason=receipt_identity_unavailable detail=%q\n", err)
		fmt.Printf("%s reason=receipt_mismatch\n", validationExecuted)

		return validationExecuted, cache.runValidation(runner, command, environment)
	}
	if mismatches := cache.receipt.key.mismatches(key); len(mismatches) != 0 {
		fmt.Printf("VALIDATION_RECEIPT_MISMATCH fields=%s previousKey=%s currentKey=%s\n",
			strings.Join(mismatches, ","),
			cache.receipt.digest,
			validationReceiptDigest(key),
		)
		fmt.Printf("%s reason=receipt_mismatch\n", validationExecuted)

		return validationExecuted, cache.runValidation(runner, command, environment)
	}

	fmt.Printf("%s key=%s gates=%s\n", validationReusedExactState, cache.receipt.digest, gates)

	return validationReusedExactState, nil
}

func (cache *validationReceiptCache) captureKey(
	runner *boundCommandRunner,
	command string,
	environment map[string]string,
	runStateDir string,
) (validationReceiptKey, string, error) {
	if err := runner.verifyValidationReceiptBoundary(); err != nil {
		return validationReceiptKey{}, "", protectedValidationIdentityError{err: err}
	}
	if err := cache.verifyToolUnchanged(); err != nil {
		return validationReceiptKey{}, "", protectedValidationIdentityError{err: err}
	}
	selectedGates, commandErr := runner.output(cache.gatesCommand, environment)
	if err := runner.verifyValidationReceiptBoundary(); err != nil {
		return validationReceiptKey{}, "", protectedValidationIdentityError{err: err}
	}
	if err := cache.verifyToolUnchanged(); err != nil {
		return validationReceiptKey{}, "", protectedValidationIdentityError{err: err}
	}
	if commandErr != nil {
		return validationReceiptKey{}, "", fmt.Errorf("resolving selected validation gates: %w", commandErr)
	}
	if len(bytes.TrimSpace(selectedGates)) == 0 {
		return validationReceiptKey{}, "", errors.New("selected validation gates are empty")
	}

	candidateState, err := captureWorkspaceState(runner.candidateWorktree, runStateDir)
	if err != nil {
		return validationReceiptKey{}, "", fmt.Errorf("capturing candidate workspace identity: %w", err)
	}
	candidateSnapshot, err := rootguard.Capture(runner.candidateWorktree)
	if err != nil {
		return validationReceiptKey{}, "", fmt.Errorf("capturing protected candidate root identity: %w", err)
	}
	bindingContent, err := os.ReadFile(runner.bindingFile)
	if err != nil {
		return validationReceiptKey{}, "", fmt.Errorf("reading worktree binding identity: %w", err)
	}
	guardContent, err := os.ReadFile(runner.gitGuardPath())
	if err != nil {
		return validationReceiptKey{}, "", fmt.Errorf("reading installed Git guard identity: %w", err)
	}
	validationEnvironment, err := environmentFingerprint(runner.environment(environment))
	if err != nil {
		return validationReceiptKey{}, "", err
	}
	validationRunFingerprint, err := directoryFingerprint(runner.validationRunDir)
	if err != nil {
		return validationReceiptKey{}, "", fmt.Errorf("capturing validation run directory identity: %w", err)
	}

	gates := strings.Join(strings.Fields(string(selectedGates)), ",")
	key := validationReceiptKey{
		Version:                    validationReceiptVersion,
		BaseRef:                    cache.base.Ref,
		BaseSHA:                    cache.base.SHA,
		CandidateWorktree:          runner.candidateWorktree,
		CandidateHead:              candidateState.Head,
		CandidateFingerprint:       candidateState.Fingerprint,
		CandidateRootFingerprint:   rootguardSnapshotFingerprint(candidateSnapshot),
		TrustedRootCheckout:        runner.trustedRootCheckout,
		TrustedRootFingerprint:     rootguardSnapshotFingerprint(runner.rootSnapshot),
		TrustedToolWorktree:        cache.toolRoot,
		TrustedToolHead:            cache.toolSnapshot.Head,
		TrustedToolFingerprint:     rootguardSnapshotFingerprint(cache.toolSnapshot),
		ValidationCommand:          command,
		ValidationGatesCommand:     cache.gatesCommand,
		SelectedGatesFingerprint:   bytesFingerprint(selectedGates),
		ValidationEnvironment:      validationEnvironment,
		ValidationRunDirectory:     runner.validationRunDir,
		ValidationRunFingerprint:   validationRunFingerprint,
		ReviewStateDirectory:       runStateDir,
		WorktreeBindingFile:        runner.bindingFile,
		WorktreeBindingFingerprint: bytesFingerprint(bindingContent),
		GitGuardFingerprint:        bytesFingerprint(guardContent),
	}

	return key, gates, nil
}

func (cache *validationReceiptCache) runValidation(
	runner *boundCommandRunner,
	command string,
	environment map[string]string,
) error {
	if cache.enabled {
		if err := cache.verifyToolUnchanged(); err != nil {
			return err
		}
	}
	commandErr := runner.run("validation", command, environment)
	if !cache.enabled {
		return commandErr
	}
	toolErr := cache.verifyToolUnchanged()
	if commandErr != nil && toolErr != nil {
		return errors.Join(commandErr, toolErr)
	}
	if commandErr != nil {
		return commandErr
	}

	return toolErr
}

func (cache *validationReceiptCache) verifyToolUnchanged() error {
	current, err := rootguard.Capture(cache.toolRoot)
	if err != nil {
		return fmt.Errorf("TRUSTED_TOOL_MUTATION_DETECTED: capturing trusted validation tool identity: %w", err)
	}
	if err := rootguard.Compare(cache.toolSnapshot, current); err != nil {
		return fmt.Errorf("TRUSTED_TOOL_MUTATION_DETECTED: %w", err)
	}

	return nil
}

func (key validationReceiptKey) mismatches(current validationReceiptKey) []string {
	mismatches := make([]string, 0)
	compare := func(name, expected, actual string) {
		if expected != actual {
			mismatches = append(mismatches, name)
		}
	}
	compare("version", key.Version, current.Version)
	compare("baseRef", key.BaseRef, current.BaseRef)
	compare("baseSha", key.BaseSHA, current.BaseSHA)
	compare("candidateWorktree", key.CandidateWorktree, current.CandidateWorktree)
	compare("candidateHead", key.CandidateHead, current.CandidateHead)
	compare("candidateFingerprint", key.CandidateFingerprint, current.CandidateFingerprint)
	compare("candidateRootFingerprint", key.CandidateRootFingerprint, current.CandidateRootFingerprint)
	compare("trustedRootCheckout", key.TrustedRootCheckout, current.TrustedRootCheckout)
	compare("trustedRootFingerprint", key.TrustedRootFingerprint, current.TrustedRootFingerprint)
	compare("trustedToolWorktree", key.TrustedToolWorktree, current.TrustedToolWorktree)
	compare("trustedToolHead", key.TrustedToolHead, current.TrustedToolHead)
	compare("trustedToolFingerprint", key.TrustedToolFingerprint, current.TrustedToolFingerprint)
	compare("validationCommand", key.ValidationCommand, current.ValidationCommand)
	compare("validationGatesCommand", key.ValidationGatesCommand, current.ValidationGatesCommand)
	compare("selectedGates", key.SelectedGatesFingerprint, current.SelectedGatesFingerprint)
	compare("validationEnvironment", key.ValidationEnvironment, current.ValidationEnvironment)
	compare("validationRunDirectory", key.ValidationRunDirectory, current.ValidationRunDirectory)
	compare("validationRunContents", key.ValidationRunFingerprint, current.ValidationRunFingerprint)
	compare("reviewStateDirectory", key.ReviewStateDirectory, current.ReviewStateDirectory)
	compare("worktreeBindingFile", key.WorktreeBindingFile, current.WorktreeBindingFile)
	compare("worktreeBinding", key.WorktreeBindingFingerprint, current.WorktreeBindingFingerprint)
	compare("gitGuard", key.GitGuardFingerprint, current.GitGuardFingerprint)

	return mismatches
}

func validationReceiptDigest(key validationReceiptKey) string {
	hasher := sha256.New()
	for _, value := range []string{
		key.Version,
		key.BaseRef,
		key.BaseSHA,
		key.CandidateWorktree,
		key.CandidateHead,
		key.CandidateFingerprint,
		key.CandidateRootFingerprint,
		key.TrustedRootCheckout,
		key.TrustedRootFingerprint,
		key.TrustedToolWorktree,
		key.TrustedToolHead,
		key.TrustedToolFingerprint,
		key.ValidationCommand,
		key.ValidationGatesCommand,
		key.SelectedGatesFingerprint,
		key.ValidationEnvironment,
		key.ValidationRunDirectory,
		key.ValidationRunFingerprint,
		key.ReviewStateDirectory,
		key.WorktreeBindingFile,
		key.WorktreeBindingFingerprint,
		key.GitGuardFingerprint,
	} {
		writeHashField(hasher, []byte(value))
	}

	return hex.EncodeToString(hasher.Sum(nil))
}

func rootguardSnapshotFingerprint(snapshot rootguard.Snapshot) string {
	hasher := sha256.New()
	writeHashField(hasher, []byte(snapshot.Head))
	writeHashField(hasher, []byte(snapshot.Branch))
	writeHashField(hasher, snapshot.Status)
	writeHashField(hasher, []byte(snapshot.WorkspaceFingerprint))

	return hex.EncodeToString(hasher.Sum(nil))
}

func environmentFingerprint(environment []string) (string, error) {
	values := slices.Clone(environment)
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		name, _, found := strings.Cut(value, "=")
		if !found || name == "" {
			return "", fmt.Errorf("validation environment contains invalid entry %q", value)
		}
		if _, exists := seen[name]; exists {
			return "", fmt.Errorf("validation environment contains duplicate variable %q", name)
		}
		seen[name] = struct{}{}
	}
	slices.Sort(values)
	hasher := sha256.New()
	for _, value := range values {
		writeHashField(hasher, []byte(value))
	}

	return hex.EncodeToString(hasher.Sum(nil)), nil
}

func bytesFingerprint(value []byte) string {
	digest := sha256.Sum256(value)

	return hex.EncodeToString(digest[:])
}

func directoryFingerprint(directory string) (string, error) {
	if !filepath.IsAbs(directory) {
		return "", errors.New("directory path is not absolute")
	}
	root, err := os.OpenRoot(directory)
	if err != nil {
		return "", err
	}
	defer func() {
		_ = root.Close() // Fingerprinting errors take precedence over best-effort descriptor cleanup.
	}()

	hasher := sha256.New()
	writeHashField(hasher, []byte("ledger-validation-directory-v1"))
	rootInfo, err := root.Lstat(".")
	if err != nil {
		return "", fmt.Errorf("reading validation directory metadata: %w", err)
	}
	writeDirectoryMetadata(hasher, rootInfo)
	if err := hashDirectoryEntries(hasher, root, "."); err != nil {
		return "", err
	}

	return hex.EncodeToString(hasher.Sum(nil)), nil
}

func hashDirectoryEntries(hasher hash.Hash, root *os.Root, directory string) error {
	opened, err := root.Open(directory)
	if err != nil {
		return fmt.Errorf("opening directory %q: %w", directory, err)
	}
	entries, readErr := opened.ReadDir(-1)
	closeErr := opened.Close()
	if readErr != nil && closeErr != nil {
		return errors.Join(readErr, closeErr)
	}
	if readErr != nil {
		return fmt.Errorf("reading directory %q: %w", directory, readErr)
	}
	if closeErr != nil {
		return fmt.Errorf("closing directory %q: %w", directory, closeErr)
	}
	slices.SortFunc(entries, func(left, right os.DirEntry) int {
		return strings.Compare(left.Name(), right.Name())
	})

	for _, entry := range entries {
		path := entry.Name()
		if directory != "." {
			path = filepath.Join(directory, path)
		}
		if !filepath.IsLocal(path) || path == "." {
			return fmt.Errorf("directory entry path is not local: %q", path)
		}
		info, err := root.Lstat(path)
		if err != nil {
			return fmt.Errorf("reading directory entry metadata %q: %w", path, err)
		}
		writeHashField(hasher, []byte(path))
		writeDirectoryMetadata(hasher, info)

		switch {
		case info.Mode()&os.ModeSymlink != 0:
			writeHashField(hasher, []byte("symlink"))
			target, err := root.Readlink(path)
			if err != nil {
				return fmt.Errorf("reading directory symlink %q: %w", path, err)
			}
			writeHashField(hasher, []byte(target))
		case info.Mode().IsRegular():
			writeHashField(hasher, []byte("regular"))
			file, err := root.Open(path)
			if err != nil {
				return fmt.Errorf("opening directory file %q: %w", path, err)
			}
			contentHasher := sha256.New()
			logicalBytes, copyErr := io.Copy(contentHasher, file)
			openedInfo, statErr := file.Stat()
			closeErr := file.Close()
			if copyErr != nil {
				return fmt.Errorf("reading directory file %q: %w", path, copyErr)
			}
			if statErr != nil {
				return fmt.Errorf("checking directory file %q after read: %w", path, statErr)
			}
			if closeErr != nil {
				return fmt.Errorf("closing directory file %q: %w", path, closeErr)
			}
			if !openedInfo.Mode().IsRegular() ||
				!os.SameFile(info, openedInfo) ||
				openedInfo.Mode() != info.Mode() ||
				openedInfo.Size() != logicalBytes ||
				!openedInfo.ModTime().Equal(info.ModTime()) {
				return fmt.Errorf("directory file %q changed metadata, type, or identity while being read", path)
			}
			writeHashField(hasher, contentHasher.Sum(nil))
		case info.IsDir():
			writeHashField(hasher, []byte("directory"))
			if err := hashDirectoryEntries(hasher, root, path); err != nil {
				return err
			}
		default:
			writeHashField(hasher, []byte("special"))
		}
	}

	return nil
}

func writeDirectoryMetadata(hasher hash.Hash, info os.FileInfo) {
	writeHashField(hasher, []byte(info.Mode().String()))
	writeHashField(hasher, []byte(strconv.FormatInt(info.Size(), 10)))
	writeHashField(hasher, []byte(strconv.FormatInt(info.ModTime().UnixNano(), 10)))
}
