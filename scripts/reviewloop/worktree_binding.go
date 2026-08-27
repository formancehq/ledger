package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
)

type worktreeBindingFile struct {
	Version             int    `json:"version"`
	ExpectedPRNumber    int    `json:"expectedPrNumber"`
	CandidateWorktree   string `json:"candidateWorktree"`
	ExpectedHead        string `json:"expectedHead"`
	TrustedRootCheckout string `json:"trustedRootCheckout"`
}

type rootCheckoutSnapshot struct {
	Head                 string
	Branch               string
	Status               []byte
	WorkspaceFingerprint string
}

type boundCommandRunner struct {
	prNumber            int
	candidateWorktree   string
	expectedHead        string
	trustedRootCheckout string
	validationRunDir    string
	bindingFile         string
	bindingFileContent  []byte
	gitGuardBin         string
	rootSnapshot        rootCheckoutSnapshot
}

func newBoundCommandRunner(
	prNumber int,
	candidateWorktree string,
	expectedHead string,
	trustedRootCheckout string,
	validationRunDir string,
	bindingFile string,
	gitGuardSource string,
) (*boundCommandRunner, error) {
	if prNumber < 1 {
		return nil, errors.New("EXPECTED_PR_NUMBER must be a positive integer")
	}
	if !isFullCommitSHA(expectedHead) {
		return nil, errors.New("AI_WORKTREE_EXPECTED_HEAD must be a full commit SHA")
	}

	candidate, err := resolveExistingDirectory(candidateWorktree)
	if err != nil {
		return nil, fmt.Errorf("resolving CANDIDATE_WORKTREE: %w", err)
	}
	trustedRoot, err := resolveExistingDirectory(trustedRootCheckout)
	if err != nil {
		return nil, fmt.Errorf("resolving TRUSTED_ROOT_CHECKOUT: %w", err)
	}
	if candidate == trustedRoot {
		return nil, errors.New("ROOT_CHECKOUT_AS_CANDIDATE_FORBIDDEN")
	}

	validation, err := resolveExistingDirectory(validationRunDir)
	if err != nil {
		return nil, fmt.Errorf("resolving VALIDATION_RUN_DIR: %w", err)
	}
	for _, pair := range [][2]string{{validation, candidate}, {candidate, validation}, {validation, trustedRoot}, {trustedRoot, validation}} {
		within, pathErr := pathWithin(pair[0], pair[1])
		if pathErr != nil {
			return nil, fmt.Errorf("checking validation directory isolation: %w", pathErr)
		}
		if within {
			return nil, errors.New("VALIDATION_RUN_DIR must be distinct from both checkout worktrees")
		}
	}

	resolvedBindingFile, err := resolveExistingFile(bindingFile)
	if err != nil {
		return nil, fmt.Errorf("resolving worktree binding file: %w", err)
	}
	insideCandidate, err := pathWithin(resolvedBindingFile, candidate)
	if err != nil {
		return nil, fmt.Errorf("checking worktree binding file location: %w", err)
	}
	if insideCandidate {
		return nil, errors.New("worktree binding file must be outside the candidate worktree")
	}
	bindingContent, err := os.ReadFile(resolvedBindingFile)
	if err != nil {
		return nil, fmt.Errorf("reading worktree binding file: %w", err)
	}

	guardSource, err := resolveExistingFile(gitGuardSource)
	if err != nil {
		return nil, fmt.Errorf("resolving Git mutation guard: %w", err)
	}
	guardContent, err := os.ReadFile(guardSource)
	if err != nil {
		return nil, fmt.Errorf("reading Git mutation guard: %w", err)
	}
	guardBin := filepath.Join(validation, "git-guard-bin")
	if err := os.MkdirAll(guardBin, 0o700); err != nil {
		return nil, fmt.Errorf("creating Git mutation guard directory: %w", err)
	}
	if err := os.WriteFile(filepath.Join(guardBin, "git"), guardContent, 0o700); err != nil {
		return nil, fmt.Errorf("installing Git mutation guard: %w", err)
	}

	runner := &boundCommandRunner{
		prNumber:            prNumber,
		candidateWorktree:   candidate,
		expectedHead:        expectedHead,
		trustedRootCheckout: trustedRoot,
		validationRunDir:    validation,
		bindingFile:         resolvedBindingFile,
		bindingFileContent:  bindingContent,
		gitGuardBin:         guardBin,
	}
	if err := runner.verifyWorktreeBinding(); err != nil {
		return nil, err
	}
	runner.rootSnapshot, err = captureRootCheckoutSnapshot(trustedRoot)
	if err != nil {
		return nil, err
	}
	statusDigest := sha256.Sum256(runner.rootSnapshot.Status)
	fmt.Printf(
		"ROOT_PROTECTION_ARMED head=%s branch=%s statusSha256=%s workspaceFingerprint=%s\n",
		runner.rootSnapshot.Head,
		runner.rootSnapshot.Branch,
		hex.EncodeToString(statusDigest[:]),
		runner.rootSnapshot.WorkspaceFingerprint,
	)

	return runner, nil
}

func (runner *boundCommandRunner) run(label, command string, extraEnv map[string]string) error {
	if err := runner.verifyWorktreeBinding(); err != nil {
		fmt.Fprintf(os.Stderr, "WORKTREE_BINDING_GATE=FAIL (%v)\n", err)

		return err
	}
	if err := runner.verifyRootUnchanged(); err != nil {
		fmt.Fprintf(os.Stderr, "ROOT_MUTATION_DETECTED (%v)\n", err)

		return err
	}

	fmt.Printf(
		"WORKTREE_BINDING_GATE=PASS role=%s pr=%d path=%s head=%s\n",
		label,
		runner.prNumber,
		runner.candidateWorktree,
		runner.expectedHead,
	)
	cmd := exec.Command("bash", "-lc", command)
	cmd.Dir = runner.candidateWorktree
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Stdin = os.Stdin
	cmd.Env = runner.environment(extraEnv)
	commandErr := cmd.Run()
	rootErr := runner.verifyRootUnchanged()
	if rootErr != nil {
		fmt.Fprintf(os.Stderr, "ROOT_MUTATION_DETECTED (%v)\n", rootErr)
		if commandErr != nil {
			return errors.Join(commandErr, rootErr)
		}

		return rootErr
	}
	fmt.Println("ROOT_UNCHANGED=PASS")

	return commandErr
}

func (runner *boundCommandRunner) environment(extra map[string]string) []string {
	values := map[string]string{
		"EXPECTED_PR_NUMBER":        strconv.Itoa(runner.prNumber),
		"EXPECTED_WORKTREE":         runner.candidateWorktree,
		"EXPECTED_HEAD":             runner.expectedHead,
		"AI_WORKTREE_PR":            strconv.Itoa(runner.prNumber),
		"AI_WORKTREE_PATH":          runner.candidateWorktree,
		"AI_WORKTREE_EXPECTED_HEAD": runner.expectedHead,
		"AI_WORKTREE_BINDING_FILE":  runner.bindingFile,
		"TRUSTED_ROOT_CHECKOUT":     runner.trustedRootCheckout,
		"CANDIDATE_WORKTREE":        runner.candidateWorktree,
		"VALIDATION_RUN_DIR":        runner.validationRunDir,
		"PATH":                      runner.gitGuardBin + string(os.PathListSeparator) + os.Getenv("PATH"),
	}
	maps.Copy(values, extra)

	return replaceEnvironment(
		removeEnvironment(os.Environ(), "AI_GIT_REAL_PATH", "AI_GIT_ORIGINAL_PATH"),
		values,
	)
}

func (runner *boundCommandRunner) verifyWorktreeBinding() error {
	currentContent, err := os.ReadFile(runner.bindingFile)
	if err != nil {
		return fmt.Errorf("reading worktree binding file: %w", err)
	}
	if !bytes.Equal(currentContent, runner.bindingFileContent) {
		return errors.New("worktree binding file changed during the run")
	}
	var binding worktreeBindingFile
	decoder := json.NewDecoder(bytes.NewReader(currentContent))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&binding); err != nil {
		return fmt.Errorf("decoding worktree binding file: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return errors.New("worktree binding file contains trailing JSON")
	}
	if binding.Version != 1 {
		return fmt.Errorf("unsupported worktree binding version %d", binding.Version)
	}
	if binding.ExpectedPRNumber != runner.prNumber {
		return fmt.Errorf(
			"CROSS_PR_WORKTREE_CONTAMINATION: expected PR %d, binding belongs to PR %d",
			runner.prNumber,
			binding.ExpectedPRNumber,
		)
	}
	boundCandidate, err := resolveExistingDirectory(binding.CandidateWorktree)
	if err != nil {
		return fmt.Errorf("resolving bound candidate worktree: %w", err)
	}
	if boundCandidate != runner.candidateWorktree {
		return errors.New("worktree binding path does not match AI_WORKTREE_PATH")
	}
	boundRoot, err := resolveExistingDirectory(binding.TrustedRootCheckout)
	if err != nil {
		return fmt.Errorf("resolving bound trusted root checkout: %w", err)
	}
	if boundRoot != runner.trustedRootCheckout {
		return errors.New("worktree binding root does not match TRUSTED_ROOT_CHECKOUT")
	}
	if binding.ExpectedHead != runner.expectedHead {
		return errors.New("worktree binding head does not match AI_WORKTREE_EXPECTED_HEAD")
	}

	workingDirectory, err := os.Getwd()
	if err != nil {
		return fmt.Errorf("reading launcher cwd: %w", err)
	}
	workingDirectory, err = resolveExistingDirectory(workingDirectory)
	if err != nil {
		return fmt.Errorf("resolving launcher cwd: %w", err)
	}
	if workingDirectory != runner.candidateWorktree {
		return fmt.Errorf("launcher cwd is %s, expected candidate worktree %s", workingDirectory, runner.candidateWorktree)
	}

	topLevel, err := gitOutput(runner.candidateWorktree, "rev-parse", "--show-toplevel")
	if err != nil {
		return fmt.Errorf("reading candidate top-level: %w", err)
	}
	resolvedTopLevel, err := resolveExistingDirectory(strings.TrimSpace(string(topLevel)))
	if err != nil {
		return fmt.Errorf("resolving candidate top-level: %w", err)
	}
	if resolvedTopLevel != runner.candidateWorktree {
		return fmt.Errorf("candidate top-level is %s, expected %s", resolvedTopLevel, runner.candidateWorktree)
	}
	head, err := gitOutput(runner.candidateWorktree, "rev-parse", "HEAD")
	if err != nil {
		return fmt.Errorf("reading candidate HEAD: %w", err)
	}
	if strings.TrimSpace(string(head)) != runner.expectedHead {
		return fmt.Errorf("candidate HEAD is %s, expected %s", strings.TrimSpace(string(head)), runner.expectedHead)
	}

	rootTopLevel, err := gitOutput(runner.trustedRootCheckout, "rev-parse", "--show-toplevel")
	if err != nil {
		return fmt.Errorf("reading trusted root top-level: %w", err)
	}
	resolvedRootTopLevel, err := resolveExistingDirectory(strings.TrimSpace(string(rootTopLevel)))
	if err != nil {
		return fmt.Errorf("resolving trusted root top-level: %w", err)
	}
	if resolvedRootTopLevel != runner.trustedRootCheckout {
		return errors.New("TRUSTED_ROOT_CHECKOUT is not a Git worktree root")
	}
	candidateCommonDir, err := gitCommonDirectory(runner.candidateWorktree)
	if err != nil {
		return err
	}
	rootCommonDir, err := gitCommonDirectory(runner.trustedRootCheckout)
	if err != nil {
		return err
	}
	if candidateCommonDir != rootCommonDir {
		return errors.New("candidate and trusted root do not belong to the same Git worktree set")
	}

	return nil
}

func (runner *boundCommandRunner) verifyRootUnchanged() error {
	current, err := captureRootCheckoutSnapshot(runner.trustedRootCheckout)
	if err != nil {
		return err
	}
	if current.Head != runner.rootSnapshot.Head {
		return fmt.Errorf("root HEAD changed: got %s, expected %s", current.Head, runner.rootSnapshot.Head)
	}
	if current.Branch != runner.rootSnapshot.Branch {
		return fmt.Errorf("root branch changed: got %s, expected %s", current.Branch, runner.rootSnapshot.Branch)
	}
	if !bytes.Equal(current.Status, runner.rootSnapshot.Status) {
		return errors.New("root status changed")
	}
	if current.WorkspaceFingerprint != runner.rootSnapshot.WorkspaceFingerprint {
		return errors.New("root workspace content changed")
	}

	return nil
}

func captureRootCheckoutSnapshot(root string) (rootCheckoutSnapshot, error) {
	head, err := gitOutput(root, "rev-parse", "HEAD")
	if err != nil {
		return rootCheckoutSnapshot{}, fmt.Errorf("capturing ROOT_HEAD: %w", err)
	}
	branch, err := gitOutput(root, "rev-parse", "--abbrev-ref", "HEAD")
	if err != nil {
		return rootCheckoutSnapshot{}, fmt.Errorf("capturing ROOT_BRANCH: %w", err)
	}
	status, err := gitOutput(root, "status", "--porcelain=v1", "--untracked-files=all")
	if err != nil {
		return rootCheckoutSnapshot{}, fmt.Errorf("capturing ROOT_STATUS: %w", err)
	}
	workspaceFingerprint, err := captureRootWorkspaceFingerprint(root)
	if err != nil {
		return rootCheckoutSnapshot{}, fmt.Errorf("capturing ROOT_STATUS content fingerprint: %w", err)
	}

	return rootCheckoutSnapshot{
		Head:                 strings.TrimSpace(string(head)),
		Branch:               strings.TrimSpace(string(branch)),
		Status:               status,
		WorkspaceFingerprint: workspaceFingerprint,
	}, nil
}

func captureRootWorkspaceFingerprint(root string) (string, error) {
	workspace, err := captureWorkspaceState(root)
	if err != nil {
		return "", err
	}
	ignoredOutput, err := gitOutput(root, "ls-files", "--others", "--ignored", "--exclude-standard", "-z")
	if err != nil {
		return "", fmt.Errorf("listing ignored workspace files: %w", err)
	}

	hasher := sha256.New()
	writeHashField(hasher, []byte(workspace.Fingerprint))
	for rawPath := range bytes.SplitSeq(ignoredOutput, []byte{0}) {
		if len(rawPath) == 0 {
			continue
		}
		path := string(rawPath)
		absolutePath := filepath.Join(root, filepath.FromSlash(path))
		info, err := os.Lstat(absolutePath)
		if err != nil {
			return "", fmt.Errorf("reading ignored path metadata %s: %w", path, err)
		}
		writeHashField(hasher, rawPath)
		writeHashField(hasher, []byte(info.Mode().String()))

		var content []byte
		switch {
		case info.Mode()&os.ModeSymlink != 0:
			target, err := os.Readlink(absolutePath)
			if err != nil {
				return "", fmt.Errorf("reading ignored symlink %s: %w", path, err)
			}
			content = []byte(target)
		case info.Mode().IsRegular():
			content, err = os.ReadFile(absolutePath)
			if err != nil {
				return "", fmt.Errorf("reading ignored file %s: %w", path, err)
			}
		}
		writeHashField(hasher, content)
	}

	return hex.EncodeToString(hasher.Sum(nil)), nil
}

func gitCommonDirectory(worktree string) (string, error) {
	output, err := gitOutput(worktree, "rev-parse", "--git-common-dir")
	if err != nil {
		return "", fmt.Errorf("reading Git common directory for %s: %w", worktree, err)
	}
	common := strings.TrimSpace(string(output))
	if !filepath.IsAbs(common) {
		common = filepath.Join(worktree, common)
	}
	resolved, err := filepath.EvalSymlinks(common)
	if err != nil {
		return "", fmt.Errorf("resolving Git common directory for %s: %w", worktree, err)
	}

	return filepath.Clean(resolved), nil
}

func resolveExistingDirectory(path string) (string, error) {
	if !filepath.IsAbs(path) {
		return "", errors.New("path is not absolute")
	}
	resolved, err := filepath.EvalSymlinks(filepath.Clean(path))
	if err != nil {
		return "", err
	}
	info, err := os.Stat(resolved)
	if err != nil {
		return "", err
	}
	if !info.IsDir() {
		return "", errors.New("path is not a directory")
	}

	return resolved, nil
}

func resolveExistingFile(path string) (string, error) {
	if !filepath.IsAbs(path) {
		return "", errors.New("path is not absolute")
	}
	resolved, err := filepath.EvalSymlinks(filepath.Clean(path))
	if err != nil {
		return "", err
	}
	info, err := os.Stat(resolved)
	if err != nil {
		return "", err
	}
	if !info.Mode().IsRegular() {
		return "", errors.New("path is not a regular file")
	}

	return resolved, nil
}

func replaceEnvironment(current []string, replacements map[string]string) []string {
	result := make([]string, 0, len(current)+len(replacements))
	for _, item := range current {
		key, _, found := strings.Cut(item, "=")
		if _, replaced := replacements[key]; found && replaced {
			continue
		}
		result = append(result, item)
	}
	for key, value := range replacements {
		result = append(result, key+"="+value)
	}

	return result
}

func removeEnvironment(current []string, removedKeys ...string) []string {
	removed := make(map[string]struct{}, len(removedKeys))
	for _, key := range removedKeys {
		removed[key] = struct{}{}
	}
	result := make([]string, 0, len(current))
	for _, item := range current {
		key, _, found := strings.Cut(item, "=")
		if _, remove := removed[key]; found && remove {
			continue
		}
		result = append(result, item)
	}

	return result
}

func isFullCommitSHA(value string) bool {
	if len(value) < 40 || len(value) > 64 {
		return false
	}
	for _, character := range value {
		if character < '0' || (character > '9' && character < 'a') || character > 'f' {
			return false
		}
	}

	return true
}
