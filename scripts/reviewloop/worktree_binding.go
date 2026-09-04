package main

import (
	"errors"
	"fmt"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
)

type boundCommandRunner struct {
	prNumber            int
	candidateWorktree   string
	expectedHead        string
	trustedRootCheckout string
	validationRunDir    string
}

func newBoundCommandRunner(
	prNumber int,
	candidateWorktree string,
	expectedHead string,
	trustedRootCheckout string,
	validationRunDir string,
) (*boundCommandRunner, error) {
	if prNumber < 1 {
		return nil, errors.New("EXPECTED_PR_NUMBER must be a positive integer")
	}
	if !isFullCommitSHA(expectedHead) {
		return nil, errors.New("EXPECTED_HEAD must be a full commit SHA")
	}

	candidate, err := resolveExistingDirectory(candidateWorktree)
	if err != nil {
		return nil, fmt.Errorf("resolving candidate worktree: %w", err)
	}
	trustedRoot, err := resolveExistingDirectory(trustedRootCheckout)
	if err != nil {
		return nil, fmt.Errorf("resolving trusted root checkout: %w", err)
	}
	if candidate == trustedRoot {
		return nil, errors.New("ROOT_CHECKOUT_AS_CANDIDATE_FORBIDDEN")
	}

	validation, err := resolveExistingDirectory(validationRunDir)
	if err != nil {
		return nil, fmt.Errorf("resolving validation run directory: %w", err)
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

	runner := &boundCommandRunner{
		prNumber:            prNumber,
		candidateWorktree:   candidate,
		expectedHead:        expectedHead,
		trustedRootCheckout: trustedRoot,
		validationRunDir:    validation,
	}
	if err := runner.verifyInitialIdentity(); err != nil {
		return nil, err
	}

	return runner, nil
}

func (runner *boundCommandRunner) run(label, command string, extraEnv map[string]string) error {
	if err := runner.verifyCandidateIdentity(); err != nil {
		fmt.Fprintf(os.Stderr, "WORKTREE_BINDING_GATE=FAIL (%v)\n", err)

		return err
	}

	fmt.Printf(
		"WORKTREE_BINDING_GATE=PASS role=%s pr=%d path=%s head=%s\n",
		label,
		runner.prNumber,
		runner.candidateWorktree,
		runner.expectedHead,
	)
	cmd := exec.Command("bash", "-c", command)
	cmd.Dir = runner.candidateWorktree
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Stdin = os.Stdin
	cmd.Env = runner.environment(extraEnv)

	return cmd.Run()
}

func (runner *boundCommandRunner) environment(extra map[string]string) []string {
	values := map[string]string{
		"EXPECTED_PR_NUMBER": strconv.Itoa(runner.prNumber),
		"EXPECTED_WORKTREE":  runner.candidateWorktree,
		"EXPECTED_HEAD":      runner.expectedHead,
		"VALIDATION_RUN_DIR": runner.validationRunDir,
	}
	maps.Copy(values, extra)

	return replaceEnvironment(os.Environ(), values)
}

func (runner *boundCommandRunner) verifyInitialIdentity() error {
	if err := runner.verifyCandidateIdentity(); err != nil {
		return err
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
		return errors.New("trusted root is not a Git worktree root")
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

func (runner *boundCommandRunner) verifyCandidateIdentity() error {
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

	return nil
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
