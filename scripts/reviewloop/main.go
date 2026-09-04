package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"hash"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
)

const (
	exitError            = 1
	exitFindings         = 2
	exitValidationFailed = 4
	defaultValidationCmd = "bash scripts/agent-check-pr"
	changeTargetKind     = "BASE_COMPARISON"
)

type finding struct {
	ID         string  `json:"id"`
	Severity   string  `json:"severity"`
	Blocking   *bool   `json:"blocking"`
	Title      string  `json:"title"`
	Location   *string `json:"location"`
	Evidence   string  `json:"evidence"`
	Impact     string  `json:"impact"`
	Resolution string  `json:"resolution"`
}

type knownFinding struct {
	ID     string `json:"id"`
	Status string `json:"status"`
	Reason string `json:"reason"`
}

type knownFindingsSnapshot struct {
	Version        int               `json:"version"`
	PRNumber       int               `json:"pr_number"`
	Head           string            `json:"head"`
	ReviewDecision string            `json:"review_decision"`
	Findings       []json.RawMessage `json:"findings"`
}

type reviewResult struct {
	Decision             string         `json:"decision"`
	Head                 string         `json:"head"`
	WorktreeFingerprint  string         `json:"worktree_fingerprint"`
	KnownFindings        []knownFinding `json:"known_findings"`
	Findings             []finding      `json:"findings"`
	ResidualRisk         string         `json:"residual_risk"`
	HumanDecisionContext *string        `json:"human_decision_context"`
}

type workspaceState struct {
	Head        string
	Fingerprint string
}

type reviewBase struct {
	Ref string
	SHA string
}

type worktreeChangeKinds struct {
	Staged    bool `json:"staged"`
	Unstaged  bool `json:"unstaged"`
	Untracked bool `json:"untracked"`
}

type reviewChangeTarget struct {
	Kind            string              `json:"kind"`
	BaseRef         string              `json:"base_ref"`
	BaseSHA         string              `json:"base_sha"`
	MergeBaseSHA    string              `json:"merge_base_sha"`
	Head            string              `json:"head"`
	WorktreeScope   worktreeChangeKinds `json:"worktree_scope"`
	WorktreePresent worktreeChangeKinds `json:"worktree_present"`
	UntrackedPaths  []string            `json:"untracked_paths"`
}

func main() {
	var reviewCmd, validationCmd, knownFindingsCmd, knownFindingsFile, stateDir, baseRef string
	var candidateWorktree, expectedHead, trustedRoot, validationRunDir string
	var prNumber int

	flag.StringVar(&reviewCmd, "review-cmd", "", "command that writes the review JSON to $AI_REVIEW_RESULT")
	flag.StringVar(&validationCmd, "validation-cmd", defaultValidationCmd, "local validation command run once before final review")
	flag.StringVar(&knownFindingsCmd, "known-findings-cmd", "", "optional command that collects unresolved GitHub findings immediately before review")
	flag.StringVar(&knownFindingsFile, "known-findings-file", "", "file written by --known-findings-cmd and treated as immutable review input")
	flag.StringVar(&stateDir, "state-dir", "build/ai-review-loop", "directory for review-loop state")
	flag.StringVar(&baseRef, "base", "", "explicit git ref for committed changes under review")
	flag.IntVar(&prNumber, "pr", 0, "expected pull request number")
	flag.StringVar(&candidateWorktree, "worktree", "", "absolute dedicated candidate worktree path")
	flag.StringVar(&expectedHead, "expected-head", "", "full candidate HEAD expected before every subprocess")
	flag.StringVar(&trustedRoot, "trusted-root", "", "absolute primary checkout protected from mutations")
	flag.StringVar(&validationRunDir, "validation-run-dir", "", "absolute cache/temp directory distinct from both worktrees")
	flag.Parse()

	if strings.TrimSpace(reviewCmd) == "" {
		fatal(errors.New("--review-cmd is required"))
	}
	if strings.TrimSpace(validationCmd) == "" {
		fatal(errors.New("--validation-cmd must not be empty"))
	}
	if strings.TrimSpace(baseRef) == "" {
		fatal(errors.New("--base is required"))
	}
	if (strings.TrimSpace(knownFindingsCmd) == "") != (strings.TrimSpace(knownFindingsFile) == "") {
		fatal(errors.New("--known-findings-cmd and --known-findings-file must be provided together"))
	}
	runner, err := newBoundCommandRunner(
		prNumber,
		candidateWorktree,
		expectedHead,
		trustedRoot,
		validationRunDir,
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "WORKTREE_BINDING_GATE=FAIL (%v)\n", err)
		fatal(err)
	}
	repositoryRoot := runner.candidateWorktree
	base, err := resolveReviewBase(repositoryRoot, baseRef)
	if err != nil {
		fatal(err)
	}
	validationEnv := map[string]string{
		"AI_REVIEW_BASE_SHA": base.SHA,
	}
	runStateDir, err := createRunStateDir(repositoryRoot, runner.trustedRootCheckout, stateDir)
	if err != nil {
		fatal(err)
	}
	fmt.Printf("==> review-loop: state directory %s\n", runStateDir)

	initialState, err := captureWorkspaceState(repositoryRoot, runStateDir)
	if err != nil {
		fatal(err)
	}
	fmt.Println("==> review-loop: proportional validation")
	if err := runner.run("validation", validationCmd, validationEnv); err != nil {
		fail(exitValidationFailed, "VALIDATION_FAILED", fmt.Errorf("proportional validation failed: %w", err))
	}
	validatedState, err := captureWorkspaceState(repositoryRoot, runStateDir)
	if err != nil {
		fatal(err)
	}
	if validatedState != initialState {
		fail(exitValidationFailed, "VALIDATION_FAILED", fmt.Errorf(
			"proportional validation changed the candidate workspace: before %s/%s, after %s/%s",
			initialState.Head,
			initialState.Fingerprint,
			validatedState.Head,
			validatedState.Fingerprint,
		))
	}

	var knownFindingsContent []byte
	if strings.TrimSpace(knownFindingsCmd) != "" {
		fmt.Println("==> review-loop: collect unresolved GitHub findings")
		if err := runner.run("known-findings", knownFindingsCmd, nil); err != nil {
			fatal(fmt.Errorf("known-findings collection failed: %w", err))
		}
		knownFindingsContent, err = os.ReadFile(knownFindingsFile)
		if err != nil {
			fatal(fmt.Errorf("reading known-findings snapshot: %w", err))
		}
		if err := validateKnownFindingsSnapshot(knownFindingsContent, prNumber, expectedHead); err != nil {
			fatal(err)
		}
	}

	reviewedState, err := captureWorkspaceState(repositoryRoot, runStateDir)
	if err != nil {
		fatal(err)
	}
	if reviewedState != validatedState {
		fatal(fmt.Errorf(
			"candidate workspace changed while collecting review inputs: before %s/%s, after %s/%s",
			validatedState.Head,
			validatedState.Fingerprint,
			reviewedState.Head,
			reviewedState.Fingerprint,
		))
	}

	resultPath := filepath.Join(runStateDir, "final-review.json")
	changeTarget, err := captureReviewChangeTarget(repositoryRoot, base, reviewedState, runStateDir)
	if err != nil {
		fatal(err)
	}
	changeTargetPath := filepath.Join(runStateDir, "final-review-target.json")
	changeTargetContent, err := writeReviewChangeTarget(changeTargetPath, changeTarget)
	if err != nil {
		fatal(err)
	}
	env := map[string]string{
		"AI_REVIEW_RESULT":               resultPath,
		"AI_REVIEW_HEAD":                 reviewedState.Head,
		"AI_REVIEW_WORKTREE_FINGERPRINT": reviewedState.Fingerprint,
		"AI_REVIEW_CHANGE_TARGET":        changeTargetPath,
	}

	fmt.Println("==> review-loop: exact final technical review")
	if err := runner.run("review", reviewCmd, env); err != nil {
		fail(exitError, "REVIEW_FAILED", fmt.Errorf("review command failed: %w", err))
	}
	if err := verifyFileUnchanged(changeTargetPath, changeTargetContent); err != nil {
		fail(exitError, "REVIEW_FAILED", fmt.Errorf("review command changed its target description: %w", err))
	}
	if knownFindingsContent != nil {
		if err := verifyFileUnchanged(knownFindingsFile, knownFindingsContent); err != nil {
			fail(exitError, "REVIEW_FAILED", fmt.Errorf("review command changed the known-findings snapshot: %w", err))
		}
	}
	currentState, err := captureWorkspaceState(repositoryRoot, runStateDir)
	if err != nil {
		fail(exitError, "REVIEW_FAILED", err)
	}
	if currentState != reviewedState {
		fail(exitError, "REVIEW_FAILED", fmt.Errorf(
			"workspace changed while the review command was running: before %s/%s, after %s/%s",
			reviewedState.Head,
			reviewedState.Fingerprint,
			currentState.Head,
			currentState.Fingerprint,
		))
	}

	result, err := loadReviewResult(resultPath)
	if err != nil {
		fail(exitError, "REVIEW_FAILED", err)
	}
	if err := validateReviewTarget(result, reviewedState); err != nil {
		fail(exitError, "REVIEW_FAILED", err)
	}
	hasFindings, err := classifyReview(result)
	if err != nil {
		fail(exitError, "REVIEW_FAILED", err)
	}
	fmt.Printf("Final review result: %s\n", resultPath)
	printOutcome(result, hasFindings)
	if hasFindings {
		os.Exit(exitFindings)
	}
}

func validateKnownFindingsSnapshot(content []byte, prNumber int, expectedHead string) error {
	var snapshot knownFindingsSnapshot
	if err := json.Unmarshal(content, &snapshot); err != nil {
		return fmt.Errorf("decoding known-findings snapshot: %w", err)
	}
	if snapshot.Version != 1 || snapshot.PRNumber != prNumber || snapshot.Head != expectedHead || snapshot.Findings == nil {
		return errors.New("known-findings snapshot target mismatch")
	}

	return nil
}

func classifyReview(result reviewResult) (bool, error) {
	humanDecision := strings.TrimSpace(*result.HumanDecisionContext) != ""
	switch result.Decision {
	case "APPROVE":
		if len(result.Findings) != 0 || humanDecision {
			return false, errors.New("review result is inconsistent: APPROVE contains findings or human-decision context")
		}

		return false, nil
	case "FINDINGS":
		if len(result.Findings) == 0 && !humanDecision {
			return false, errors.New("review result is inconsistent: FINDINGS contains no findings or human-decision context")
		}

		return true, nil
	default:
		return false, fmt.Errorf("unknown review decision %q", result.Decision)
	}
}

func loadReviewResult(path string) (reviewResult, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		return reviewResult{}, fmt.Errorf("review command did not produce %s: %w", path, err)
	}
	var result reviewResult
	decoder := json.NewDecoder(bytes.NewReader(content))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&result); err != nil {
		return reviewResult{}, fmt.Errorf("decoding review result %s: %w", path, err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return reviewResult{}, fmt.Errorf("decoding review result %s: trailing JSON content", path)
	}
	if strings.TrimSpace(result.Head) == "" {
		return reviewResult{}, errors.New("review result must include the reviewed head SHA")
	}
	if strings.TrimSpace(result.WorktreeFingerprint) == "" {
		return reviewResult{}, errors.New("review result must include the reviewed worktree fingerprint")
	}
	if result.Findings == nil {
		return reviewResult{}, errors.New("review result must include the findings array")
	}
	if result.KnownFindings == nil {
		return reviewResult{}, errors.New("review result must include the known_findings array")
	}
	if result.HumanDecisionContext == nil {
		return reviewResult{}, errors.New("review result must include human_decision_context")
	}
	if !oneOf(result.Decision, "APPROVE", "FINDINGS") {
		return reviewResult{}, fmt.Errorf("invalid decision %q", result.Decision)
	}
	if !oneOf(result.ResidualRisk, "LOW", "MEDIUM", "HIGH") {
		return reviewResult{}, fmt.Errorf("invalid residual_risk %q", result.ResidualRisk)
	}
	findingIDs := make(map[string]finding, len(result.Findings))
	for index, item := range result.Findings {
		if strings.TrimSpace(item.ID) == "" || strings.TrimSpace(item.Title) == "" || strings.TrimSpace(item.Evidence) == "" || strings.TrimSpace(item.Impact) == "" || strings.TrimSpace(item.Resolution) == "" {
			return reviewResult{}, fmt.Errorf("finding %d is missing required fields", index+1)
		}
		if item.Location == nil {
			return reviewResult{}, fmt.Errorf("finding %d is missing location", index+1)
		}
		if item.Blocking == nil {
			return reviewResult{}, fmt.Errorf("finding %d is missing explicit blocking flag", index+1)
		}
		if !oneOf(item.Severity, "P0", "P1", "P2", "P3") {
			return reviewResult{}, fmt.Errorf("finding %d has invalid severity %q", index+1, item.Severity)
		}
		if _, exists := findingIDs[item.ID]; exists {
			return reviewResult{}, fmt.Errorf("duplicate finding id %q", item.ID)
		}
		findingIDs[item.ID] = item
	}
	knownIDs := make(map[string]struct{}, len(result.KnownFindings))
	for index, item := range result.KnownFindings {
		if strings.TrimSpace(item.ID) == "" || strings.TrimSpace(item.Reason) == "" {
			return reviewResult{}, fmt.Errorf("known finding %d is missing required fields", index+1)
		}
		if !oneOf(item.Status, "FIXED", "STILL_VALID", "OUTDATED", "HUMAN_DECISION_REQUIRED") {
			return reviewResult{}, fmt.Errorf("known finding %d has invalid status %q", index+1, item.Status)
		}
		if _, exists := knownIDs[item.ID]; exists {
			return reviewResult{}, fmt.Errorf("duplicate known finding id %q", item.ID)
		}
		knownIDs[item.ID] = struct{}{}
		reportedFinding, reported := findingIDs[item.ID]
		switch item.Status {
		case "STILL_VALID":
			if !reported {
				return reviewResult{}, fmt.Errorf("known finding %q is STILL_VALID but is absent from current findings", item.ID)
			}
			if reportedFinding.Blocking == nil || !*reportedFinding.Blocking {
				return reviewResult{}, fmt.Errorf("known finding %q is STILL_VALID but is not blocking", item.ID)
			}
		case "FIXED", "OUTDATED", "HUMAN_DECISION_REQUIRED":
			if reported {
				return reviewResult{}, fmt.Errorf("known finding %q is %s but is still present in current findings", item.ID, item.Status)
			}
		}
	}
	knownNeedsHuman := slices.ContainsFunc(result.KnownFindings, func(item knownFinding) bool {
		return item.Status == "HUMAN_DECISION_REQUIRED"
	})
	if knownNeedsHuman && result.Decision != "FINDINGS" {
		return reviewResult{}, errors.New("known finding requiring a human decision requires FINDINGS")
	}
	if knownNeedsHuman && strings.TrimSpace(*result.HumanDecisionContext) == "" {
		return reviewResult{}, errors.New("known finding requiring a human decision must include human_decision_context")
	}
	if result.Decision == "APPROVE" && *result.HumanDecisionContext != "" {
		return reviewResult{}, errors.New("human_decision_context must be empty when the decision is APPROVE")
	}

	return result, nil
}

func oneOf(value string, allowed ...string) bool {
	return slices.Contains(allowed, value)
}

func createRunStateDir(repositoryRoot, trustedRoot, parent string) (string, error) {
	absoluteParent := parent
	if !filepath.IsAbs(absoluteParent) {
		absoluteParent = filepath.Join(repositoryRoot, absoluteParent)
	}
	absoluteParent = filepath.Clean(absoluteParent)
	resolvedProspectiveParent, err := resolveProspectivePath(absoluteParent)
	if err != nil {
		return "", fmt.Errorf("resolving prospective state directory: %w", err)
	}
	trustedGitCommonDir, err := gitCommonDirectory(trustedRoot)
	if err != nil {
		return "", err
	}
	for _, forbiddenRoot := range []string{trustedRoot, trustedGitCommonDir} {
		within, pathErr := pathWithin(resolvedProspectiveParent, forbiddenRoot)
		if pathErr != nil {
			return "", fmt.Errorf("checking state directory isolation: %w", pathErr)
		}
		if within {
			return "", errors.New("ROOT_CHECKOUT_STATE_DIR_FORBIDDEN")
		}
	}
	if err := os.MkdirAll(absoluteParent, 0o755); err != nil {
		return "", fmt.Errorf("creating state directory: %w", err)
	}
	resolvedParent, err := filepath.EvalSymlinks(absoluteParent)
	if err != nil {
		return "", fmt.Errorf("resolving state directory symlinks: %w", err)
	}
	runStateDir, err := os.MkdirTemp(resolvedParent, "run-")
	if err != nil {
		return "", fmt.Errorf("creating isolated run state directory: %w", err)
	}

	return runStateDir, nil
}

func resolveProspectivePath(path string) (string, error) {
	current := filepath.Clean(path)
	missing := make([]string, 0)
	for {
		_, err := os.Lstat(current)
		if err == nil {
			break
		}
		if !errors.Is(err, os.ErrNotExist) {
			return "", err
		}
		parent := filepath.Dir(current)
		if parent == current {
			return "", fmt.Errorf("no existing ancestor for %s", path)
		}
		missing = append(missing, filepath.Base(current))
		current = parent
	}
	resolved, err := filepath.EvalSymlinks(current)
	if err != nil {
		return "", err
	}
	for _, v := range slices.Backward(missing) {
		resolved = filepath.Join(resolved, v)
	}

	return filepath.Clean(resolved), nil
}

func resolveReviewBase(repositoryRoot, ref string) (reviewBase, error) {
	trimmedRef := strings.TrimSpace(ref)
	if trimmedRef == "" {
		return reviewBase{}, errors.New("review base ref must not be empty")
	}
	output, err := gitOutput(repositoryRoot, "rev-parse", "--verify", "--end-of-options", trimmedRef+"^{commit}")
	if err != nil {
		return reviewBase{}, fmt.Errorf("resolving review base %q: %w", trimmedRef, err)
	}

	return reviewBase{Ref: trimmedRef, SHA: strings.TrimSpace(string(output))}, nil
}

func captureReviewChangeTarget(repositoryRoot string, base reviewBase, state workspaceState, excludedPaths ...string) (reviewChangeTarget, error) {
	mergeBaseOutput, err := gitOutput(repositoryRoot, "merge-base", base.SHA, state.Head)
	if err != nil {
		return reviewChangeTarget{}, fmt.Errorf("finding merge base between %s and %s: %w", base.SHA, state.Head, err)
	}
	stagedOutput, err := gitOutput(repositoryRoot, "diff", "--cached", "--name-only", "-z", "--")
	if err != nil {
		return reviewChangeTarget{}, fmt.Errorf("detecting staged review changes: %w", err)
	}
	unstagedOutput, err := gitOutput(repositoryRoot, "diff", "--name-only", "-z", "--")
	if err != nil {
		return reviewChangeTarget{}, fmt.Errorf("detecting unstaged review changes: %w", err)
	}
	untrackedPaths, err := listIncludedUntrackedPaths(repositoryRoot, excludedPaths...)
	if err != nil {
		return reviewChangeTarget{}, fmt.Errorf("detecting untracked review changes: %w", err)
	}

	return reviewChangeTarget{
		Kind:         changeTargetKind,
		BaseRef:      base.Ref,
		BaseSHA:      base.SHA,
		MergeBaseSHA: strings.TrimSpace(string(mergeBaseOutput)),
		Head:         state.Head,
		WorktreeScope: worktreeChangeKinds{
			Staged:    true,
			Unstaged:  true,
			Untracked: true,
		},
		WorktreePresent: worktreeChangeKinds{
			Staged:    len(stagedOutput) != 0,
			Unstaged:  len(unstagedOutput) != 0,
			Untracked: len(untrackedPaths) != 0,
		},
		UntrackedPaths: untrackedPaths,
	}, nil
}

func writeReviewChangeTarget(path string, target reviewChangeTarget) ([]byte, error) {
	content, err := json.MarshalIndent(target, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("encoding review change target: %w", err)
	}
	content = append(content, '\n')
	if err := os.WriteFile(path, content, 0o644); err != nil {
		return nil, fmt.Errorf("writing review change target: %w", err)
	}

	return content, nil
}

func verifyFileUnchanged(path string, expected []byte) error {
	actual, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("reading %s: %w", path, err)
	}
	if !bytes.Equal(actual, expected) {
		return fmt.Errorf("%s content changed", path)
	}

	return nil
}

func captureWorkspaceState(repositoryRoot string, excludedPaths ...string) (workspaceState, error) {
	headBytes, err := gitOutput(repositoryRoot, "rev-parse", "HEAD")
	if err != nil {
		return workspaceState{}, fmt.Errorf("reading current HEAD: %w", err)
	}
	head := strings.TrimSpace(string(headBytes))

	stagedDiff, err := gitOutput(repositoryRoot, "diff", "--cached", "--binary", "--full-index", "HEAD", "--")
	if err != nil {
		return workspaceState{}, fmt.Errorf("reading staged workspace diff: %w", err)
	}
	unstagedDiff, err := gitOutput(repositoryRoot, "diff", "--binary", "--full-index", "--")
	if err != nil {
		return workspaceState{}, fmt.Errorf("reading unstaged workspace diff: %w", err)
	}
	untrackedPaths, err := listIncludedUntrackedPaths(repositoryRoot, excludedPaths...)
	if err != nil {
		return workspaceState{}, err
	}

	hasher := sha256.New()
	writeHashField(hasher, []byte(head))
	writeHashField(hasher, stagedDiff)
	writeHashField(hasher, unstagedDiff)

	for _, path := range untrackedPaths {
		absolutePath := filepath.Join(repositoryRoot, filepath.FromSlash(path))
		info, err := os.Lstat(absolutePath)
		if err != nil {
			return workspaceState{}, fmt.Errorf("reading untracked file metadata %s: %w", path, err)
		}
		writeHashField(hasher, []byte(path))
		writeHashField(hasher, []byte(info.Mode().String()))

		var content []byte
		switch {
		case info.Mode()&os.ModeSymlink != 0:
			target, err := os.Readlink(absolutePath)
			if err != nil {
				return workspaceState{}, fmt.Errorf("reading untracked symlink %s: %w", path, err)
			}
			content = []byte(target)
		case info.Mode().IsRegular():
			content, err = os.ReadFile(absolutePath)
			if err != nil {
				return workspaceState{}, fmt.Errorf("reading untracked file %s: %w", path, err)
			}
		default:
			return workspaceState{}, fmt.Errorf("unsupported untracked file type %s", path)
		}
		writeHashField(hasher, content)
	}

	return workspaceState{
		Head:        head,
		Fingerprint: hex.EncodeToString(hasher.Sum(nil)),
	}, nil
}

func listIncludedUntrackedPaths(repositoryRoot string, excludedPaths ...string) ([]string, error) {
	untrackedOutput, err := gitOutput(repositoryRoot, "ls-files", "--others", "--exclude-standard", "-z")
	if err != nil {
		return nil, fmt.Errorf("listing untracked workspace files: %w", err)
	}

	untrackedPaths := make([]string, 0)
	for rawPath := range bytes.SplitSeq(untrackedOutput, []byte{0}) {
		if len(rawPath) == 0 {
			continue
		}
		path := string(rawPath)
		absolutePath := filepath.Join(repositoryRoot, filepath.FromSlash(path))
		excluded := false
		for _, excludedPath := range excludedPaths {
			within, err := pathWithin(absolutePath, excludedPath)
			if err != nil {
				return nil, fmt.Errorf("checking excluded state path %s: %w", path, err)
			}
			if within {
				excluded = true

				break
			}
		}
		if !excluded {
			untrackedPaths = append(untrackedPaths, path)
		}
	}
	slices.Sort(untrackedPaths)

	return untrackedPaths, nil
}

func pathWithin(path, root string) (bool, error) {
	relative, err := filepath.Rel(root, path)
	if err != nil {
		return false, err
	}

	return relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator)) && !filepath.IsAbs(relative), nil
}

func writeHashField(hasher hash.Hash, value []byte) {
	var length [8]byte
	binary.BigEndian.PutUint64(length[:], uint64(len(value)))
	// hash.Hash.Write is specified to never return an error.
	_, _ = hasher.Write(length[:])
	_, _ = hasher.Write(value)
}

func gitOutput(directory string, arguments ...string) ([]byte, error) {
	cmd := exec.Command("git", arguments...)
	if directory != "" {
		cmd.Dir = directory
	}
	output, err := cmd.Output()
	if err != nil {
		var exitError *exec.ExitError
		if errors.As(err, &exitError) {
			return nil, fmt.Errorf("git %s: %w: %s", strings.Join(arguments, " "), err, strings.TrimSpace(string(exitError.Stderr)))
		}

		return nil, fmt.Errorf("git %s: %w", strings.Join(arguments, " "), err)
	}

	return output, nil
}

func validateReviewTarget(result reviewResult, expected workspaceState) error {
	if result.Head != expected.Head {
		return fmt.Errorf("reviewed head mismatch: got %q, expected %q", result.Head, expected.Head)
	}
	if result.WorktreeFingerprint != expected.Fingerprint {
		return fmt.Errorf("reviewed worktree fingerprint mismatch: got %q, expected %q", result.WorktreeFingerprint, expected.Fingerprint)
	}

	return nil
}

func printOutcome(result reviewResult, hasFindings bool) {
	outcome := "APPROVE"
	if hasFindings {
		outcome = "FINDINGS"
	}
	fmt.Printf("\nREVIEW_LOOP_RESULT: %s\n", outcome)
	fmt.Printf("Head reviewed: %s\n", result.Head)
	fmt.Printf("Actionable findings: %d\n", len(result.Findings))
	fmt.Printf("Residual risk: %s\n", result.ResidualRisk)
	if result.HumanDecisionContext != nil && *result.HumanDecisionContext != "" {
		fmt.Printf("Human decision context: %s\n", *result.HumanDecisionContext)
	}
}

func fail(status int, result string, err error) {
	fmt.Fprintf(os.Stderr, "review-loop: %v\n", err)
	fmt.Printf("REVIEW_LOOP_RESULT: %s\n", result)
	os.Exit(status)
}

func fatal(err error) {
	fmt.Fprintf(os.Stderr, "review-loop: %v\n", err)
	os.Exit(exitError)
}
