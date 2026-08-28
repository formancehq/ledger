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
	"strconv"
	"strings"
)

const (
	exitError            = 1
	exitHumanDecision    = 2
	exitAutoFixRequired  = 3
	defaultMaxPasses     = 3
	defaultValidationCmd = "bash scripts/agent-check-pr"
	changeTargetKind     = "BASE_COMPARISON"
)

type finding struct {
	ID          string  `json:"id"`
	Severity    string  `json:"severity"`
	Blocking    *bool   `json:"blocking"`
	AutoFixable *bool   `json:"auto_fixable"`
	Title       string  `json:"title"`
	Location    *string `json:"location"`
	Evidence    string  `json:"evidence"`
	Impact      string  `json:"impact"`
	Resolution  string  `json:"resolution"`
}

type previousFinding struct {
	ID     string `json:"id"`
	Status string `json:"status"`
	Reason string `json:"reason"`
}

type reviewResult struct {
	Decision             string            `json:"decision"`
	Head                 string            `json:"head"`
	WorktreeFingerprint  string            `json:"worktree_fingerprint"`
	PreviousFindings     []previousFinding `json:"previous_findings"`
	Findings             []finding         `json:"findings"`
	ResidualRisk         string            `json:"residual_risk"`
	HumanDecisionContext *string           `json:"human_decision_context"`
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

type loopAction string

type fileSnapshot struct {
	path    string
	content []byte
}

const (
	actionReady   loopAction = "READY_FOR_HUMAN_REVIEW"
	actionAutoFix loopAction = "AUTO_FIX_REQUIRED"
	actionHuman   loopAction = "HUMAN_DECISION_REQUIRED"
)

func main() {
	var reviewCmd, fixCmd, validationCmd, stateDir, baseRef string
	var candidateWorktree, expectedHead, trustedRoot, bindingFile, validationRunDir, gitGuard string
	var maxPasses, prNumber int

	flag.StringVar(&reviewCmd, "review-cmd", "", "command that writes the review JSON to $AI_REVIEW_RESULT")
	flag.StringVar(&fixCmd, "fix-cmd", "", "command that fixes findings from $AI_REVIEW_FINDINGS")
	flag.StringVar(&validationCmd, "validation-cmd", defaultValidationCmd, "local validation command run after fixes and before approval")
	flag.StringVar(&stateDir, "state-dir", "build/ai-review-loop", "directory for review-loop state")
	flag.StringVar(&baseRef, "base", "", "explicit git ref for committed changes under review")
	flag.IntVar(&prNumber, "pr", 0, "expected pull request number")
	flag.StringVar(&candidateWorktree, "worktree", "", "absolute dedicated candidate worktree path")
	flag.StringVar(&expectedHead, "expected-head", "", "full candidate HEAD expected before every subprocess")
	flag.StringVar(&trustedRoot, "trusted-root", "", "absolute primary checkout protected from mutations")
	flag.StringVar(&bindingFile, "binding-file", "", "immutable PR/worktree binding JSON")
	flag.StringVar(&validationRunDir, "validation-run-dir", "", "absolute cache/temp directory distinct from both worktrees")
	flag.StringVar(&gitGuard, "git-guard", "", "absolute trusted ai-git-guard script")
	flag.IntVar(&maxPasses, "max-passes", defaultMaxPasses, "maximum review passes")
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
	if maxPasses < 1 {
		fatal(errors.New("--max-passes must be at least 1"))
	}
	runner, err := newBoundCommandRunner(
		prNumber,
		candidateWorktree,
		expectedHead,
		trustedRoot,
		validationRunDir,
		bindingFile,
		gitGuard,
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

	var previousResult string
	var previousReview *reviewResult
	for pass := 1; pass <= maxPasses; pass++ {
		resultPath := filepath.Join(runStateDir, fmt.Sprintf("review-%d.json", pass))
		reviewedState, err := captureWorkspaceState(repositoryRoot, runStateDir)
		if err != nil {
			fatal(err)
		}
		changeTarget, err := captureReviewChangeTarget(repositoryRoot, base, reviewedState, runStateDir)
		if err != nil {
			fatal(err)
		}
		changeTargetPath := filepath.Join(runStateDir, fmt.Sprintf("target-%d.json", pass))
		changeTargetContent, err := writeReviewChangeTarget(changeTargetPath, changeTarget)
		if err != nil {
			fatal(err)
		}

		env := map[string]string{
			"AI_REVIEW_PASS":                 strconv.Itoa(pass),
			"AI_REVIEW_RESULT":               resultPath,
			"AI_REVIEW_HEAD":                 reviewedState.Head,
			"AI_REVIEW_WORKTREE_FINGERPRINT": reviewedState.Fingerprint,
			"AI_REVIEW_CHANGE_TARGET":        changeTargetPath,
		}
		if previousResult != "" {
			env["AI_REVIEW_PREVIOUS_RESULT"] = previousResult
		}

		fmt.Printf("==> review-loop: review pass %d/%d\n", pass, maxPasses)
		if err := runner.run("review", reviewCmd, env); err != nil {
			fatal(fmt.Errorf("review command failed: %w", err))
		}
		if err := verifyFileUnchanged(changeTargetPath, changeTargetContent); err != nil {
			fatal(fmt.Errorf("review command changed its target description: %w", err))
		}
		currentState, err := captureWorkspaceState(repositoryRoot, runStateDir)
		if err != nil {
			fatal(err)
		}
		if currentState != reviewedState {
			fatal(fmt.Errorf(
				"workspace changed while the review command was running: before %s/%s, after %s/%s",
				reviewedState.Head,
				reviewedState.Fingerprint,
				currentState.Head,
				currentState.Fingerprint,
			))
		}

		result, err := loadReviewResult(resultPath)
		if err != nil {
			fatal(err)
		}
		if err := validatePreviousFindings(result, previousReview); err != nil {
			fatal(err)
		}
		if err := validateReviewTarget(result, reviewedState); err != nil {
			fatal(err)
		}
		action, blockers, err := decide(result)
		if err != nil {
			fatal(err)
		}

		switch action {
		case actionReady:
			fmt.Println("==> review-loop: local validation before readiness")
			if err := runner.run("validation", validationCmd, validationEnv); err != nil {
				fatal(fmt.Errorf("local validation failed before readiness: %w", err))
			}
			validatedState, err := captureWorkspaceState(repositoryRoot, runStateDir)
			if err != nil {
				fatal(err)
			}
			if validatedState != reviewedState {
				fatal(fmt.Errorf(
					"local validation changed the approved workspace: before %s/%s, after %s/%s",
					reviewedState.Head,
					reviewedState.Fingerprint,
					validatedState.Head,
					validatedState.Fingerprint,
				))
			}
			printOutcome(action, pass, result, blockers)

			return
		case actionHuman:
			printOutcome(action, pass, result, blockers)
			os.Exit(exitHumanDecision)
		case actionAutoFix:
			if strings.TrimSpace(fixCmd) == "" {
				printOutcome(action, pass, result, blockers)
				os.Exit(exitAutoFixRequired)
			}
			if pass == maxPasses {
				fmt.Fprintf(os.Stderr, "review-loop: maximum passes reached with %d blocking finding(s)\n", len(blockers))
				printOutcome(actionHuman, pass, result, blockers)
				os.Exit(exitHumanDecision)
			}

			findingsPath := filepath.Join(runStateDir, fmt.Sprintf("fix-%d.json", pass))
			if err := writeFindings(findingsPath, blockers); err != nil {
				fatal(err)
			}
			fixerInputs, err := captureFileSnapshots(findingsPath, resultPath)
			if err != nil {
				fatal(fmt.Errorf("capturing immutable fixer inputs: %w", err))
			}

			fmt.Printf("==> review-loop: auto-fix %d blocking finding(s)\n", len(blockers))
			if err := runner.run("fix", fixCmd, map[string]string{
				"AI_REVIEW_PASS":     strconv.Itoa(pass),
				"AI_REVIEW_FINDINGS": findingsPath,
				"AI_REVIEW_RESULT":   resultPath,
			}); err != nil {
				fatal(fmt.Errorf("fix command failed: %w", err))
			}
			if err := verifyFileSnapshotsUnchanged(fixerInputs); err != nil {
				fatal(fmt.Errorf("fix command changed immutable review state: %w", err))
			}

			fmt.Println("==> review-loop: validation after auto-fix")
			if err := runner.run("validation", validationCmd, validationEnv); err != nil {
				fatal(fmt.Errorf("validation failed after auto-fix: %w", err))
			}
			previousResult = resultPath
			previous := result
			previousReview = &previous
		}
	}
}

func decide(result reviewResult) (loopAction, []finding, error) {
	var blockers []finding
	for index, item := range result.Findings {
		if item.Blocking == nil || item.AutoFixable == nil {
			return "", nil, fmt.Errorf("finding %d is missing explicit blocking flags", index+1)
		}
		if *item.Blocking {
			blockers = append(blockers, item)
		}
	}

	switch result.Decision {
	case "APPROVE":
		if len(blockers) != 0 {
			return "", nil, errors.New("review result is inconsistent: APPROVE contains blocking findings")
		}

		return actionReady, nil, nil
	case "HUMAN_DECISION_REQUIRED":
		return actionHuman, blockers, nil
	case "REQUEST_CHANGES":
		if len(blockers) == 0 {
			return "", nil, errors.New("review result is inconsistent: REQUEST_CHANGES has no blocking findings")
		}
		for _, item := range blockers {
			if !*item.AutoFixable {
				return actionHuman, blockers, nil
			}
		}

		return actionAutoFix, blockers, nil
	default:
		return "", nil, fmt.Errorf("unknown review decision %q", result.Decision)
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
	if result.PreviousFindings == nil {
		return reviewResult{}, errors.New("review result must include the previous_findings array")
	}
	if result.HumanDecisionContext == nil {
		return reviewResult{}, errors.New("review result must include human_decision_context")
	}
	if !oneOf(result.Decision, "APPROVE", "REQUEST_CHANGES", "HUMAN_DECISION_REQUIRED") {
		return reviewResult{}, fmt.Errorf("invalid decision %q", result.Decision)
	}
	if !oneOf(result.ResidualRisk, "LOW", "MEDIUM", "HIGH") {
		return reviewResult{}, fmt.Errorf("invalid residual_risk %q", result.ResidualRisk)
	}
	findingIDs := make(map[string]struct{}, len(result.Findings))
	for index, item := range result.Findings {
		if strings.TrimSpace(item.ID) == "" || strings.TrimSpace(item.Title) == "" || strings.TrimSpace(item.Evidence) == "" || strings.TrimSpace(item.Impact) == "" || strings.TrimSpace(item.Resolution) == "" {
			return reviewResult{}, fmt.Errorf("finding %d is missing required fields", index+1)
		}
		if item.Location == nil {
			return reviewResult{}, fmt.Errorf("finding %d is missing location", index+1)
		}
		if item.Blocking == nil || item.AutoFixable == nil {
			return reviewResult{}, fmt.Errorf("finding %d is missing explicit blocking flags", index+1)
		}
		if !oneOf(item.Severity, "P0", "P1", "P2", "P3") {
			return reviewResult{}, fmt.Errorf("finding %d has invalid severity %q", index+1, item.Severity)
		}
		if _, exists := findingIDs[item.ID]; exists {
			return reviewResult{}, fmt.Errorf("duplicate finding id %q", item.ID)
		}
		findingIDs[item.ID] = struct{}{}
	}
	previousIDs := make(map[string]struct{}, len(result.PreviousFindings))
	for index, item := range result.PreviousFindings {
		if strings.TrimSpace(item.ID) == "" || strings.TrimSpace(item.Reason) == "" {
			return reviewResult{}, fmt.Errorf("previous finding %d is missing required fields", index+1)
		}
		if !oneOf(item.Status, "FIXED", "STILL_VALID", "OUTDATED") {
			return reviewResult{}, fmt.Errorf("previous finding %d has invalid status %q", index+1, item.Status)
		}
		if _, exists := previousIDs[item.ID]; exists {
			return reviewResult{}, fmt.Errorf("duplicate previous finding id %q", item.ID)
		}
		previousIDs[item.ID] = struct{}{}
	}
	if result.Decision == "HUMAN_DECISION_REQUIRED" && strings.TrimSpace(*result.HumanDecisionContext) == "" {
		return reviewResult{}, errors.New("HUMAN_DECISION_REQUIRED must include human_decision_context")
	}
	if result.Decision != "HUMAN_DECISION_REQUIRED" && *result.HumanDecisionContext != "" {
		return reviewResult{}, errors.New("human_decision_context must be empty unless the decision is HUMAN_DECISION_REQUIRED")
	}

	return result, nil
}

func validatePreviousFindings(result reviewResult, previous *reviewResult) error {
	if previous == nil {
		if len(result.PreviousFindings) != 0 {
			return errors.New("first review must use an empty previous_findings array")
		}

		return nil
	}

	previousIDs := make(map[string]struct{}, len(previous.Findings))
	for _, item := range previous.Findings {
		previousIDs[item.ID] = struct{}{}
	}
	if len(result.PreviousFindings) != len(previousIDs) {
		return fmt.Errorf("re-review classified %d previous findings, expected %d", len(result.PreviousFindings), len(previousIDs))
	}
	currentIDs := make(map[string]struct{}, len(result.Findings))
	for _, item := range result.Findings {
		currentIDs[item.ID] = struct{}{}
	}
	for _, item := range result.PreviousFindings {
		if _, exists := previousIDs[item.ID]; !exists {
			return fmt.Errorf("previous finding classification references unknown id %q", item.ID)
		}
		_, stillReported := currentIDs[item.ID]
		switch item.Status {
		case "STILL_VALID":
			if !stillReported {
				return fmt.Errorf("previous finding %q is STILL_VALID but is absent from current findings", item.ID)
			}
		case "FIXED", "OUTDATED":
			if stillReported {
				return fmt.Errorf("previous finding %q is %s but is still present in current findings", item.ID, item.Status)
			}
		}
	}

	return nil
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

func captureFileSnapshots(paths ...string) ([]fileSnapshot, error) {
	snapshots := make([]fileSnapshot, 0, len(paths))
	for _, path := range paths {
		content, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("reading %s: %w", path, err)
		}
		snapshots = append(snapshots, fileSnapshot{path: path, content: content})
	}

	return snapshots, nil
}

func verifyFileSnapshotsUnchanged(snapshots []fileSnapshot) error {
	for _, snapshot := range snapshots {
		if err := verifyFileUnchanged(snapshot.path, snapshot.content); err != nil {
			return err
		}
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

func writeFindings(path string, findings []finding) error {
	content, err := json.MarshalIndent(struct {
		Findings []finding `json:"findings"`
	}{Findings: findings}, "", "  ")
	if err != nil {
		return fmt.Errorf("encoding fix findings: %w", err)
	}
	if err := os.WriteFile(path, append(content, '\n'), 0o644); err != nil {
		return fmt.Errorf("writing fix findings: %w", err)
	}

	return nil
}

func printOutcome(action loopAction, pass int, result reviewResult, blockers []finding) {
	fmt.Printf("\nREVIEW_LOOP_RESULT: %s\n", action)
	fmt.Printf("Passes: %d\n", pass)
	fmt.Printf("Head reviewed: %s\n", result.Head)
	fmt.Printf("Blocking findings: %d\n", len(blockers))
	fmt.Printf("Residual risk: %s\n", result.ResidualRisk)
	if result.HumanDecisionContext != nil && *result.HumanDecisionContext != "" {
		fmt.Printf("Human decision context: %s\n", *result.HumanDecisionContext)
	}
}

func fatal(err error) {
	fmt.Fprintf(os.Stderr, "review-loop: %v\n", err)
	os.Exit(exitError)
}
