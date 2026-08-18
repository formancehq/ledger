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
	defaultValidationCmd = "bash scripts/agent-check"
)

type finding struct {
	ID          string `json:"id"`
	Severity    string `json:"severity"`
	Blocking    *bool  `json:"blocking"`
	AutoFixable *bool  `json:"auto_fixable"`
	Title       string `json:"title"`
	Location    string `json:"location,omitempty"`
	Evidence    string `json:"evidence"`
	Impact      string `json:"impact"`
	Resolution  string `json:"resolution"`
}

type reviewResult struct {
	Decision             string    `json:"decision"`
	Head                 string    `json:"head"`
	WorktreeFingerprint  string    `json:"worktree_fingerprint"`
	Findings             []finding `json:"findings"`
	ResidualRisk         string    `json:"residual_risk"`
	HumanDecisionContext string    `json:"human_decision_context,omitempty"`
}

type workspaceState struct {
	Head        string
	Fingerprint string
}

type loopAction string

const (
	actionReady   loopAction = "READY_FOR_HUMAN_REVIEW"
	actionAutoFix loopAction = "AUTO_FIX_REQUIRED"
	actionHuman   loopAction = "HUMAN_DECISION_REQUIRED"
)

func main() {
	var reviewCmd, fixCmd, validationCmd, stateDir string
	var maxPasses int

	flag.StringVar(&reviewCmd, "review-cmd", "", "command that writes the review JSON to $AI_REVIEW_RESULT")
	flag.StringVar(&fixCmd, "fix-cmd", "", "command that fixes findings from $AI_REVIEW_FINDINGS")
	flag.StringVar(&validationCmd, "validation-cmd", defaultValidationCmd, "command run after every auto-fix pass")
	flag.StringVar(&stateDir, "state-dir", "build/ai-review-loop", "directory for review-loop state")
	flag.IntVar(&maxPasses, "max-passes", defaultMaxPasses, "maximum review passes")
	flag.Parse()

	if strings.TrimSpace(reviewCmd) == "" {
		fatal(errors.New("--review-cmd is required"))
	}
	if maxPasses < 1 {
		fatal(errors.New("--max-passes must be at least 1"))
	}
	repositoryRoot, err := gitRepositoryRoot()
	if err != nil {
		fatal(err)
	}
	runStateDir, err := createRunStateDir(stateDir)
	if err != nil {
		fatal(err)
	}
	fmt.Printf("==> review-loop: state directory %s\n", runStateDir)
	stateParent := filepath.Dir(runStateDir)

	var previousResult string
	for pass := 1; pass <= maxPasses; pass++ {
		resultPath := filepath.Join(runStateDir, fmt.Sprintf("review-%d.json", pass))
		reviewedState, err := captureWorkspaceState(repositoryRoot, stateParent)
		if err != nil {
			fatal(err)
		}

		env := map[string]string{
			"AI_REVIEW_PASS":                 strconv.Itoa(pass),
			"AI_REVIEW_RESULT":               resultPath,
			"AI_REVIEW_HEAD":                 reviewedState.Head,
			"AI_REVIEW_WORKTREE_FINGERPRINT": reviewedState.Fingerprint,
		}
		if previousResult != "" {
			env["AI_REVIEW_PREVIOUS_RESULT"] = previousResult
		}

		fmt.Printf("==> review-loop: review pass %d/%d\n", pass, maxPasses)
		if err := runCommand(reviewCmd, env); err != nil {
			fatal(fmt.Errorf("review command failed: %w", err))
		}
		currentState, err := captureWorkspaceState(repositoryRoot, stateParent)
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
		if err := validateReviewTarget(result, reviewedState); err != nil {
			fatal(err)
		}
		action, blockers, err := decide(result)
		if err != nil {
			fatal(err)
		}

		switch action {
		case actionReady:
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

			fmt.Printf("==> review-loop: auto-fix %d blocking finding(s)\n", len(blockers))
			if err := runCommand(fixCmd, map[string]string{
				"AI_REVIEW_PASS":     strconv.Itoa(pass),
				"AI_REVIEW_FINDINGS": findingsPath,
				"AI_REVIEW_RESULT":   resultPath,
			}); err != nil {
				fatal(fmt.Errorf("fix command failed: %w", err))
			}

			fmt.Println("==> review-loop: validation after auto-fix")
			if err := runCommand(validationCmd, nil); err != nil {
				fatal(fmt.Errorf("validation failed after auto-fix: %w", err))
			}
			previousResult = resultPath
		}
	}
}

func decide(result reviewResult) (loopAction, []finding, error) {
	decision := strings.ToUpper(strings.TrimSpace(result.Decision))
	var blockers []finding
	for index, item := range result.Findings {
		if item.Blocking == nil || item.AutoFixable == nil {
			return "", nil, fmt.Errorf("finding %d is missing explicit blocking flags", index+1)
		}
		if *item.Blocking {
			blockers = append(blockers, item)
		}
	}

	switch decision {
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
	if !oneOf(strings.ToUpper(result.ResidualRisk), "LOW", "MEDIUM", "HIGH") {
		return reviewResult{}, fmt.Errorf("invalid residual_risk %q", result.ResidualRisk)
	}
	for index, item := range result.Findings {
		if strings.TrimSpace(item.ID) == "" || strings.TrimSpace(item.Title) == "" || strings.TrimSpace(item.Evidence) == "" || strings.TrimSpace(item.Impact) == "" || strings.TrimSpace(item.Resolution) == "" {
			return reviewResult{}, fmt.Errorf("finding %d is missing required fields", index+1)
		}
		if item.Blocking == nil || item.AutoFixable == nil {
			return reviewResult{}, fmt.Errorf("finding %d is missing explicit blocking flags", index+1)
		}
		if !oneOf(strings.ToUpper(item.Severity), "P0", "P1", "P2", "P3") {
			return reviewResult{}, fmt.Errorf("finding %d has invalid severity %q", index+1, item.Severity)
		}
	}

	return result, nil
}

func oneOf(value string, allowed ...string) bool {
	return slices.Contains(allowed, value)
}

func createRunStateDir(parent string) (string, error) {
	absoluteParent, err := filepath.Abs(parent)
	if err != nil {
		return "", fmt.Errorf("resolving state directory: %w", err)
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

func gitRepositoryRoot() (string, error) {
	root, err := gitOutput("", "rev-parse", "--show-toplevel")
	if err != nil {
		return "", fmt.Errorf("finding git repository root: %w", err)
	}

	return strings.TrimSpace(string(root)), nil
}

func captureWorkspaceState(repositoryRoot string, excludedPaths ...string) (workspaceState, error) {
	headBytes, err := gitOutput(repositoryRoot, "rev-parse", "HEAD")
	if err != nil {
		return workspaceState{}, fmt.Errorf("reading current HEAD: %w", err)
	}
	head := strings.TrimSpace(string(headBytes))

	trackedDiff, err := gitOutput(repositoryRoot, "diff", "--binary", "--full-index", "HEAD", "--")
	if err != nil {
		return workspaceState{}, fmt.Errorf("reading tracked workspace diff: %w", err)
	}
	untrackedOutput, err := gitOutput(repositoryRoot, "ls-files", "--others", "--exclude-standard", "-z")
	if err != nil {
		return workspaceState{}, fmt.Errorf("listing untracked workspace files: %w", err)
	}

	hasher := sha256.New()
	writeHashField(hasher, []byte(head))
	writeHashField(hasher, trackedDiff)

	var untrackedPaths []string
	for rawPath := range bytes.SplitSeq(untrackedOutput, []byte{0}) {
		if len(rawPath) != 0 {
			untrackedPaths = append(untrackedPaths, string(rawPath))
		}
	}
	slices.Sort(untrackedPaths)
	for _, path := range untrackedPaths {
		absolutePath := filepath.Join(repositoryRoot, filepath.FromSlash(path))
		excluded := false
		for _, excludedPath := range excludedPaths {
			within, err := pathWithin(absolutePath, excludedPath)
			if err != nil {
				return workspaceState{}, fmt.Errorf("checking excluded state path %s: %w", path, err)
			}
			if within {
				excluded = true

				break
			}
		}
		if excluded {
			continue
		}
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

func runCommand(command string, extraEnv map[string]string) error {
	cmd := exec.Command("bash", "-lc", command)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Stdin = os.Stdin
	cmd.Env = os.Environ()
	for key, value := range extraEnv {
		cmd.Env = append(cmd.Env, key+"="+value)
	}

	return cmd.Run()
}

func printOutcome(action loopAction, pass int, result reviewResult, blockers []finding) {
	fmt.Printf("\nREVIEW_LOOP_RESULT: %s\n", action)
	fmt.Printf("Passes: %d\n", pass)
	fmt.Printf("Head reviewed: %s\n", result.Head)
	fmt.Printf("Blocking findings: %d\n", len(blockers))
	fmt.Printf("Residual risk: %s\n", result.ResidualRisk)
	if result.HumanDecisionContext != "" {
		fmt.Printf("Human decision context: %s\n", result.HumanDecisionContext)
	}
}

func fatal(err error) {
	fmt.Fprintf(os.Stderr, "review-loop: %v\n", err)
	os.Exit(exitError)
}
