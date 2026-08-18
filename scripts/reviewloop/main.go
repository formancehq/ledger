package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
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
	ID           string `json:"id"`
	Severity     string `json:"severity"`
	Blocking     bool   `json:"blocking"`
	AutoFixable  bool   `json:"auto_fixable"`
	Title        string `json:"title"`
	Location     string `json:"location,omitempty"`
	Evidence     string `json:"evidence"`
	Impact       string `json:"impact"`
	Resolution   string `json:"resolution"`
}

type reviewResult struct {
	Decision             string    `json:"decision"`
	Head                 string    `json:"head"`
	Findings             []finding `json:"findings"`
	ResidualRisk         string    `json:"residual_risk"`
	HumanDecisionContext string    `json:"human_decision_context,omitempty"`
}

type loopAction string

const (
	actionReady     loopAction = "READY_FOR_HUMAN_REVIEW"
	actionAutoFix   loopAction = "AUTO_FIX_REQUIRED"
	actionHuman     loopAction = "HUMAN_DECISION_REQUIRED"
)

func main() {
	var reviewCmd string
	var fixCmd string
	var validationCmd string
	var stateDir string
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
	if err := os.MkdirAll(stateDir, 0o755); err != nil {
		fatal(fmt.Errorf("creating state directory: %w", err))
	}

	var previousResult string
	for pass := 1; pass <= maxPasses; pass++ {
		resultPath := filepath.Join(stateDir, fmt.Sprintf("review-%d.json", pass))
		_ = os.Remove(resultPath)

		env := map[string]string{
			"AI_REVIEW_PASS":   fmt.Sprintf("%d", pass),
			"AI_REVIEW_RESULT": resultPath,
		}
		if previousResult != "" {
			env["AI_REVIEW_PREVIOUS_RESULT"] = previousResult
		}

		fmt.Printf("==> review-loop: review pass %d/%d\n", pass, maxPasses)
		if err := runCommand(reviewCmd, env); err != nil {
			fatal(fmt.Errorf("review command failed: %w", err))
		}

		result, err := loadReviewResult(resultPath)
		if err != nil {
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

			findingsPath := filepath.Join(stateDir, fmt.Sprintf("fix-%d.json", pass))
			if err := writeFindings(findingsPath, blockers); err != nil {
				fatal(err)
			}

			fmt.Printf("==> review-loop: auto-fix %d blocking finding(s)\n", len(blockers))
			if err := runCommand(fixCmd, map[string]string{
				"AI_REVIEW_PASS":     fmt.Sprintf("%d", pass),
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
	for _, item := range result.Findings {
		if item.Blocking {
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
			if !item.AutoFixable {
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
	if err := json.Unmarshal(content, &result); err != nil {
		return reviewResult{}, fmt.Errorf("decoding review result %s: %w", path, err)
	}
	if result.Head == "" {
		return reviewResult{}, errors.New("review result must include the reviewed head SHA")
	}
	for index, item := range result.Findings {
		if item.ID == "" || item.Severity == "" || item.Title == "" || item.Evidence == "" || item.Impact == "" || item.Resolution == "" {
			return reviewResult{}, fmt.Errorf("finding %d is missing required fields", index+1)
		}
	}
	return result, nil
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
