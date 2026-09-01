package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"time"
)

func main() {
	if err := run(os.Args[1:], os.Stdout, os.Stderr); err != nil {
		if _, writeErr := fmt.Fprintf(os.Stderr, "ai-campaign: %v\n", err); writeErr != nil {
			os.Exit(1)
		}
		os.Exit(1)
	}
}

func run(arguments []string, stdout, stderr io.Writer) error {
	if len(arguments) == 0 {
		if err := usage(stderr); err != nil {
			return err
		}

		return errors.New("command is required")
	}
	switch arguments[0] {
	case "-h", "--help", "help":
		return usage(stdout)
	case "import":
		return runImport(arguments[1:], stdout, stderr)
	case "inspect":
		return runInspect(arguments[1:], stdout, stderr)
	case "next":
		return runNext(arguments[1:], stdout, stderr)
	default:
		if err := usage(stderr); err != nil {
			return err
		}

		return fmt.Errorf("unknown command %q", arguments[0])
	}
}

func runImport(arguments []string, stdout, stderr io.Writer) error {
	values, flags, err := parseArguments(arguments, map[string]bool{"--audit": true, "--output": true})
	if err != nil {
		return err
	}
	if len(values) != 1 || flags["--audit"] == "" {
		return errors.New("usage: ai-campaign import <qualified-result> --audit <source-audit> [--output <path>]")
	}
	repoRoot, err := repositoryRoot()
	if err != nil {
		return err
	}
	campaign, err := (importer{now: time.Now}).run(flags["--audit"], values[0])
	if err != nil {
		return err
	}
	output := flags["--output"]
	if output == "" {
		output = defaultCampaignPath(repoRoot, campaign)
	}
	output, err = validateCampaignDestination(repoRoot, output, true)
	if err != nil {
		return err
	}
	if _, err := os.Lstat(output); err == nil {
		existing, loadErr := loadCampaign(output)
		if loadErr != nil {
			return fmt.Errorf("refusing to replace existing state: %w", loadErr)
		}
		if existing.CampaignID != campaign.CampaignID {
			return fmt.Errorf("refusing to replace campaign %s with %s", existing.CampaignID, campaign.CampaignID)
		}
		if err := writeHuman(stderr, "Campaign %s already imported at %s\n", existing.CampaignID, output); err != nil {
			return err
		}

		return writeJSON(stdout, existing)
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("inspect campaign output: %w", err)
	}
	if err := writeCampaign(output, campaign); err != nil {
		return err
	}
	if err := writeHuman(stderr, "Imported %s (%d findings) -> %s\n", campaign.CampaignID,
		len(campaign.SourceFacts.Findings), output); err != nil {
		return err
	}

	return writeJSON(stdout, campaign)
}

func runInspect(arguments []string, stdout, stderr io.Writer) error {
	values, flags, err := parseArguments(arguments, map[string]bool{
		"--offline": false, "--repo": true, "--jira-project": true, "--remote": true, "--target": true,
	})
	if err != nil {
		return err
	}
	if len(values) != 1 {
		return errors.New("usage: ai-campaign inspect <campaign> [--offline] [--repo <owner/name>] [--jira-project <key>] [--remote <name>] [--target <branch>]")
	}
	repoRoot, err := repositoryRoot()
	if err != nil {
		return err
	}
	campaignPath, err := validateCampaignDestination(repoRoot, values[0], false)
	if err != nil {
		return err
	}
	campaign, err := loadCampaign(campaignPath)
	if err != nil {
		return err
	}
	options := inspectOptions{
		repository:  valueOr(flags["--repo"], "formancehq/ledger"),
		jiraProject: valueOr(flags["--jira-project"], "EN"),
		remote:      valueOr(flags["--remote"], "origin"),
		target:      valueOr(flags["--target"], "release/v3.0"),
		offline:     flags["--offline"] == "true",
	}
	if err := validateInspectOptions(options); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	result := (inspector{
		now: time.Now, jira: commandJiraProvider{}, github: commandGitHubProvider{}, git: commandGitProvider{},
	}).run(ctx, campaign, options)
	if err := writeCampaign(campaignPath, campaign); err != nil {
		return err
	}
	if err := writeInspectionHuman(stderr, result); err != nil {
		return err
	}

	return writeJSON(stdout, result)
}

func runNext(arguments []string, stdout, stderr io.Writer) error {
	values, _, err := parseArguments(arguments, map[string]bool{})
	if err != nil {
		return err
	}
	if len(values) != 1 {
		return errors.New("usage: ai-campaign next <campaign>")
	}
	campaign, err := loadCampaign(values[0])
	if err != nil {
		return err
	}
	result := buildNextResult(campaign)
	if err := writeHuman(stderr, "Campaign %s next actions (%s external truth):\n", result.CampaignID, result.Freshness); err != nil {
		return err
	}
	for _, finding := range result.Findings {
		if err := writeHuman(stderr, "  %s: %s\n", finding.ID, finding.NextAction); err != nil {
			return err
		}
	}

	return writeJSON(stdout, result)
}

func parseArguments(arguments []string, specification map[string]bool) ([]string, map[string]string, error) {
	values := []string{}
	flags := map[string]string{}
	for index := 0; index < len(arguments); index++ {
		argument := arguments[index]
		requiresValue, known := specification[argument]
		if !known {
			if strings.HasPrefix(argument, "-") {
				return nil, nil, fmt.Errorf("unknown option %q", argument)
			}
			values = append(values, argument)

			continue
		}
		if !requiresValue {
			flags[argument] = "true"

			continue
		}
		if index+1 >= len(arguments) || strings.HasPrefix(arguments[index+1], "-") {
			return nil, nil, fmt.Errorf("%s requires a value", argument)
		}
		index++
		flags[argument] = arguments[index]
	}

	return values, flags, nil
}

func valueOr(value, fallback string) string {
	if value == "" {
		return fallback
	}

	return value
}

func validateInspectOptions(options inspectOptions) error {
	if !jiraProjectPattern.MatchString(options.jiraProject) {
		return errors.New("invalid Jira project")
	}
	if !githubRepoPattern.MatchString(options.repository) {
		return errors.New("invalid GitHub repository")
	}
	if !gitRemotePattern.MatchString(options.remote) {
		return errors.New("invalid Git remote")
	}
	if options.target == "" || exec.Command("git", "check-ref-format", "refs/heads/"+options.target).Run() != nil {
		return errors.New("invalid target branch")
	}

	return nil
}

func writeJSON(destination io.Writer, value any) error {
	encoder := json.NewEncoder(destination)
	encoder.SetIndent("", "  ")
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(value); err != nil {
		return fmt.Errorf("write JSON output: %w", err)
	}

	return nil
}

func writeInspectionHuman(destination io.Writer, inspection *Inspection) error {
	if err := writeHuman(destination, "Campaign %s: %s external truth\n", inspection.CampaignID, inspection.Freshness); err != nil {
		return err
	}
	for _, finding := range inspection.Findings {
		jira := finding.Jira.Status
		if len(finding.Jira.Issues) == 1 {
			jira = finding.Jira.Issues[0].Key + "/" + valueOr(finding.Jira.Issues[0].Status, "UNKNOWN")
			if finding.Jira.Status == "UNKNOWN" {
				jira = "UNKNOWN(cached " + jira + ")"
			}
		}
		pullRequest := finding.PR.Status
		if len(finding.PR.Matches) == 1 {
			match := finding.PR.Matches[0]
			head := match.HeadSHA
			if len(head) > 12 {
				head = head[:12]
			}
			pullRequest = fmt.Sprintf("#%d/%s@%s", match.Number, match.State, valueOr(head, "UNKNOWN"))
			if finding.PR.Status == "UNKNOWN" {
				pullRequest = "UNKNOWN(cached " + pullRequest + ")"
			}
		}
		blockers := "-"
		if len(finding.Blockers) > 0 {
			blockers = strings.Join(finding.Blockers, ",")
		}
		if err := writeHuman(destination, "  %s [%s] %s jira=%s pr=%s freshness=%s blockers=%s next=%s\n",
			finding.ID, finding.Qualification, finding.State, jira, pullRequest, finding.Freshness, blockers,
			finding.NextAction); err != nil {
			return err
		}
	}

	return nil
}

func writeHuman(destination io.Writer, format string, arguments ...any) error {
	if _, err := fmt.Fprintf(destination, format, arguments...); err != nil {
		return fmt.Errorf("write human output: %w", err)
	}

	return nil
}

func usage(destination io.Writer) error {
	return writeHuman(destination, `%s`, `Usage:
  scripts/ai-campaign import <qualified-result> --audit <source-audit> [--output <path>]
  scripts/ai-campaign inspect <campaign> [--offline]
  scripts/ai-campaign next <campaign>

Human summaries are written to stderr; stable JSON is written to stdout.
`)
}
