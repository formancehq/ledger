package main

import (
	"context"
	"errors"
	"io"
	"time"
)

func runDispatch(arguments []string, stdout, stderr io.Writer) error {
	values, flags, err := parseArguments(arguments, map[string]bool{
		"--finding": true, "--audit": true, "--qualified": true, "--claimant": true, "--workflow": true,
		"--repo": true, "--jira-project": true, "--remote": true, "--target": true,
		"--worktree": true, "--output": true,
	})
	if err != nil {
		return err
	}
	if len(values) != 1 || flags["--finding"] == "" || flags["--audit"] == "" || flags["--qualified"] == "" {
		return errors.New("usage: ai-campaign dispatch <campaign> --finding <id> --audit <source-audit> --qualified <qualified-result> [--claimant <id>] [--workflow <classification>] [--worktree <path>] [--output <path>] [--repo <owner/name>] [--jira-project <key>] [--remote <name>] [--target <branch>]")
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
	finding, err := findingByID(campaign, flags["--finding"])
	if err != nil {
		return err
	}
	evidence, err := loadDispatchEvidence(flags["--audit"], flags["--qualified"], campaign, finding.ID)
	if err != nil {
		return err
	}
	claimant, err := resolveClaimant(flags["--claimant"], false)
	if err != nil {
		return err
	}
	inspectOptions := inspectOptions{
		repository:  valueOr(flags["--repo"], "formancehq/ledger"),
		jiraProject: valueOr(flags["--jira-project"], "EN"), remote: valueOr(flags["--remote"], "origin"),
		target: valueOr(flags["--target"], "release/v3.0"), claimant: claimant,
	}
	if err := validateInspectOptions(inspectOptions); err != nil {
		return err
	}
	if flags["--workflow"] != "" && !validWorkflow(flags["--workflow"]) {
		return errors.New("workflow must be BUGFIX, TEST_GAP, TOOLING_FIX, or DOCUMENTATION")
	}
	inspectContext, cancelInspection := context.WithTimeout(context.Background(), 2*time.Minute)
	runner := inspector{now: time.Now, jira: commandJiraProvider{}, github: commandGitHubProvider{},
		git: commandGitProvider{}, claims: commandClaimProvider{}}
	inspection := runner.run(inspectContext, campaign, inspectOptions)
	cancelInspection()
	if err := writeCampaign(campaignPath, campaign); err != nil {
		return err
	}
	var observed InspectionFinding
	for _, candidate := range inspection.Findings {
		if candidate.ID == finding.ID {
			observed = candidate

			break
		}
	}
	if observed.ID == "" {
		return errors.New("fresh inspection omitted dispatch finding")
	}
	dispatchContext, cancelDispatch := context.WithTimeout(context.Background(), 2*time.Minute)
	result := dispatchFinding(dispatchContext, campaign, finding, observed, evidence, dispatchOptions{
		repository: inspectOptions.repository, remote: inspectOptions.remote, target: inspectOptions.target,
		claimant: claimant, workflow: flags["--workflow"], repoRoot: repoRoot, campaignPath: campaignPath,
		worktreePath: flags["--worktree"], workItemPath: flags["--output"], findWorkPRs: findCampaignWorkPRs,
	}, time.Now)
	cancelDispatch()
	if result.Status == "DISPATCHED" || result.Status == "STALE_AT_DISPATCH" ||
		result.Status == "ALREADY_DISPATCHED" || result.Resources.BoundClaimSHA != "" {
		postContext, cancelPostInspection := context.WithTimeout(context.Background(), 2*time.Minute)
		_ = runner.run(postContext, campaign, inspectOptions)
		cancelPostInspection()
		if err := writeCampaign(campaignPath, campaign); err != nil {
			return err
		}
	}
	if err := writeHuman(stderr, "%s %s", result.Status, finding.ID); err != nil {
		return err
	}
	if result.Resources.WorkBranch != "" {
		if err := writeHuman(stderr, " branch=%s", result.Resources.WorkBranch); err != nil {
			return err
		}
	}
	if result.Reason != "" {
		if err := writeHuman(stderr, " reason=%s", result.Reason); err != nil {
			return err
		}
	}
	if err := writeHuman(stderr, " next=%s\n", result.NextAction); err != nil {
		return err
	}

	return writeJSON(stdout, result)
}
