package main

import (
	"context"
	"errors"
	"io"
	"os/exec"
	"strings"
	"time"
)

func runClaim(arguments []string, stdout, stderr io.Writer) error {
	values, flags, err := parseArguments(arguments, map[string]bool{
		"--finding": true, "--claimant": true, "--lease": true, "--remote": true, "--target": true,
		"--work-branch": true,
	})
	if err != nil {
		return err
	}
	if len(values) != 1 || flags["--finding"] == "" {
		return errors.New("usage: ai-campaign claim <campaign> --finding <id> [--claimant <id>] [--lease <duration>] [--work-branch <branch>] [--remote <name>] [--target <branch>]")
	}
	lease, err := validateClaimLease(flags["--lease"])
	if err != nil {
		return err
	}
	if err := validateWorkBranch(flags["--work-branch"]); err != nil {
		return err
	}
	claimant, err := resolveClaimant(flags["--claimant"], true)
	if err != nil {
		return err
	}
	campaign, finding, err := loadClaimInput(values[0], flags["--finding"])
	if err != nil {
		return err
	}
	remote := valueOr(flags["--remote"], "origin")
	target := valueOr(flags["--target"], "release/v3.0")
	if err := validateClaimCommandOptions(remote, target); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	result := createClaim(ctx, campaign, finding, claimant, lease, remote, target, flags["--work-branch"], time.Now)

	return emitClaimResult(stdout, stderr, result)
}

func createClaim(
	ctx context.Context,
	campaign *Campaign,
	finding SourceFinding,
	claimant string,
	lease time.Duration,
	remote string,
	target string,
	workBranch string,
	now func() time.Time,
) ClaimResult {
	result := baseClaimResult(campaign, finding)
	if finding.Qualification != "CONFIRMED" || !finding.Dispatchable {
		result.Status = "FINDING_NOT_CLAIMABLE"
		result.Reason = "qualification is " + finding.Qualification

		return result
	}
	if workBranch == target {
		result.Status = "BROKEN_CAMPAIGN_BINDING"
		result.Reason = "work branch cannot equal target branch"

		return result
	}
	repository, err := openClaimRepository(ctx, remote)
	if err != nil {
		return remoteUnavailable(result, err)
	}
	defer repository.close()
	refs, err := repository.listRefs(ctx, result.RemoteRef, "refs/heads/"+target)
	if err != nil {
		return remoteUnavailable(result, err)
	}
	targetSHA := refs["refs/heads/"+target]
	if targetSHA == "" || targetSHA != campaign.AuditedSHA {
		result.Status = "BROKEN_CAMPAIGN_BINDING"
		result.Reason = "target branch is missing or no longer equals audited SHA"

		return result
	}
	if observedSHA := refs[result.RemoteRef]; observedSHA != "" {
		return classifyExistingClaim(ctx, repository, result, campaign, finding, target, observedSHA, now().UTC())
	}
	createdAt := now().UTC()
	record := ClaimRecord{
		SchemaVersion: claimSchemaVersion, CampaignID: campaign.CampaignID, AuditID: campaign.AuditID,
		FindingID: finding.ID, FindingIdentityDigest: finding.IdentityDigest,
		SourceQualifiedDigest: campaign.SourceDigests.Qualified, AuditedSHA: campaign.AuditedSHA,
		Qualification: finding.Qualification, Claimant: claimant, CreatedAt: createdAt,
		ExpiresAt: createdAt.Add(lease), TargetBranch: target, TargetSHA: targetSHA, WorkBranch: workBranch,
	}
	claimSHA, err := repository.writeClaim(ctx, result.RemoteRef, "", record)
	if err != nil {
		refs, refreshErr := repository.listRefs(ctx, result.RemoteRef)
		if refreshErr != nil {
			return remoteUnavailable(result, refreshErr)
		}
		if observedSHA := refs[result.RemoteRef]; observedSHA != "" {
			return classifyExistingClaim(ctx, repository, result, campaign, finding, target, observedSHA, now().UTC())
		}

		return remoteUnavailable(result, err)
	}
	result.Status = "CLAIMED"
	result.ClaimSHA = claimSHA
	result.ObservedClaimSHA = claimSHA
	result.Claim = &record

	return result
}

func classifyExistingClaim(
	ctx context.Context,
	repository *claimRepository,
	result ClaimResult,
	campaign *Campaign,
	finding SourceFinding,
	targetBranch string,
	observedSHA string,
	now time.Time,
) ClaimResult {
	result.ObservedClaimSHA = observedSHA
	observed, err := repository.readClaim(ctx, result.RemoteRef, observedSHA)
	if err != nil {
		result.Status = "CLAIM_CONFLICT"
		result.Reason = conciseError(err)

		return result
	}
	result.Claim = &observed.Record
	if !observed.Record.matches(campaign, finding, targetBranch) {
		result.Status = "CLAIM_CONFLICT"
		result.Reason = "remote claim identity does not match campaign finding"

		return result
	}
	if isExpired(observed.Record, now) {
		result.Status = "CLAIM_EXPIRED"
		result.Reason = "expired claim requires explicit renew, release, or takeover"

		return result
	}
	result.Status = "ALREADY_CLAIMED"
	result.Reason = "finding has a live remote claim"

	return result
}

func loadClaimInput(path, findingID string) (*Campaign, SourceFinding, error) {
	campaign, err := loadCampaign(path)
	if err != nil {
		return nil, SourceFinding{}, err
	}
	finding, err := findingByID(campaign, findingID)
	if err != nil {
		return nil, SourceFinding{}, err
	}

	return campaign, finding, nil
}

func validateClaimCommandOptions(remote, target string) error {
	if !gitRemotePattern.MatchString(remote) {
		return errors.New("invalid Git remote")
	}
	if target == "" || execCheckBranch(target) != nil || strings.HasPrefix(target, "ai-claims/") {
		return errors.New("invalid target branch")
	}

	return nil
}

func execCheckBranch(branch string) error {
	return exec.Command("git", "check-ref-format", "refs/heads/"+branch).Run()
}

func baseClaimResult(campaign *Campaign, finding SourceFinding) ClaimResult {
	return ClaimResult{
		SchemaVersion: "ai-campaign-claim-result/v1", CampaignID: campaign.CampaignID,
		FindingID: finding.ID, RemoteRef: claimRef(campaign.AuditID, finding.ID),
	}
}

func remoteUnavailable(result ClaimResult, err error) ClaimResult {
	result.Status = "REMOTE_UNAVAILABLE"
	result.Reason = conciseError(err)

	return result
}

func emitClaimResult(stdout, stderr io.Writer, result ClaimResult) error {
	if err := writeHuman(stderr, "%s %s ref=%s", result.Status, result.FindingID, result.RemoteRef); err != nil {
		return err
	}
	if result.ObservedClaimSHA != "" {
		if err := writeHuman(stderr, " observed=%s", result.ObservedClaimSHA); err != nil {
			return err
		}
	}
	if result.Reason != "" {
		if err := writeHuman(stderr, " reason=%s", result.Reason); err != nil {
			return err
		}
	}
	if err := writeHuman(stderr, "\n"); err != nil {
		return err
	}

	return writeJSON(stdout, result)
}

func claimResultChanged(result ClaimResult, reason string) ClaimResult {
	result.Status = "CLAIM_CHANGED"
	result.Reason = reason

	return result
}

func expectedClaimMatches(expected, observed string) bool {
	return expected == "" || expected == observed
}

func requireClaimOwner(result ClaimResult, record ClaimRecord, claimant string) (ClaimResult, bool) {
	if record.Claimant != claimant {
		result.Status = "NOT_OWNER"
		result.Reason = "remote claim belongs to " + record.Claimant

		return result, false
	}

	return result, true
}
