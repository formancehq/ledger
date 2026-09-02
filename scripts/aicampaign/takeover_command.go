package main

import (
	"context"
	"errors"
	"io"
	"time"
)

func runTakeover(arguments []string, stdout, stderr io.Writer) error {
	values, flags, err := parseArguments(arguments, map[string]bool{
		"--finding": true, "--claimant": true, "--lease": true, "--remote": true,
		"--work-branch": true, "--expected-claim-sha": true,
	})
	if err != nil {
		return err
	}
	if len(values) != 1 || flags["--finding"] == "" {
		return errors.New("usage: ai-campaign takeover <campaign> --finding <id> [--claimant <id>] [--lease <duration>] [--work-branch <branch>] [--expected-claim-sha <sha>] [--remote <name>]")
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
	if !gitRemotePattern.MatchString(remote) {
		return errors.New("invalid Git remote")
	}
	expectedSHA := flags["--expected-claim-sha"]
	if expectedSHA != "" && !shaPattern.MatchString(expectedSHA) {
		return errors.New("invalid expected claim SHA")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	result := takeoverClaim(ctx, campaign, finding, claimant, lease, remote, flags["--work-branch"], expectedSHA, time.Now)

	return emitClaimResult(stdout, stderr, result)
}

func takeoverClaim(
	ctx context.Context,
	campaign *Campaign,
	finding SourceFinding,
	claimant string,
	lease time.Duration,
	remote string,
	workBranch string,
	expectedSHA string,
	now func() time.Time,
) ClaimResult {
	result := baseClaimResult(campaign, finding)
	repository, err := openClaimRepository(ctx, remote)
	if err != nil {
		return remoteUnavailable(result, err)
	}
	defer repository.close()
	refs, err := repository.listRefs(ctx, result.RemoteRef)
	if err != nil {
		return remoteUnavailable(result, err)
	}
	observedSHA := refs[result.RemoteRef]
	if observedSHA == "" {
		result.Status = "CLAIM_MISSING"
		result.Reason = "takeover requires an existing expired claim"

		return result
	}
	result.ObservedClaimSHA = observedSHA
	if !expectedClaimMatches(expectedSHA, observedSHA) {
		return claimResultChanged(result, "remote claim does not equal expected SHA")
	}
	observed, err := repository.readClaim(ctx, result.RemoteRef, observedSHA)
	if err != nil {
		result.Status = "CLAIM_CONFLICT"
		result.Reason = conciseError(err)

		return result
	}
	result.Claim = &observed.Record
	if !observed.Record.matches(campaign, finding) {
		result.Status = "CLAIM_CONFLICT"
		result.Reason = "remote claim identity does not match campaign finding"

		return result
	}
	if workBranch == observed.Record.TargetBranch {
		result.Status = "CLAIM_CONFLICT"
		result.Reason = "work branch cannot equal target branch"

		return result
	}
	createdAt := now().UTC()
	if createdAt.Before(observed.Record.CreatedAt) {
		result.Status = "HUMAN_DECISION_REQUIRED"
		result.Reason = "local clock precedes claim creation time"

		return result
	}
	if !isExpired(observed.Record, createdAt) {
		result.Status = "CLAIM_CONFLICT"
		result.Reason = "live claims cannot be taken over"

		return result
	}
	if workBranch == "" {
		workBranch = observed.Record.WorkBranch
	}
	record := ClaimRecord{
		SchemaVersion: claimSchemaVersion, CampaignID: campaign.CampaignID, AuditID: campaign.AuditID,
		FindingID: finding.ID, FindingIdentityDigest: finding.IdentityDigest,
		SourceQualifiedDigest: campaign.SourceDigests.Qualified, AuditedSHA: campaign.AuditedSHA,
		Qualification: finding.Qualification, Claimant: claimant, CreatedAt: createdAt,
		ExpiresAt: createdAt.Add(lease), TargetBranch: observed.Record.TargetBranch,
		TargetSHA: observed.Record.TargetSHA, WorkBranch: workBranch,
		Predecessor: &ClaimPredecessor{
			ClaimSHA: observedSHA, Claimant: observed.Record.Claimant,
			CreatedAt: observed.Record.CreatedAt, ExpiresAt: observed.Record.ExpiresAt, Reason: "EXPIRED_TAKEOVER",
		},
	}
	claimSHA, err := repository.writeClaim(ctx, result.RemoteRef, observedSHA, record)
	if err != nil {
		return classifyMutationFailure(ctx, repository, result, observedSHA, err)
	}
	result.Status = "TAKEN_OVER"
	result.ClaimSHA = claimSHA
	result.ObservedClaimSHA = claimSHA
	result.Claim = &record

	return result
}
