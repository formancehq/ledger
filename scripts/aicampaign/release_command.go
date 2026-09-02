package main

import (
	"context"
	"errors"
	"io"
	"time"
)

func runRelease(arguments []string, stdout, stderr io.Writer) error {
	values, flags, err := parseArguments(arguments, map[string]bool{
		"--finding": true, "--claimant": true, "--remote": true, "--target": true, "--expected-claim-sha": true,
	})
	if err != nil {
		return err
	}
	if len(values) != 1 || flags["--finding"] == "" {
		return errors.New("usage: ai-campaign release <campaign> --finding <id> --claimant <id> [--expected-claim-sha <sha>] [--remote <name>] [--target <branch>]")
	}
	claimant, err := resolveClaimant(flags["--claimant"], false)
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
	expectedSHA := flags["--expected-claim-sha"]
	if expectedSHA != "" && !shaPattern.MatchString(expectedSHA) {
		return errors.New("invalid expected claim SHA")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	result := releaseClaim(ctx, campaign, finding, claimant, remote, target, expectedSHA)
	if result.Status == "RELEASED" && clearCachedClaim(campaign, finding.ID) {
		if writeErr := writeCampaign(values[0], campaign); writeErr != nil {
			result.Reason = "remote released; local campaign cache update failed: " + conciseError(writeErr)
		}
	}

	return emitClaimResult(stdout, stderr, result)
}

func releaseClaim(
	ctx context.Context,
	campaign *Campaign,
	finding SourceFinding,
	claimant string,
	remote string,
	targetBranch string,
	expectedSHA string,
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
		result.Reason = "remote claim ref does not exist"

		return result
	}
	result.ObservedClaimSHA = observedSHA
	if !expectedClaimMatches(expectedSHA, observedSHA) {
		return claimResultChanged(result, "remote claim does not equal expected SHA")
	}
	observed, err := repository.readClaim(ctx, result.RemoteRef, observedSHA)
	if err != nil {
		return claimResultChanged(result, conciseError(err))
	}
	result.Claim = &observed.Record
	if !observed.Record.matches(campaign, finding, targetBranch) {
		result.Status = "CLAIM_CHANGED"
		result.Reason = "remote claim identity does not match campaign finding"

		return result
	}
	if ownerResult, ok := requireClaimOwner(result, observed.Record, claimant); !ok {
		return ownerResult
	}
	if err := repository.deleteClaim(ctx, result.RemoteRef, observedSHA); err != nil {
		return classifyMutationFailure(ctx, repository, result, observedSHA, err)
	}
	result.Status = "RELEASED"

	return result
}

func clearCachedClaim(campaign *Campaign, findingID string) bool {
	if campaign.Observations == nil {
		return false
	}
	for index := range campaign.Observations.Findings {
		finding := &campaign.Observations.Findings[index]
		if finding.ID != findingID {
			continue
		}
		source, err := findingByID(campaign, findingID)
		if err != nil {
			return false
		}
		claim := emptyClaimObservation(campaign.AuditID, findingID, "FRESH")
		*finding = projectFinding(source, finding.Jira, finding.PR, campaign.AuditedSHA,
			campaign.Observations.Target.Branch, campaign.Observations.Target.ObservedSHA,
			campaign.Observations.Freshness, claim)

		return true
	}

	return false
}
