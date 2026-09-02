package main

import (
	"context"
	"errors"
	"io"
	"time"
)

func runRenew(arguments []string, stdout, stderr io.Writer) error {
	values, flags, err := parseArguments(arguments, map[string]bool{
		"--finding": true, "--claimant": true, "--lease": true, "--remote": true,
		"--target": true, "--work-branch": true, "--expected-claim-sha": true,
	})
	if err != nil {
		return err
	}
	if len(values) != 1 || flags["--finding"] == "" {
		return errors.New("usage: ai-campaign renew <campaign> --finding <id> --claimant <id> [--lease <duration>] [--work-branch <branch>] [--expected-claim-sha <sha>] [--remote <name>] [--target <branch>]")
	}
	lease, err := validateClaimLease(flags["--lease"])
	if err != nil {
		return err
	}
	if err := validateWorkBranch(flags["--work-branch"]); err != nil {
		return err
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
	result := renewClaim(ctx, campaign, finding, claimant, lease, remote, target,
		flags["--work-branch"], expectedSHA, time.Now)

	return emitClaimResult(stdout, stderr, result)
}

func renewClaim(
	ctx context.Context,
	campaign *Campaign,
	finding SourceFinding,
	claimant string,
	lease time.Duration,
	remote string,
	targetBranch string,
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
		result.Status = "CLAIM_CONFLICT"
		result.Reason = "remote claim identity does not match campaign finding"

		return result
	}
	if ownerResult, ok := requireClaimOwner(result, observed.Record, claimant); !ok {
		return ownerResult
	}
	if workBranch == observed.Record.TargetBranch {
		result.Status = "CLAIM_CONFLICT"
		result.Reason = "work branch cannot equal target branch"

		return result
	}
	renewedAt := now().UTC()
	if renewedAt.Before(observed.Record.CreatedAt) {
		result.Status = "CLAIM_CONFLICT"
		result.Reason = "local clock precedes claim creation time"

		return result
	}
	record := observed.Record
	record.RenewedAt = &renewedAt
	record.RenewalCount++
	record.ExpiresAt = renewedAt.Add(lease)
	if workBranch != "" {
		record.WorkBranch = workBranch
	}
	claimSHA, err := repository.writeClaim(ctx, result.RemoteRef, observedSHA, record)
	if err != nil {
		return classifyMutationFailure(ctx, repository, result, observedSHA, err)
	}
	result.Status = "RENEWED"
	result.ClaimSHA = claimSHA
	result.ObservedClaimSHA = claimSHA
	result.Claim = &record

	return result
}

func classifyMutationFailure(
	ctx context.Context,
	repository *claimRepository,
	result ClaimResult,
	previousSHA string,
	mutationError error,
) ClaimResult {
	refs, err := repository.listRefs(ctx, result.RemoteRef)
	if err != nil {
		return remoteUnavailable(result, err)
	}
	currentSHA := refs[result.RemoteRef]
	result.ObservedClaimSHA = currentSHA
	if currentSHA != previousSHA {
		return claimResultChanged(result, "remote claim changed during compare-and-swap")
	}

	return remoteUnavailable(result, mutationError)
}
