package main

import (
	"context"
	"time"
)

type claimProvider interface {
	Observe(context.Context, string, *Campaign, time.Time, string, map[string]string) (map[string]ClaimObservation, error)
}

type commandClaimProvider struct{}

func (commandClaimProvider) Observe(
	ctx context.Context,
	remote string,
	campaign *Campaign,
	now time.Time,
	currentClaimant string,
	previousClaimSHAs map[string]string,
) (map[string]ClaimObservation, error) {
	repository, err := openClaimRepository(ctx, remote)
	if err != nil {
		return nil, err
	}
	defer repository.close()
	refs := make([]string, 0, len(campaign.SourceFacts.Findings))
	for _, finding := range campaign.SourceFacts.Findings {
		refs = append(refs, claimRef(campaign.AuditID, finding.ID))
	}
	remoteRefs, err := repository.listRefs(ctx, refs...)
	if err != nil {
		return nil, err
	}
	observations := make(map[string]ClaimObservation, len(campaign.SourceFacts.Findings))
	for _, finding := range campaign.SourceFacts.Findings {
		ref := claimRef(campaign.AuditID, finding.ID)
		observation := emptyClaimObservation(campaign.AuditID, finding.ID, "FRESH")
		if finding.Qualification != "CONFIRMED" {
			observation.State = "NON_CLAIMABLE"
			observations[finding.ID] = observation

			continue
		}
		observedSHA := remoteRefs[ref]
		if observedSHA == "" {
			observations[finding.ID] = observation

			continue
		}
		observation.ObservedClaimSHA = observedSHA
		claim, readErr := repository.readClaim(ctx, ref, observedSHA)
		if readErr != nil {
			observation.State = "AMBIGUOUS"
			observation.Problem = "INVALID_CLAIM_RECORD: " + conciseError(readErr)
			observations[finding.ID] = observation

			continue
		}
		if !claim.Record.matches(campaign, finding) {
			observation.State = "AMBIGUOUS"
			observation.Problem = "CLAIM_IDENTITY_CONFLICT"
			observations[finding.ID] = observation

			continue
		}
		if previousSHA := previousClaimSHAs[finding.ID]; previousSHA != "" && previousSHA != observedSHA &&
			!repository.isAncestor(ctx, previousSHA, observedSHA) {
			observation.State = "AMBIGUOUS"
			observation.Problem = "CLAIM_HISTORY_REWRITTEN"
			observations[finding.ID] = observation

			continue
		}
		observation.Claimant = claim.Record.Claimant
		observation.CreatedAt = timePointer(claim.Record.CreatedAt)
		observation.ExpiresAt = timePointer(claim.Record.ExpiresAt)
		observation.WorkBranch = claim.Record.WorkBranch
		observation.OwnedBySession = currentClaimant != "" && currentClaimant == claim.Record.Claimant
		if isExpired(claim.Record, now) {
			observation.State = "CLAIM_EXPIRED"
		} else {
			observation.State = "CLAIMED"
		}
		observations[finding.ID] = observation
	}

	return observations, nil
}

func emptyClaimObservation(auditID, findingID, freshness string) ClaimObservation {
	return ClaimObservation{
		State: "UNCLAIMED", RemoteRef: claimRef(auditID, findingID), Freshness: freshness,
	}
}

func timePointer(value time.Time) *time.Time {
	valueCopy := value

	return &valueCopy
}

func unavailableClaimObservation(
	campaign *Campaign,
	finding SourceFinding,
	previous ClaimObservation,
	reason error,
) ClaimObservation {
	hadPreviousObservation := previous.RemoteRef != ""
	if previous.RemoteRef == "" {
		previous = emptyClaimObservation(campaign.AuditID, finding.ID, "UNAVAILABLE")
	}
	previous.State = "UNKNOWN"
	previous.OwnedBySession = false
	previous.Problem = "REMOTE_UNAVAILABLE: " + conciseError(reason)
	if hadPreviousObservation {
		previous.Freshness = "STALE"
	} else {
		previous.Freshness = "UNAVAILABLE"
	}

	return previous
}
