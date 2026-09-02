package main

import (
	"context"
	"errors"
	"slices"
	"strings"
	"time"
)

type inspectOptions struct {
	repository  string
	jiraProject string
	remote      string
	target      string
	offline     bool
	claimant    string
}

type inspector struct {
	now    func() time.Time
	jira   jiraProvider
	github githubProvider
	git    gitProvider
	claims claimProvider
}

func (runner inspector) run(ctx context.Context, campaign *Campaign, options inspectOptions) *Inspection {
	previous := previousObservations(campaign)
	if options.offline {
		observations := staleObservations(campaign, previous, options.target)
		campaign.Observations = observations

		return buildInspection(campaign)
	}
	now := runner.now().UTC()

	jiraValues, jiraError := runner.jira.Observe(ctx, options.jiraProject, campaign.SourceFacts.Findings)
	jiraProviderObservation := freshProvider(now)
	if jiraError != nil {
		jiraProviderObservation = unavailableProvider(previous.provider("jira"), jiraError)
	}
	jiraForGitHub := jiraValues
	if jiraError != nil {
		jiraForGitHub = previous.jiraValues()
	}

	githubValues, githubError := runner.github.Observe(ctx, options.repository, campaign.SourceFacts.Findings, jiraForGitHub)
	githubProviderObservation := freshProvider(now)
	if githubError != nil {
		githubProviderObservation = unavailableProvider(previous.provider("github"), githubError)
	}

	previousClaimSHAs := make(map[string]string, len(campaign.SourceFacts.Findings))
	for _, source := range campaign.SourceFacts.Findings {
		previousClaimSHAs[source.ID] = previous.claim(source.ID).ObservedClaimSHA
	}
	claimValues, claimError := runner.claims.Observe(
		ctx, options.remote, campaign, options.target, now, options.claimant, previousClaimSHAs,
	)
	claimProviderObservation := freshProvider(now)
	if claimError != nil {
		claimProviderObservation = unavailableProvider(previous.provider("claims"), claimError)
		claimValues = make(map[string]ClaimObservation, len(campaign.SourceFacts.Findings))
		for _, source := range campaign.SourceFacts.Findings {
			claimValues[source.ID] = unavailableClaimObservation(campaign, source, previous.claim(source.ID), claimError)
		}
	} else {
		for _, source := range campaign.SourceFacts.Findings {
			current := claimValues[source.ID]
			prior := previous.claim(source.ID)
			if current.State == "UNCLAIMED" && prior.ObservedClaimSHA != "" && prior.State != "UNCLAIMED" {
				current = prior
				current.State = "CLAIM_HISTORY_MISSING"
				current.Freshness = "FRESH"
				current.OwnedBySession = current.Claimant != "" && current.Claimant == options.claimant
				current.Problem = "previously observed remote claim ref is missing"
				claimValues[source.ID] = current
			}
		}
	}

	refs := []string{"refs/heads/" + options.target}
	prValuesForRefs := githubValues
	if githubError != nil {
		prValuesForRefs = previous.prValues()
	}
	for _, matches := range prValuesForRefs {
		for _, match := range matches {
			if !match.CrossRepository && match.HeadRef != "" {
				refs = append(refs, "refs/heads/"+match.HeadRef)
			}
		}
	}
	for _, claim := range claimValues {
		if claim.WorkBranch != "" {
			refs = append(refs, "refs/heads/"+claim.WorkBranch)
		}
	}
	slices.Sort(refs)
	refs = slices.Compact(refs)
	gitValues, gitError := runner.git.ObserveRefs(ctx, options.remote, refs)
	gitProviderObservation := freshProvider(now)
	if gitError != nil {
		gitProviderObservation = unavailableProvider(previous.provider("git"), gitError)
	}

	providers := ProviderObservations{
		GitHub: githubProviderObservation,
		Jira:   jiraProviderObservation,
		Git:    gitProviderObservation,
		Claims: claimProviderObservation,
	}
	freshness := combinedFreshness(providers)
	var refreshedAt *time.Time
	if freshness == "FRESH" || freshness == "PARTIAL" {
		refreshedAt = &now
	} else if previous.set != nil {
		refreshedAt = previous.set.RefreshedAt
	}
	observations := &ObservationSet{
		FactType:    observationFactType,
		RefreshedAt: refreshedAt,
		Freshness:   freshness,
		Providers:   providers,
		Findings:    []FindingObservation{},
		Target: TargetObservation{
			FactType: observationFactType,
			Branch:   options.target,
		},
	}
	if gitError == nil {
		observations.Target.ObservedSHA = gitValues["refs/heads/"+options.target]
	} else {
		observations.Target.ObservedSHA = previous.targetSHA()
	}

	for _, source := range campaign.SourceFacts.Findings {
		jiraObservation := JiraObservation{Status: "UNBOUND", BindingBasis: "AI_AUDIT_MARKER", Issues: []JiraIssue{}}
		if jiraError == nil {
			jiraObservation.Issues = nonNilJiraIssues(jiraValues[source.ID])
			jiraObservation.Status = bindingStatus(len(jiraObservation.Issues))
		} else {
			jiraObservation = previous.jira(source.ID)
			jiraObservation.Status = "UNKNOWN"
		}

		prObservation := PRObservation{Status: "UNBOUND", Matches: []PRMatch{}}
		if githubError == nil {
			prObservation.Matches = nonNilPRMatches(githubValues[source.ID])
			prObservation.Status = bindingStatus(len(prObservation.Matches))
			if len(prObservation.Matches) > 0 {
				prObservation.BindingBasis = prObservation.Matches[0].BindingBasis
			}
		} else {
			prObservation = previous.pr(source.ID)
			prObservation.Status = "UNKNOWN"
		}

		for index := range prObservation.Matches {
			match := &prObservation.Matches[index]
			if match.Merged {
				match.BranchExists = true

				continue
			}
			if gitError == nil {
				if match.CrossRepository {
					// GitHub retains a pull request's head commit after a fork or
					// source branch is deleted. Without an authoritative lookup
					// against that fork, the branch is not proven to exist.
					match.BranchExists = false
				} else {
					match.BranchExists = gitValues["refs/heads/"+match.HeadRef] == match.HeadSHA && match.HeadSHA != ""
				}
			}
		}
		claimObservation := claimValues[source.ID]
		if claimObservation.WorkBranch != "" && gitError == nil &&
			gitValues["refs/heads/"+claimObservation.WorkBranch] == "" &&
			(claimObservation.State == "CLAIMED" || claimObservation.State == "CLAIM_EXPIRED") {
			claimObservation.State = "BROKEN_BINDING"
			claimObservation.Problem = "CLAIM_WORK_BRANCH_MISSING"
		}

		projection := projectFinding(source, jiraObservation, prObservation,
			campaign.AuditedSHA, options.target, observations.Target.ObservedSHA, freshness, claimObservation)
		observations.Findings = append(observations.Findings, projection)
	}
	campaign.Observations = observations

	return buildInspection(campaign)
}

type priorObservationSet struct {
	set *ObservationSet
}

func previousObservations(campaign *Campaign) priorObservationSet {
	return priorObservationSet{set: campaign.Observations}
}

func (previous priorObservationSet) provider(name string) ProviderObservation {
	if previous.set == nil {
		return ProviderObservation{}
	}
	switch name {
	case "github":
		return previous.set.Providers.GitHub
	case "jira":
		return previous.set.Providers.Jira
	case "git":
		return previous.set.Providers.Git
	case "claims":
		return previous.set.Providers.Claims
	default:
		panic("unknown provider " + name)
	}
}

func (previous priorObservationSet) claim(id string) ClaimObservation {
	if finding, ok := previous.finding(id); ok && finding.Claim.RemoteRef != "" {
		return finding.Claim
	}

	return ClaimObservation{}
}

func (previous priorObservationSet) finding(id string) (FindingObservation, bool) {
	if previous.set == nil {
		return FindingObservation{}, false
	}
	for _, finding := range previous.set.Findings {
		if finding.ID == id {
			return finding, true
		}
	}

	return FindingObservation{}, false
}

func (previous priorObservationSet) jira(id string) JiraObservation {
	if finding, ok := previous.finding(id); ok {
		finding.Jira.Issues = nonNilJiraIssues(finding.Jira.Issues)

		return finding.Jira
	}

	return JiraObservation{Status: "UNKNOWN", BindingBasis: "AI_AUDIT_MARKER", Issues: []JiraIssue{}}
}

func (previous priorObservationSet) pr(id string) PRObservation {
	if finding, ok := previous.finding(id); ok {
		finding.PR.Matches = nonNilPRMatches(finding.PR.Matches)

		return finding.PR
	}

	return PRObservation{Status: "UNKNOWN", Matches: []PRMatch{}}
}

func (previous priorObservationSet) jiraValues() map[string][]JiraIssue {
	values := map[string][]JiraIssue{}
	if previous.set != nil {
		for _, finding := range previous.set.Findings {
			values[finding.ID] = finding.Jira.Issues
		}
	}

	return values
}

func (previous priorObservationSet) prValues() map[string][]PRMatch {
	values := map[string][]PRMatch{}
	if previous.set != nil {
		for _, finding := range previous.set.Findings {
			values[finding.ID] = finding.PR.Matches
		}
	}

	return values
}

func (previous priorObservationSet) targetSHA() string {
	if previous.set == nil {
		return ""
	}

	return previous.set.Target.ObservedSHA
}

func staleObservations(campaign *Campaign, previous priorObservationSet, target string) *ObservationSet {
	providers := ProviderObservations{
		GitHub: staleProvider(previous.provider("github"), "offline inspection"),
		Jira:   staleProvider(previous.provider("jira"), "offline inspection"),
		Git:    staleProvider(previous.provider("git"), "offline inspection"),
		Claims: staleProvider(previous.provider("claims"), "offline inspection"),
	}
	freshness := combinedFreshness(providers)
	observations := &ObservationSet{
		FactType:  observationFactType,
		Freshness: freshness,
		Providers: providers,
		Findings:  []FindingObservation{},
		Target: TargetObservation{
			FactType:    observationFactType,
			Branch:      target,
			ObservedSHA: previous.targetSHA(),
		},
	}
	if previous.set != nil {
		observations.RefreshedAt = previous.set.RefreshedAt
	}
	for _, source := range campaign.SourceFacts.Findings {
		jira := previous.jira(source.ID)
		jira.Status = "UNKNOWN"
		pr := previous.pr(source.ID)
		pr.Status = "UNKNOWN"
		claim := unavailableClaimObservation(campaign, source, previous.claim(source.ID), errors.New("offline inspection"))
		observations.Findings = append(observations.Findings,
			projectFinding(source, jira, pr, campaign.AuditedSHA, target, observations.Target.ObservedSHA, freshness, claim))
	}

	return observations
}

func projectFinding(
	source SourceFinding,
	jira JiraObservation,
	pr PRObservation,
	auditedSHA string,
	targetBranch string,
	targetSHA string,
	freshness string,
	claim ClaimObservation,
) FindingObservation {
	projection := FindingObservation{
		FactType:          observationFactType,
		ID:                source.ID,
		Jira:              jira,
		PR:                pr,
		Claim:             claim,
		ObservedTargetSHA: targetSHA,
		Blockers:          []string{},
		Freshness:         freshness,
		Confidence:        "HIGH",
	}
	if len(pr.Matches) == 1 {
		projection.ObservedCandidateSHA = pr.Matches[0].HeadSHA
		if pr.Matches[0].BindingBasis == "JIRA_KEY" {
			projection.Confidence = "MEDIUM"
		}
	}

	if source.Qualification != "CONFIRMED" {
		projection.State = "NON_DISPATCHABLE"
		projection.Blockers = append(projection.Blockers, "QUALIFICATION_"+source.Qualification)
		switch source.Qualification {
		case "QUESTION":
			projection.NextAction = "HUMAN_DECISION_REQUIRED"
		default:
			projection.NextAction = "NO_ACTION"
		}

		return projection
	}

	switch {
	case jira.Status == "AMBIGUOUS":
		projection.State = "AMBIGUOUS"
		projection.Blockers = append(projection.Blockers, "AMBIGUOUS_JIRA_BINDING")
		projection.NextAction = "RESOLVE_BINDING"
	case pr.Status == "AMBIGUOUS":
		projection.State = "AMBIGUOUS"
		projection.Blockers = append(projection.Blockers, "AMBIGUOUS_PR_BINDING")
		projection.NextAction = "RESOLVE_BINDING"
	case claim.State == "AMBIGUOUS" || claim.State == "CLAIM_HISTORY_MISSING":
		projection.State = "AMBIGUOUS"
		projection.Blockers = append(projection.Blockers, claim.State)
		if claim.Problem != "" {
			projection.Blockers = append(projection.Blockers, claim.Problem)
		}
		projection.NextAction = "REFRESH_REQUIRED"
	case claim.State == "BROKEN_BINDING":
		projection.State = "BROKEN_BINDING"
		projection.Blockers = append(projection.Blockers, claim.Problem)
		projection.NextAction = "REPAIR_BINDING"
	case len(pr.Matches) == 1:
		match := pr.Matches[0]
		switch {
		case match.BaseRef != targetBranch:
			projection.State = "BROKEN_BINDING"
			projection.Blockers = append(projection.Blockers, "PR_TARGET_MISMATCH")
			projection.NextAction = "REPAIR_BINDING"
		case match.Merged:
			projection.State = "MERGED"
			projection.NextAction = "VERIFY_RESOLUTION"
			if match.BindingBasis != "AI_AUDIT_MARKER" {
				projection.Blockers = append(projection.Blockers, "MERGED_WITHOUT_EXACT_AUDIT_MARKER")
			}
		case !match.BranchExists:
			projection.State = "BROKEN_BINDING"
			if match.CrossRepository {
				projection.Blockers = append(projection.Blockers, "CROSS_REPOSITORY_BRANCH_UNVERIFIED")
			} else {
				projection.Blockers = append(projection.Blockers, "PR_BRANCH_MISSING")
			}
			projection.NextAction = "REPAIR_BINDING"
		case strings.EqualFold(match.State, "OPEN"):
			projection.State = "PR_OPEN"
			projection.NextAction = "CONTINUE_PR"
		case strings.EqualFold(match.State, "CLOSED"):
			projection.State = "PR_CLOSED"
			projection.Blockers = append(projection.Blockers, "PR_CLOSED_UNMERGED")
			projection.NextAction = "REVIEW_CLOSED_PR"
		default:
			projection.State = "BROKEN_BINDING"
			projection.Blockers = append(projection.Blockers, "PR_STATE_UNKNOWN")
			projection.NextAction = "REPAIR_BINDING"
		}
	case targetSHA == "":
		projection.State = "BLOCKED"
		projection.Blockers = append(projection.Blockers, "TARGET_SHA_UNKNOWN")
		projection.NextAction = "REFRESH_REQUIRED"
	case targetSHA != "" && targetSHA != auditedSHA:
		if claim.WorkIdentity != "" {
			projection.State = "STALE_AT_DISPATCH"
			projection.NextAction = "BASE_UPDATE_REQUIRED"
		} else {
			projection.State = "BLOCKED"
			projection.NextAction = "REQUALIFY_ON_CURRENT_TARGET"
		}
		projection.Blockers = append(projection.Blockers, "TARGET_ADVANCED")
	case claim.State == "CLAIM_EXPIRED":
		projection.State = "CLAIM_EXPIRED"
		projection.NextAction = "REVIEW_EXPIRED_CLAIM"
	case claim.State == "CLAIMED":
		if claim.WorkIdentity != "" {
			projection.State = "DISPATCHED"
			if claim.OwnedBySession {
				projection.NextAction = "START_ENGINEERING_AGENT"
			} else {
				projection.NextAction = "WAIT_OR_COORDINATE"
			}
		} else {
			projection.State = "CLAIMED"
			if claim.OwnedBySession {
				projection.NextAction = "DISPATCH"
			} else {
				projection.NextAction = "WAIT_OR_COORDINATE"
			}
		}
	case len(jira.Issues) == 1:
		projection.State = "TRACKED"
		projection.NextAction = "CLAIM"
	default:
		projection.State = "CONFIRMED_UNASSIGNED"
		projection.NextAction = "CLAIM"
	}

	if freshness != "FRESH" {
		projection.Freshness = freshness
		projection.Confidence = "LOW"
		projection.Blockers = appendUnique(projection.Blockers, "STALE_EXTERNAL_TRUTH")
		projection.NextAction = "REFRESH_REQUIRED"
	}

	return projection
}

func buildInspection(campaign *Campaign) *Inspection {
	observations := campaign.Observations
	inspection := &Inspection{
		SchemaVersion: inspectionSchemaVersion,
		CampaignID:    campaign.CampaignID,
		AuditID:       campaign.AuditID,
		AuditedSHA:    campaign.AuditedSHA,
		SourceDigests: campaign.SourceDigests,
		RefreshedAt:   observations.RefreshedAt,
		Freshness:     observations.Freshness,
		Providers:     observations.Providers,
		Findings:      []InspectionFinding{},
	}
	byID := make(map[string]FindingObservation, len(observations.Findings))
	for _, finding := range observations.Findings {
		byID[finding.ID] = finding
	}
	for _, source := range campaign.SourceFacts.Findings {
		observation := byID[source.ID]
		inspection.Findings = append(inspection.Findings, InspectionFinding{
			ID:                   source.ID,
			Title:                source.Title,
			Severity:             source.Severity,
			Qualification:        source.Qualification,
			Dispatchable:         source.Dispatchable,
			State:                observation.State,
			Jira:                 observation.Jira,
			PR:                   observation.PR,
			Claim:                observation.Claim,
			ObservedCandidateSHA: observation.ObservedCandidateSHA,
			ObservedTargetSHA:    observation.ObservedTargetSHA,
			Blockers:             nonNilStrings(observation.Blockers),
			NextAction:           observation.NextAction,
			Freshness:            observation.Freshness,
			Confidence:           observation.Confidence,
		})
	}
	// Persist the same final projection that inspect emits; next therefore never
	// reinterprets a different state from the cached campaign artifact.
	observationIndexes := make(map[string]int, len(observations.Findings))
	for index, observation := range observations.Findings {
		observationIndexes[observation.ID] = index
	}
	for _, projected := range inspection.Findings {
		index := observationIndexes[projected.ID]
		observations.Findings[index].State = projected.State
		observations.Findings[index].Blockers = projected.Blockers
		observations.Findings[index].NextAction = projected.NextAction
	}

	return inspection
}

func freshProvider(now time.Time) ProviderObservation {
	return ProviderObservation{Status: "FRESH", ObservedAt: &now}
}

func unavailableProvider(previous ProviderObservation, err error) ProviderObservation {
	if previous.ObservedAt != nil {
		return ProviderObservation{Status: "STALE", ObservedAt: previous.ObservedAt, Error: conciseError(err)}
	}

	return ProviderObservation{Status: "UNAVAILABLE", Error: conciseError(err)}
}

func staleProvider(previous ProviderObservation, reason string) ProviderObservation {
	status := "UNAVAILABLE"
	if previous.ObservedAt != nil {
		status = "STALE"
	}

	return ProviderObservation{Status: status, ObservedAt: previous.ObservedAt, Error: reason}
}

func combinedFreshness(providers ProviderObservations) string {
	statuses := []string{providers.GitHub.Status, providers.Jira.Status, providers.Git.Status, providers.Claims.Status}
	fresh := 0
	stale := 0
	for _, status := range statuses {
		switch status {
		case "FRESH":
			fresh++
		case "STALE":
			stale++
		}
	}
	switch {
	case fresh == len(statuses):
		return "FRESH"
	case fresh > 0:
		return "PARTIAL"
	case stale > 0:
		return "STALE"
	default:
		return "UNAVAILABLE"
	}
}

func bindingStatus(count int) string {
	switch count {
	case 0:
		return "UNBOUND"
	case 1:
		return "BOUND"
	default:
		return "AMBIGUOUS"
	}
}

func appendUnique(values []string, value string) []string {
	if !slices.Contains(values, value) {
		return append(values, value)
	}

	return values
}

func nonNilStrings(values []string) []string {
	if values == nil {
		return []string{}
	}

	return values
}

func nonNilJiraIssues(values []JiraIssue) []JiraIssue {
	if values == nil {
		return []JiraIssue{}
	}

	return values
}

func nonNilPRMatches(values []PRMatch) []PRMatch {
	if values == nil {
		return []PRMatch{}
	}

	return values
}

func conciseError(err error) string {
	message := strings.TrimSpace(err.Error())
	if newline := strings.IndexByte(message, '\n'); newline >= 0 {
		message = message[:newline]
	}
	if len(message) > 240 {
		message = message[:240]
	}

	return message
}
