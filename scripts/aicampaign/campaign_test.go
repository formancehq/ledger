package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const (
	testAuditID = "tooling-integrity"
	testHead    = "0123456789abcdef0123456789abcdef01234567"
	testTarget  = "release/v3.0"
)

var testNow = time.Date(2026, time.September, 1, 14, 0, 0, 0, time.UTC)

func TestImportValidQualifiedAudit(t *testing.T) {
	t.Parallel()

	sourcePath, qualifiedPath := writeImportFixture(t,
		[]fixtureFinding{{"confirmed", "CONFIRMED"}, {"likely", "LIKELY"}, {"question", "QUESTION"}, {"rejected", "REJECTED"}})
	campaign, err := (importer{now: func() time.Time { return testNow }}).run(sourcePath, qualifiedPath)
	require.NoError(t, err)
	require.Equal(t, campaignSchemaVersion, campaign.SchemaVersion)
	require.Equal(t, testAuditID, campaign.AuditID)
	require.Equal(t, testHead, campaign.AuditedSHA)
	require.Len(t, campaign.SourceFacts.Findings, 4)
	require.True(t, digestPattern.MatchString(campaign.SourceDigests.Audit))
	require.True(t, digestPattern.MatchString(campaign.SourceDigests.Qualified))
	require.NoError(t, campaign.validate())
}

func TestImportAllowsQualifiedAuditWithNoFindings(t *testing.T) {
	t.Parallel()

	sourcePath, qualifiedPath := writeImportFixture(t, []fixtureFinding{})
	campaign, err := (importer{now: func() time.Time { return testNow }}).run(sourcePath, qualifiedPath)
	require.NoError(t, err)
	require.Empty(t, campaign.SourceFacts.Findings)
	require.NotNil(t, campaign.SourceFacts.Findings)
	require.NoError(t, campaign.validate())

	inspection := runFakeInspection(campaign, fakeObservations{})
	require.Empty(t, inspection.Findings)
	require.NotNil(t, inspection.Findings)
	next := buildNextResult(campaign)
	require.Empty(t, next.Findings)
	require.NotNil(t, next.Findings)

	encoded, err := json.Marshal(campaign)
	require.NoError(t, err)
	require.Contains(t, string(encoded), `"findings":[]`)
}

func TestImportRetainsNonConfirmedFindingsAsNonDispatchable(t *testing.T) {
	t.Parallel()

	sourcePath, qualifiedPath := writeImportFixture(t,
		[]fixtureFinding{{"confirmed", "CONFIRMED"}, {"likely", "LIKELY"}, {"question", "QUESTION"}, {"rejected", "REJECTED"}})
	campaign, err := (importer{now: func() time.Time { return testNow }}).run(sourcePath, qualifiedPath)
	require.NoError(t, err)

	qualifications := map[string]SourceFinding{}
	for _, finding := range campaign.SourceFacts.Findings {
		qualifications[finding.Qualification] = finding
	}
	require.True(t, qualifications["CONFIRMED"].Dispatchable)
	for _, status := range []string{"LIKELY", "QUESTION", "REJECTED"} {
		require.False(t, qualifications[status].Dispatchable, status)
	}

	next := buildNextResult(campaign)
	actions := nextActions(next)
	require.Equal(t, "REFRESH_REQUIRED", actions[testAuditID+"/confirmed"])
	require.Equal(t, "NO_ACTION", actions[testAuditID+"/likely"])
	require.Equal(t, "HUMAN_DECISION_REQUIRED", actions[testAuditID+"/question"])
	require.Equal(t, "NO_ACTION", actions[testAuditID+"/rejected"])
}

func TestImportRejectsDuplicateFindingIDs(t *testing.T) {
	t.Parallel()

	sourcePath, qualifiedPath := writeImportFixture(t,
		[]fixtureFinding{{"duplicate", "CONFIRMED"}, {"duplicate", "REJECTED"}})
	_, err := (importer{now: time.Now}).run(sourcePath, qualifiedPath)
	require.ErrorContains(t, err, "duplicate finding id")
}

func TestImportRejectsDigestAndProvenanceMismatch(t *testing.T) {
	t.Parallel()

	for name, mutate := range map[string]func(map[string]any){
		"source digest": func(qualified map[string]any) {
			qualified["sourceAuditDigest"] = "sha256:" + strings.Repeat("f", 64)
		},
		"audit id": func(qualified map[string]any) { qualified["audit_id"] = "another-audit" },
		"head":     func(qualified map[string]any) { qualified["head"] = strings.Repeat("1", 40) },
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			sourcePath, qualifiedPath := writeImportFixture(t, []fixtureFinding{{"finding", "CONFIRMED"}})
			qualified := readObject(t, qualifiedPath)
			mutate(qualified)
			writeObject(t, qualifiedPath, qualified)
			_, err := (importer{now: time.Now}).run(sourcePath, qualifiedPath)
			require.Error(t, err)
		})
	}
}

func TestImportRejectsIncompleteQualificationSet(t *testing.T) {
	t.Parallel()

	sourcePath, qualifiedPath := writeImportFixture(t,
		[]fixtureFinding{{"first", "CONFIRMED"}, {"second", "LIKELY"}})
	qualified := readObject(t, qualifiedPath)
	qualified["results"] = qualified["results"].([]any)[:1]
	writeObject(t, qualifiedPath, qualified)
	_, err := (importer{now: time.Now}).run(sourcePath, qualifiedPath)
	require.ErrorContains(t, err, "incomplete qualification set")
}

func TestExactJiraMarkerDiscovery(t *testing.T) {
	t.Parallel()

	marker := "AI-AUDIT:" + testAuditID + "/finding"
	response := map[string]any{"issues": []any{
		map[string]any{"key": "EN-41", "fields": map[string]any{
			"description": "Context\n" + marker + "\n", "status": map[string]any{"name": "In Progress"},
			"assignee": map[string]any{"displayName": "Ledger Agent"},
		}},
		map[string]any{"key": "EN-42", "fields": map[string]any{"description": marker + "-extended"}},
		map[string]any{"key": "EN-43", "fields": map[string]any{"summary": marker, "description": "no marker"}},
	}}
	content, err := json.Marshal(response)
	require.NoError(t, err)
	issues, err := parseJiraIssues(content, marker)
	require.NoError(t, err)
	require.Equal(t, []JiraIssue{{Key: "EN-41", Status: "In Progress", Assignee: "Ledger Agent"}}, issues)
}

func TestExactPRBindingDiscovery(t *testing.T) {
	t.Parallel()

	marker := "AI-AUDIT:" + testAuditID + "/finding"
	require.True(t, exactLine("Context\n"+marker+"\n", marker))
	require.False(t, exactLine("Context\n"+marker+"-extended\n", marker))
	require.True(t, exactJiraReference("Fixes EN-4242", "EN-4242"))
	require.True(t, exactJiraReference("Fixes [EN-4242](https://example.test/EN-4242).", "EN-4242"))
	require.False(t, exactJiraReference("Possibly related: EN-4242", "EN-4242"))
}

func TestInspectMarksMultiplePRsAmbiguous(t *testing.T) {
	t.Parallel()

	campaign := testCampaign("CONFIRMED")
	inspection := runFakeInspection(campaign, fakeObservations{
		pullRequests: []PRMatch{
			openPR(41, "first", "AI_AUDIT_MARKER"),
			openPR(42, "second", "AI_AUDIT_MARKER"),
		},
	})
	finding := inspection.Findings[0]
	require.Equal(t, "AMBIGUOUS", finding.State)
	require.Equal(t, "RESOLVE_BINDING", finding.NextAction)
	require.Contains(t, finding.Blockers, "AMBIGUOUS_PR_BINDING")
}

func TestInspectClosedUnmergedPR(t *testing.T) {
	t.Parallel()

	campaign := testCampaign("CONFIRMED")
	closed := openPR(41, "closed", "AI_AUDIT_MARKER")
	closed.State = "CLOSED"
	inspection := runFakeInspection(campaign, fakeObservations{pullRequests: []PRMatch{closed}})
	require.Equal(t, "PR_CLOSED", inspection.Findings[0].State)
	require.Equal(t, "REVIEW_CLOSED_PR", inspection.Findings[0].NextAction)
	require.Contains(t, inspection.Findings[0].Blockers, "PR_CLOSED_UNMERGED")
}

func TestInspectMergedPRRequiresResolutionVerification(t *testing.T) {
	t.Parallel()

	for name, basis := range map[string]string{"exact marker": "AI_AUDIT_MARKER", "jira key only": "JIRA_KEY"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			campaign := testCampaign("CONFIRMED")
			merged := openPR(41, "merged", basis)
			merged.State = "MERGED"
			merged.Merged = true
			inspection := runFakeInspection(campaign, fakeObservations{pullRequests: []PRMatch{merged}})
			require.Equal(t, "MERGED", inspection.Findings[0].State)
			require.Equal(t, "VERIFY_RESOLUTION", inspection.Findings[0].NextAction)
			if basis == "JIRA_KEY" {
				require.Contains(t, inspection.Findings[0].Blockers, "MERGED_WITHOUT_EXACT_AUDIT_MARKER")
			}
		})
	}
}

func TestInspectOfflineGitHubNeverReportsReady(t *testing.T) {
	t.Parallel()

	campaign := testCampaign("CONFIRMED")
	inspection := runFakeInspection(campaign, fakeObservations{
		jira: []JiraIssue{{Key: "EN-41", Status: "Open"}}, githubError: errors.New("offline"),
	})
	require.Equal(t, "UNKNOWN", inspection.Findings[0].PR.Status)
	require.Equal(t, "REFRESH_REQUIRED", inspection.Findings[0].NextAction)
	require.NotEqual(t, "READY_FOR_CLAIM_OR_DISPATCH", inspection.Findings[0].NextAction)
}

func TestClaimedFindingStillHonorsFreshnessAndTargetGates(t *testing.T) {
	t.Parallel()

	campaign := testCampaign("CONFIRMED")
	source := campaign.SourceFacts.Findings[0]
	createdAt := testNow.Add(-time.Hour)
	expiresAt := testNow.Add(defaultClaimLease)
	claim := ClaimObservation{
		State: "CLAIMED", Claimant: testClaimantA, CreatedAt: &createdAt, ExpiresAt: &expiresAt,
		RemoteRef: claimRef(campaign.AuditID, source.ID), ObservedClaimSHA: strings.Repeat("c", 40),
		Freshness: "FRESH", OwnedBySession: true,
	}
	providerUnavailable := runFakeInspection(campaign, fakeObservations{
		claims: map[string]ClaimObservation{source.ID: claim}, githubError: errors.New("offline"),
	})
	require.Equal(t, "CLAIMED", providerUnavailable.Findings[0].State)
	require.Equal(t, "REFRESH_REQUIRED", providerUnavailable.Findings[0].NextAction)

	campaign = testCampaign("CONFIRMED")
	source = campaign.SourceFacts.Findings[0]
	claim.RemoteRef = claimRef(campaign.AuditID, source.ID)
	targetAdvanced := runFakeInspection(campaign, fakeObservations{
		claims: map[string]ClaimObservation{source.ID: claim}, targetSHA: strings.Repeat("a", 40),
	})
	require.Equal(t, "BLOCKED", targetAdvanced.Findings[0].State)
	require.Equal(t, "REQUALIFY_ON_CURRENT_TARGET", targetAdvanced.Findings[0].NextAction)
}

func TestInspectOfflineJiraMarksStatusUnknown(t *testing.T) {
	t.Parallel()

	campaign := testCampaign("CONFIRMED")
	inspection := runFakeInspection(campaign, fakeObservations{jiraError: errors.New("offline")})
	require.Equal(t, "UNKNOWN", inspection.Findings[0].Jira.Status)
	require.Equal(t, "REFRESH_REQUIRED", inspection.Findings[0].NextAction)
}

func TestOfflineInspectUsesStaleCache(t *testing.T) {
	t.Parallel()

	campaign := testCampaign("CONFIRMED")
	first := runFakeInspection(campaign, fakeObservations{jira: []JiraIssue{{Key: "EN-41", Status: "Open"}}})
	require.Equal(t, "CLAIM", first.Findings[0].NextAction)
	stale := (inspector{}).run(context.Background(), campaign, inspectOptions{target: testTarget, offline: true})
	require.Equal(t, "STALE", stale.Freshness)
	require.Equal(t, "EN-41", stale.Findings[0].Jira.Issues[0].Key)
	require.Equal(t, "UNKNOWN", stale.Findings[0].Jira.Status)
	require.Equal(t, "REFRESH_REQUIRED", stale.Findings[0].NextAction)
}

func TestNextActionIsDeterministic(t *testing.T) {
	t.Parallel()

	campaign := testCampaign("CONFIRMED", "LIKELY", "QUESTION", "REJECTED")
	runFakeInspection(campaign, fakeObservations{})
	first, err := json.Marshal(buildNextResult(campaign))
	require.NoError(t, err)
	second, err := json.Marshal(buildNextResult(campaign))
	require.NoError(t, err)
	require.Equal(t, first, second)
}

func TestMalformedCampaignStateFailsClosed(t *testing.T) {
	t.Parallel()

	campaign := testCampaign("CONFIRMED")
	path := filepath.Join(t.TempDir(), "campaign.json")
	require.NoError(t, writeCampaign(path, campaign))
	content := readObject(t, path)
	sourceFacts := content["sourceFacts"].(map[string]any)
	findings := sourceFacts["findings"].([]any)
	findings[0].(map[string]any)["qualification"] = "REJECTED"
	writeObject(t, path, content)
	_, err := loadCampaign(path)
	require.ErrorContains(t, err, "invalid campaign state")
}

func TestHistoricalCampaignWithoutBindingsRequiresRequalification(t *testing.T) {
	t.Parallel()

	campaign := testCampaign("CONFIRMED")
	inspection := runFakeInspection(campaign, fakeObservations{targetSHA: strings.Repeat("a", 40)})
	require.Equal(t, "BLOCKED", inspection.Findings[0].State)
	require.Equal(t, "REQUALIFY_ON_CURRENT_TARGET", inspection.Findings[0].NextAction)
	require.Contains(t, inspection.Findings[0].Blockers, "TARGET_ADVANCED")
}

func TestCommandsEmitHumanAndStructuredJSON(t *testing.T) {
	t.Parallel()

	sourcePath, qualifiedPath := writeImportFixture(t, []fixtureFinding{{"finding", "CONFIRMED"}})
	campaignPath := filepath.Join(t.TempDir(), "campaign.json")
	var importJSON bytes.Buffer
	var importHuman bytes.Buffer
	require.NoError(t, run([]string{
		"import", qualifiedPath, "--audit", sourcePath, "--output", campaignPath,
	}, &importJSON, &importHuman))
	require.Contains(t, importHuman.String(), "Imported campaign-")
	var imported Campaign
	require.NoError(t, json.Unmarshal(importJSON.Bytes(), &imported))
	require.Equal(t, testAuditID, imported.AuditID)

	var inspectJSON bytes.Buffer
	var inspectHuman bytes.Buffer
	require.NoError(t, run([]string{"inspect", campaignPath, "--offline"}, &inspectJSON, &inspectHuman))
	require.Contains(t, inspectHuman.String(), imported.CampaignID)
	var inspection Inspection
	require.NoError(t, json.Unmarshal(inspectJSON.Bytes(), &inspection))
	require.Equal(t, "UNAVAILABLE", inspection.Freshness)
	require.Equal(t, "REFRESH_REQUIRED", inspection.Findings[0].NextAction)

	var nextJSON bytes.Buffer
	var nextHuman bytes.Buffer
	require.NoError(t, run([]string{"next", campaignPath}, &nextJSON, &nextHuman))
	require.Contains(t, nextHuman.String(), "next actions")
	var next NextResult
	require.NoError(t, json.Unmarshal(nextJSON.Bytes(), &next))
	require.Equal(t, "REFRESH_REQUIRED", next.Findings[0].NextAction)
}

func TestMissingPRBranchIsBrokenBinding(t *testing.T) {
	t.Parallel()

	campaign := testCampaign("CONFIRMED")
	pullRequest := openPR(41, "deleted", "AI_AUDIT_MARKER")
	inspection := runFakeInspection(campaign, fakeObservations{
		pullRequests: pullRequestSlice(pullRequest), missingBranches: true,
	})
	require.Equal(t, "BROKEN_BINDING", inspection.Findings[0].State)
	require.Equal(t, "REPAIR_BINDING", inspection.Findings[0].NextAction)
}

func TestCrossRepositoryPRHeadOIDDoesNotProveBranchExists(t *testing.T) {
	t.Parallel()

	campaign := testCampaign("CONFIRMED")
	pullRequest := openPR(41, "fork-branch", "AI_AUDIT_MARKER")
	pullRequest.CrossRepository = true
	inspection := runFakeInspection(campaign, fakeObservations{
		pullRequests: pullRequestSlice(pullRequest),
	})
	require.Equal(t, "BROKEN_BINDING", inspection.Findings[0].State)
	require.Equal(t, "REPAIR_BINDING", inspection.Findings[0].NextAction)
	require.Contains(t, inspection.Findings[0].Blockers, "CROSS_REPOSITORY_BRANCH_UNVERIFIED")
}

type fixtureFinding struct {
	name   string
	status string
}

func writeImportFixture(t *testing.T, findings []fixtureFinding) (string, string) {
	t.Helper()

	sourceFindings := make([]any, 0, len(findings))
	qualifiedFindings := make([]any, 0, len(findings))
	for _, finding := range findings {
		id := testAuditID + "/" + finding.name
		sourceFindings = append(sourceFindings, map[string]any{
			"id": id, "severity": "P2", "title": "Title " + finding.name, "location": "scripts/example:1",
			"violated_invariant": "Invariant " + finding.name, "failure_path": "Failure path", "impact": "Impact",
			"evidence": "Evidence", "reproduction_plan": "Reproduce", "test_gap": "Gap", "confidence": "HIGH",
		})
		qualifiedFindings = append(qualifiedFindings, map[string]any{
			"id": id, "severity": "P2", "title": "Title " + finding.name, "status": finding.status,
			"challenge_summary": "Challenge", "evidence_for": []any{"Evidence"}, "evidence_against": []any{},
			"invariant_assessment": "Established", "reachability_assessment": "Reachable", "existing_tests": []any{},
			"reproduction_plan": "Reproduce", "recommended_next_action": "Next",
		})
	}
	source := map[string]any{
		"audit_id": testAuditID, "head": testHead, "summary": "Audit", "inspected_areas": []any{"scripts"},
		"findings": sourceFindings, "questions": []any{}, "residual_risk": "LOW",
	}
	directory := t.TempDir()
	sourcePath := filepath.Join(directory, "audit.json")
	qualifiedPath := filepath.Join(directory, "qualified.json")
	writeObject(t, sourcePath, source)
	sourceContent, err := os.ReadFile(sourcePath)
	require.NoError(t, err)
	qualified := map[string]any{
		"audit_id": testAuditID, "head": testHead, "sourceAuditDigest": digestBytes(sourceContent), "summary": "Challenge",
		"results": qualifiedFindings, "questions": []any{}, "residual_risk": "LOW",
	}
	writeObject(t, qualifiedPath, qualified)

	return sourcePath, qualifiedPath
}

func testCampaign(qualifications ...string) *Campaign {
	campaign := &Campaign{
		SchemaVersion: campaignSchemaVersion,
		AuditID:       testAuditID,
		AuditedSHA:    testHead,
		ImportedAt:    testNow,
		SourceDigests: SourceDigests{
			Audit: "sha256:" + strings.Repeat("a", 64), Qualified: "sha256:" + strings.Repeat("b", 64),
		},
		SourceFacts: SourceFacts{FactType: sourceFactType},
	}
	for _, qualification := range qualifications {
		id := testAuditID + "/finding"
		if len(qualifications) > 1 {
			id += "-" + strings.ToLower(qualification)
		}
		finding := SourceFinding{
			FactType: sourceFactType, ID: id, Title: "Finding", Severity: "P2", Qualification: qualification,
			Dispatchable: qualification == "CONFIRMED",
			References: FindingReferences{
				AuditFinding: "audit:#/findings", Invariant: "audit:#/invariant",
				QualificationResult: "qualified:#/results", ChallengeSummary: "qualified:#/summary",
			},
		}
		campaign.SourceFacts.Findings = append(campaign.SourceFacts.Findings, finding)
	}
	for index := range campaign.SourceFacts.Findings {
		campaign.SourceFacts.Findings[index].IdentityDigest = sourceFindingDigest(campaign, campaign.SourceFacts.Findings[index])
	}
	campaign.SourceFacts.IdentityDigest = sourceFactsDigest(campaign)
	campaign.CampaignID = campaignID(campaign)

	return campaign
}

type fakeObservations struct {
	jira            []JiraIssue
	pullRequests    []PRMatch
	jiraError       error
	githubError     error
	gitError        error
	claimError      error
	claims          map[string]ClaimObservation
	targetSHA       string
	missingBranches bool
}

func runFakeInspection(campaign *Campaign, observations fakeObservations) *Inspection {
	targetSHA := observations.targetSHA
	if targetSHA == "" {
		targetSHA = testHead
	}
	refs := map[string]string{"refs/heads/" + testTarget: targetSHA}
	if !observations.missingBranches {
		for _, pullRequest := range observations.pullRequests {
			refs["refs/heads/"+pullRequest.HeadRef] = pullRequest.HeadSHA
		}
	}
	runner := inspector{
		now:    func() time.Time { return testNow },
		jira:   fakeJiraProvider{issues: observations.jira, err: observations.jiraError},
		github: fakeGitHubProvider{pullRequests: observations.pullRequests, err: observations.githubError},
		git:    fakeGitProvider{refs: refs, err: observations.gitError},
		claims: fakeClaimProvider{values: observations.claims, err: observations.claimError},
	}

	return runner.run(context.Background(), campaign, inspectOptions{
		repository: "formancehq/ledger", jiraProject: "EN", remote: "origin", target: testTarget,
	})
}

type fakeJiraProvider struct {
	issues []JiraIssue
	err    error
}

func (provider fakeJiraProvider) Observe(_ context.Context, _ string, findings []SourceFinding) (map[string][]JiraIssue, error) {
	if provider.err != nil {
		return nil, provider.err
	}
	result := map[string][]JiraIssue{}
	for _, finding := range findings {
		result[finding.ID] = provider.issues
	}

	return result, nil
}

type fakeGitHubProvider struct {
	pullRequests []PRMatch
	err          error
}

func (provider fakeGitHubProvider) Observe(
	_ context.Context, _ string, findings []SourceFinding, _ map[string][]JiraIssue,
) (map[string][]PRMatch, error) {
	if provider.err != nil {
		return nil, provider.err
	}
	result := map[string][]PRMatch{}
	for _, finding := range findings {
		result[finding.ID] = provider.pullRequests
	}

	return result, nil
}

type fakeGitProvider struct {
	refs map[string]string
	err  error
}

type fakeClaimProvider struct {
	values map[string]ClaimObservation
	err    error
}

func (provider fakeClaimProvider) Observe(
	_ context.Context,
	_ string,
	campaign *Campaign,
	_ string,
	_ time.Time,
	_ string,
	_ map[string]string,
) (map[string]ClaimObservation, error) {
	if provider.err != nil {
		return nil, provider.err
	}
	values := make(map[string]ClaimObservation, len(campaign.SourceFacts.Findings))
	for _, finding := range campaign.SourceFacts.Findings {
		if observation, ok := provider.values[finding.ID]; ok {
			values[finding.ID] = observation
		} else if finding.Qualification == "CONFIRMED" {
			values[finding.ID] = emptyClaimObservation(campaign.AuditID, finding.ID, "FRESH")
		} else {
			observation := emptyClaimObservation(campaign.AuditID, finding.ID, "FRESH")
			observation.State = "NON_CLAIMABLE"
			values[finding.ID] = observation
		}
	}

	return values, nil
}

func (provider fakeGitProvider) ObserveRefs(context.Context, string, []string) (map[string]string, error) {
	return provider.refs, provider.err
}

func openPR(number int, branch, basis string) PRMatch {
	return PRMatch{
		Number: number, URL: "https://example.test/pr", State: "OPEN", HeadRef: branch,
		HeadSHA: strings.Repeat(string(rune('a'+number%20)), 40), BaseRef: testTarget, BaseSHA: testHead,
		BranchExists: true, ReviewDecision: "APPROVED", Checks: "SUCCESS", BindingBasis: basis,
	}
}

func pullRequestSlice(values ...PRMatch) []PRMatch { return values }

func nextActions(result *NextResult) map[string]string {
	actions := map[string]string{}
	for _, finding := range result.Findings {
		actions[finding.ID] = finding.NextAction
	}

	return actions
}

func writeObject(t *testing.T, path string, object map[string]any) {
	t.Helper()
	content, err := json.MarshalIndent(object, "", "  ")
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, append(content, '\n'), 0o600))
}

func readObject(t *testing.T, path string) map[string]any {
	t.Helper()
	content, err := os.ReadFile(path)
	require.NoError(t, err)
	var object map[string]any
	require.NoError(t, json.Unmarshal(content, &object))

	return object
}
