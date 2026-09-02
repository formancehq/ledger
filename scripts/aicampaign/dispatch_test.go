package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type dispatchFixture struct {
	remote       string
	repo         string
	campaign     *Campaign
	finding      SourceFinding
	evidence     dispatchEvidence
	claim        ClaimResult
	observed     InspectionFinding
	options      dispatchOptions
	dispatchTime time.Time
}

func TestConfirmedOwnedClaimDispatchesCanonicalWork(t *testing.T) {
	fixture := newDispatchFixture(t, "CONFIRMED", testClaimantA, "BUGFIX", true)
	result := runFixtureDispatch(fixture)
	require.Equal(t, "DISPATCHED", result.Status, result.Reason)
	require.Equal(t, "START_ENGINEERING_AGENT", result.NextAction)
	require.Equal(t, fixture.campaign.AuditedSHA, remoteRefSHA(t, fixture.remote,
		"refs/heads/"+result.Resources.WorkBranch))
	require.Equal(t, fixture.campaign.AuditedSHA, strings.TrimSpace(runGit(t, result.Resources.WorktreePath, "rev-parse", "HEAD")))
	require.Equal(t, result.Resources.WorkBranch, strings.TrimSpace(runGit(t, result.Resources.WorktreePath,
		"branch", "--show-current")))
	require.FileExists(t, result.Resources.WorkItemPath)
	require.NotNil(t, result.WorkItem)
	require.Equal(t, result.Resources.WorkIdentity, result.WorkItem.RemoteIdentity.WorkIdentity)
	require.Equal(t, []string{
		"AI-AUDIT:" + fixture.finding.ID,
		"AI-CAMPAIGN-WORK:" + result.Resources.WorkIdentity,
		"Jira: EN-42",
	}, result.WorkItem.RemoteIdentity.PRMarkers)
	require.Contains(t, result.WorkItem.EngineeringTask, "BEFORE_FIX")
	require.Contains(t, result.WorkItem.EngineeringTask, fixture.evidence.Audit.ViolatedInvariant)

	claim := readRemoteClaim(t, fixture.remote, result.Resources.ClaimRef)
	require.Equal(t, result.Resources.WorkIdentity, claim.Record.WorkIdentity)
	require.Equal(t, result.Resources.WorkBranch, claim.Record.WorkBranch)
	require.Equal(t, fixture.campaign.AuditedSHA, claim.Record.InitialWorkSHA)
	require.Equal(t, fixture.campaign.AuditedSHA, claim.Record.TargetBaseSHA)
	require.Equal(t, "BUGFIX", claim.Record.Workflow)
	require.Equal(t, "EN-42", claim.Record.JiraKey)
	require.Equal(t, fixture.claim.ClaimSHA, claimParent(t, fixture.remote, claim.SHA))
}

func TestDispatchEligibilityRefusals(t *testing.T) {
	tests := []struct {
		name      string
		configure func(*dispatchFixture)
		status    string
	}{
		{
			name: "no claim", status: "CLAIM_REQUIRED",
			configure: func(fixture *dispatchFixture) {
				runGit(t, "", "--git-dir", fixture.remote, "update-ref", "-d", fixture.claim.RemoteRef)
				fixture.observed.Claim = emptyClaimObservation(fixture.campaign.AuditID, fixture.finding.ID, "FRESH")
			},
		},
		{
			name: "another owner", status: "CLAIM_NOT_OWNER",
			configure: func(fixture *dispatchFixture) {
				runGit(t, "", "--git-dir", fixture.remote, "update-ref", "-d", fixture.claim.RemoteRef)
				other := createClaim(context.Background(), fixture.campaign, fixture.finding, testClaimantB,
					defaultClaimLease, fixture.remote, testTarget, "", func() time.Time { return testNow })
				require.Equal(t, "CLAIMED", other.Status, other.Reason)
				fixture.observed.Claim = observationFromClaim(&observedClaim{
					Record: *other.Claim, SHA: other.ClaimSHA, Ref: other.RemoteRef,
				}, testClaimantA)
			},
		},
		{
			name: "expired claim", status: "CLAIM_EXPIRED",
			configure: func(fixture *dispatchFixture) {
				expired := fixture.dispatchTime.Add(-claimClockSkew)
				fixture.observed.Claim.ExpiresAt = &expired
				fixture.observed.Claim.State = "CLAIM_EXPIRED"
			},
		},
		{
			name: "workflow unknown", status: "HUMAN_DECISION_REQUIRED",
			configure: func(fixture *dispatchFixture) { fixture.options.workflow = "" },
		},
		{
			name: "runtime bugfix without Jira", status: "JIRA_REQUIRED",
			configure: func(fixture *dispatchFixture) {
				fixture.observed.Jira = JiraObservation{Status: "UNBOUND", BindingBasis: "AI_AUDIT_MARKER", Issues: []JiraIssue{}}
			},
		},
		{
			name: "remote unavailable", status: "REMOTE_UNAVAILABLE",
			configure: func(fixture *dispatchFixture) { fixture.observed.Freshness = "UNAVAILABLE" },
		},
		{
			name: "target already advanced", status: "BASE_UPDATE_REQUIRED",
			configure: func(fixture *dispatchFixture) { fixture.observed.ObservedTargetSHA = testHead },
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newDispatchFixture(t, "CONFIRMED", testClaimantA, "BUGFIX", true)
			test.configure(&fixture)
			result := runFixtureDispatch(fixture)
			require.Equal(t, test.status, result.Status, result.Reason)
			if result.Resources.WorkBranch != "" {
				require.Empty(t, remoteRefSHA(t, fixture.remote, "refs/heads/"+result.Resources.WorkBranch))
			}
		})
	}
}

func TestNonConfirmedFindingsAreNeverDispatched(t *testing.T) {
	for _, qualification := range []string{"LIKELY", "QUESTION", "REJECTED"} {
		t.Run(qualification, func(t *testing.T) {
			fixture := newDispatchFixture(t, qualification, "", "TEST_GAP", false)
			result := runFixtureDispatch(fixture)
			require.Equal(t, "FINDING_NOT_CONFIRMED", result.Status)
		})
	}
}

func TestTestGapDoesNotRequireJiraOrRuntimeBugEvidence(t *testing.T) {
	fixture := newDispatchFixture(t, "CONFIRMED", testClaimantA, "TEST_GAP", false)
	result := runFixtureDispatch(fixture)
	require.Equal(t, "DISPATCHED", result.Status, result.Reason)
	require.NotContains(t, result.WorkItem.RequiredGates, "BEFORE_FIX")
	require.Contains(t, result.WorkItem.RequiredGates, "TEST_GAP_EVIDENCE")
	require.Empty(t, result.WorkItem.RemoteIdentity.JiraKey)
}

func TestExistingAndAmbiguousPRBindingsStopBeforeBranchCreation(t *testing.T) {
	fixture := newDispatchFixture(t, "CONFIRMED", testClaimantA, "BUGFIX", true)
	fixture.observed.PR = PRObservation{Status: "BOUND", BindingBasis: "AI_AUDIT_MARKER", Matches: []PRMatch{
		openPR(42, "existing", "AI_AUDIT_MARKER"),
	}}
	result := runFixtureDispatch(fixture)
	require.Equal(t, "EXISTING_PR", result.Status)
	require.Equal(t, "CONTINUE_PR", result.NextAction)
	require.NotNil(t, result.ExistingPR)

	ambiguous := newDispatchFixture(t, "CONFIRMED", testClaimantA, "BUGFIX", true)
	ambiguous.observed.PR = PRObservation{Status: "AMBIGUOUS", BindingBasis: "AI_AUDIT_MARKER", Matches: []PRMatch{
		openPR(42, "first", "AI_AUDIT_MARKER"), openPR(43, "second", "AI_AUDIT_MARKER"),
	}}
	result = runFixtureDispatch(ambiguous)
	require.Equal(t, "AMBIGUOUS_BINDING", result.Status)
}

func TestLastMileCampaignWorkPRBindingStopsDispatch(t *testing.T) {
	fixture := newDispatchFixture(t, "CONFIRMED", testClaimantA, "BUGFIX", true)
	fixture.options.findWorkPRs = func(_ context.Context, _ string, marker string) ([]PRMatch, error) {
		require.Equal(t, "AI-CAMPAIGN-WORK:"+workIdentity(fixture.campaign, fixture.finding, "EN-42"), marker)

		return []PRMatch{openPR(44, "last-mile", "AI_CAMPAIGN_WORK_MARKER")}, nil
	}
	result := runFixtureDispatch(fixture)
	require.Equal(t, "EXISTING_PR", result.Status, result.Reason)
	require.Equal(t, "CONTINUE_PR", result.NextAction)
	require.Empty(t, remoteRefSHA(t, fixture.remote, "refs/heads/"+result.Resources.WorkBranch))

	ambiguous := newDispatchFixture(t, "CONFIRMED", testClaimantA, "BUGFIX", true)
	ambiguous.options.findWorkPRs = func(context.Context, string, string) ([]PRMatch, error) {
		return []PRMatch{
			openPR(44, "first", "AI_CAMPAIGN_WORK_MARKER"),
			openPR(45, "second", "AI_CAMPAIGN_WORK_MARKER"),
		}, nil
	}
	result = runFixtureDispatch(ambiguous)
	require.Equal(t, "AMBIGUOUS_BINDING", result.Status, result.Reason)
}

func TestCampaignWorkPRCreatedBeforeClaimCASStopsDispatch(t *testing.T) {
	fixture := newDispatchFixture(t, "CONFIRMED", testClaimantA, "BUGFIX", true)
	searches := 0
	fixture.options.findWorkPRs = func(_ context.Context, _ string, marker string) ([]PRMatch, error) {
		require.Equal(t, "AI-CAMPAIGN-WORK:"+workIdentity(fixture.campaign, fixture.finding, "EN-42"), marker)
		searches++
		if searches == 1 {
			return []PRMatch{}, nil
		}

		return []PRMatch{openPR(46, "raced", "AI_CAMPAIGN_WORK_MARKER")}, nil
	}
	result := runFixtureDispatch(fixture)
	require.Equal(t, "EXISTING_PR", result.Status, result.Reason)
	require.Equal(t, "CONTINUE_PR", result.NextAction)
	require.Equal(t, 2, searches)
	require.Empty(t, readRemoteClaim(t, fixture.remote, fixture.claim.RemoteRef).Record.WorkIdentity)
	require.Equal(t, fixture.campaign.AuditedSHA, remoteRefSHA(t, fixture.remote,
		"refs/heads/"+result.Resources.WorkBranch))
}

func TestMergedExactPRIsAlreadyResolvedButJiraOnlyIsNotProof(t *testing.T) {
	fixture := newDispatchFixture(t, "CONFIRMED", testClaimantA, "BUGFIX", true)
	merged := openPR(42, "merged", "AI_AUDIT_MARKER")
	merged.Merged = true
	merged.State = "MERGED"
	fixture.observed.PR = PRObservation{Status: "BOUND", BindingBasis: merged.BindingBasis, Matches: []PRMatch{merged}}
	result := runFixtureDispatch(fixture)
	require.Equal(t, "ALREADY_MERGED", result.Status)

	jiraOnly := newDispatchFixture(t, "CONFIRMED", testClaimantA, "BUGFIX", true)
	merged.BindingBasis = "JIRA_KEY"
	jiraOnly.observed.PR = PRObservation{Status: "BOUND", BindingBasis: merged.BindingBasis, Matches: []PRMatch{merged}}
	result = runFixtureDispatch(jiraOnly)
	require.Equal(t, "BROKEN_BINDING", result.Status)
}

func TestUnrelatedCanonicalBranchCollisionIsRefused(t *testing.T) {
	fixture := newDispatchFixture(t, "CONFIRMED", testClaimantA, "BUGFIX", true)
	identity := workIdentity(fixture.campaign, fixture.finding, "EN-42")
	branch := workBranch("BUGFIX", fixture.finding.ID, "EN-42", identity)
	require.NoError(t, os.WriteFile(filepath.Join(fixture.repo, "collision"), []byte("unrelated\n"), 0o600))
	runGit(t, fixture.repo, "add", "collision")
	runGit(t, fixture.repo, "-c", "user.name=Test", "-c", "user.email=test@example.invalid",
		"commit", "--quiet", "-m", "unrelated")
	runGit(t, fixture.repo, "push", "--quiet", "origin", "HEAD:refs/heads/"+branch)

	result := runFixtureDispatch(fixture)
	require.Equal(t, "CONFLICTING_BRANCH", result.Status, result.Reason)
	require.Empty(t, readRemoteClaim(t, fixture.remote, fixture.claim.RemoteRef).Record.WorkIdentity)
}

func TestTargetAdvanceDuringDispatchStopsBeforeClaimBinding(t *testing.T) {
	fixture := newDispatchFixture(t, "CONFIRMED", testClaimantA, "BUGFIX", true)
	fixture.options.beforeClaimBinding = func() {
		require.NoError(t, os.WriteFile(filepath.Join(fixture.repo, "advance"), []byte("advance\n"), 0o600))
		runGit(t, fixture.repo, "add", "advance")
		runGit(t, fixture.repo, "-c", "user.name=Test", "-c", "user.email=test@example.invalid",
			"commit", "--quiet", "-m", "advance target")
		runGit(t, fixture.repo, "push", "--quiet", "origin", "HEAD:refs/heads/"+testTarget)
	}
	result := runFixtureDispatch(fixture)
	require.Equal(t, "BASE_UPDATE_REQUIRED", result.Status, result.Reason)
	require.Empty(t, readRemoteClaim(t, fixture.remote, fixture.claim.RemoteRef).Record.WorkIdentity)
}

func TestTargetAdvanceAfterClaimCASProducesStaleWorkItem(t *testing.T) {
	fixture := newDispatchFixture(t, "CONFIRMED", testClaimantA, "BUGFIX", true)
	fixture.options.afterClaimBinding = func() {
		require.NoError(t, os.WriteFile(filepath.Join(fixture.repo, "advance-after-cas"), []byte("advance\n"), 0o600))
		runGit(t, fixture.repo, "add", "advance-after-cas")
		runGit(t, fixture.repo, "-c", "user.name=Test", "-c", "user.email=test@example.invalid",
			"commit", "--quiet", "-m", "advance after claim CAS")
		runGit(t, fixture.repo, "push", "--quiet", "origin", "HEAD:refs/heads/"+testTarget)
	}
	result := runFixtureDispatch(fixture)
	require.Equal(t, "STALE_AT_DISPATCH", result.Status, result.Reason)
	require.Equal(t, "BASE_UPDATE_REQUIRED", result.NextAction)
	require.NotNil(t, result.WorkItem)
	require.Equal(t, "STALE_AT_DISPATCH", result.WorkItem.Status)
	require.Equal(t, "BASE_UPDATE_REQUIRED", result.WorkItem.CanonicalNextAction)
	require.NotEmpty(t, readRemoteClaim(t, fixture.remote, fixture.claim.RemoteRef).Record.WorkIdentity)
}

func TestClaimChangeBetweenWorktreeAndBindingFailsCAS(t *testing.T) {
	fixture := newDispatchFixture(t, "CONFIRMED", testClaimantA, "BUGFIX", true)
	fixture.options.beforeClaimBinding = func() {
		renewed := renewClaim(context.Background(), fixture.campaign, fixture.finding, testClaimantA,
			defaultClaimLease, fixture.remote, testTarget, "", fixture.claim.ClaimSHA,
			func() time.Time { return fixture.dispatchTime.Add(time.Minute) })
		require.Equal(t, "RENEWED", renewed.Status, renewed.Reason)
	}
	result := runFixtureDispatch(fixture)
	require.Equal(t, "CLAIM_CHANGED", result.Status, result.Reason)
	require.Empty(t, readRemoteClaim(t, fixture.remote, fixture.claim.RemoteRef).Record.WorkIdentity)
}

func TestCrashAfterBranchCreationReconcilesExpectedBranch(t *testing.T) {
	fixture := newDispatchFixture(t, "CONFIRMED", testClaimantA, "BUGFIX", true)
	identity := workIdentity(fixture.campaign, fixture.finding, "EN-42")
	branch := workBranch("BUGFIX", fixture.finding.ID, "EN-42", identity)
	runGit(t, fixture.repo, "push", "--quiet", "--force-with-lease=refs/heads/"+branch+":", "origin",
		fixture.campaign.AuditedSHA+":refs/heads/"+branch)

	result := runFixtureDispatch(fixture)
	require.Equal(t, "DISPATCHED", result.Status, result.Reason)
	require.Equal(t, "EXPECTED_EXISTING_WORK", result.Resources.BranchState)
}

func TestDirtyDiscoveredInitialWorktreeRefusesClaimBinding(t *testing.T) {
	fixture := newDispatchFixture(t, "CONFIRMED", testClaimantA, "BUGFIX", true)
	identity := workIdentity(fixture.campaign, fixture.finding, "EN-42")
	branch := workBranch("BUGFIX", fixture.finding.ID, "EN-42", identity)
	runGit(t, fixture.repo, "push", "--quiet", "--force-with-lease=refs/heads/"+branch+":", "origin",
		fixture.campaign.AuditedSHA+":refs/heads/"+branch)
	dirtyWorktree := filepath.Join(t.TempDir(), "dirty-worktree")
	runGit(t, fixture.repo, "worktree", "add", "--quiet", "-b", branch, dirtyWorktree, fixture.campaign.AuditedSHA)
	require.NoError(t, os.WriteFile(filepath.Join(dirtyWorktree, "untracked"), []byte("contamination\n"), 0o600))

	result := runFixtureDispatch(fixture)
	require.Equal(t, "WORKTREE_CREATION_FAILED", result.Status, result.Reason)
	require.Equal(t, "CONFLICT", result.Resources.WorktreeState)
	require.Empty(t, readRemoteClaim(t, fixture.remote, fixture.claim.RemoteRef).Record.WorkIdentity)
}

func TestDiscoveredTrustedRootWorktreeRefusesClaimBinding(t *testing.T) {
	fixture := newDispatchFixture(t, "CONFIRMED", testClaimantA, "BUGFIX", true)
	identity := workIdentity(fixture.campaign, fixture.finding, "EN-42")
	branch := workBranch("BUGFIX", fixture.finding.ID, "EN-42", identity)
	runGit(t, fixture.repo, "push", "--quiet", "--force-with-lease=refs/heads/"+branch+":", "origin",
		fixture.campaign.AuditedSHA+":refs/heads/"+branch)
	runGit(t, fixture.repo, "checkout", "--quiet", "-b", branch, fixture.campaign.AuditedSHA)

	result := runFixtureDispatch(fixture)
	require.Equal(t, "WORKTREE_CREATION_FAILED", result.Status, result.Reason)
	require.Equal(t, "FORBIDDEN", result.Resources.WorktreeState)
	require.Contains(t, result.Reason, "isolation boundary")
	require.Empty(t, readRemoteClaim(t, fixture.remote, fixture.claim.RemoteRef).Record.WorkIdentity)
}

func TestCrashAfterClaimBindingReconstructsArtifact(t *testing.T) {
	fixture := newDispatchFixture(t, "CONFIRMED", testClaimantA, "BUGFIX", true)
	fixture.options.writeWorkItem = func(string, *CampaignWork) error { return errors.New("simulated crash") }
	first := runFixtureDispatch(fixture)
	require.Equal(t, "WORK_ITEM_WRITE_FAILED", first.Status, first.Reason)
	bound := readRemoteClaim(t, fixture.remote, fixture.claim.RemoteRef)
	require.NotEmpty(t, bound.Record.WorkIdentity)
	runGit(t, fixture.repo, "worktree", "remove", first.Resources.WorktreePath)

	fixture.options.writeWorkItem = nil
	fixture.observed.Claim = observationFromClaim(bound, testClaimantA)
	second := runFixtureDispatch(fixture)
	require.Equal(t, "ALREADY_DISPATCHED", second.Status, second.Reason)
	require.FileExists(t, second.Resources.WorkItemPath)
}

func TestRepeatedDispatchAndMalformedArtifactAreIdempotent(t *testing.T) {
	fixture := newDispatchFixture(t, "CONFIRMED", testClaimantA, "BUGFIX", true)
	first := runFixtureDispatch(fixture)
	require.Equal(t, "DISPATCHED", first.Status, first.Reason)
	require.NoError(t, os.WriteFile(first.Resources.WorkItemPath, []byte("not json\n"), 0o600))
	bound := readRemoteClaim(t, fixture.remote, fixture.claim.RemoteRef)
	fixture.observed.Claim = observationFromClaim(bound, testClaimantA)
	fixture.options.worktreePath = filepath.Join(t.TempDir(), "would-be-duplicate")
	second := runFixtureDispatch(fixture)
	require.Equal(t, "ALREADY_DISPATCHED", second.Status, second.Reason)
	require.Equal(t, first.Resources.WorkBranch, second.Resources.WorkBranch)
	require.Equal(t, first.Resources.WorktreePath, second.Resources.WorktreePath)
	var reconstructed CampaignWork
	_, err := readStrictJSON(second.Resources.WorkItemPath, &reconstructed)
	require.NoError(t, err)
	require.Equal(t, first.Resources.WorkIdentity, reconstructed.RemoteIdentity.WorkIdentity)
}

func TestRepeatedStaleDispatchRemainsIdempotent(t *testing.T) {
	fixture := newDispatchFixture(t, "CONFIRMED", testClaimantA, "BUGFIX", true)
	first := runFixtureDispatch(fixture)
	require.Equal(t, "DISPATCHED", first.Status, first.Reason)

	require.NoError(t, os.WriteFile(filepath.Join(fixture.repo, "advance-before-repeat"), []byte("advance\n"), 0o600))
	runGit(t, fixture.repo, "add", "advance-before-repeat")
	runGit(t, fixture.repo, "-c", "user.name=Test", "-c", "user.email=test@example.invalid",
		"commit", "--quiet", "-m", "advance before repeated dispatch")
	runGit(t, fixture.repo, "push", "--quiet", "origin", "HEAD:refs/heads/"+testTarget)

	bound := readRemoteClaim(t, fixture.remote, fixture.claim.RemoteRef)
	fixture.observed.Claim = observationFromClaim(bound, testClaimantA)
	second := runFixtureDispatch(fixture)
	require.Equal(t, "ALREADY_DISPATCHED", second.Status, second.Reason)
	require.Equal(t, "BASE_UPDATE_REQUIRED", second.NextAction)
	require.NotNil(t, second.WorkItem)
	require.Equal(t, "STALE_AT_DISPATCH", second.WorkItem.Status)
}

func TestInspectAndNextProjectDispatchedAndStaleStates(t *testing.T) {
	fixture := newDispatchFixture(t, "CONFIRMED", testClaimantA, "BUGFIX", true)
	result := runFixtureDispatch(fixture)
	require.Equal(t, "DISPATCHED", result.Status, result.Reason)
	runner := inspector{
		now:  func() time.Time { return fixture.dispatchTime.Add(time.Minute) },
		jira: fakeJiraProvider{issues: fixture.observed.Jira.Issues}, github: fakeGitHubProvider{},
		git: commandGitProvider{}, claims: commandClaimProvider{},
	}
	inspection := runner.run(context.Background(), fixture.campaign, inspectOptions{
		repository: "formancehq/ledger", jiraProject: "EN", remote: fixture.remote,
		target: testTarget, claimant: testClaimantA,
	})
	require.Equal(t, "DISPATCHED", inspection.Findings[0].State)
	require.Equal(t, "START_ENGINEERING_AGENT", inspection.Findings[0].NextAction)
	next := buildNextResult(fixture.campaign)
	require.Equal(t, "START_ENGINEERING_AGENT", next.Findings[0].NextAction)

	require.NoError(t, os.WriteFile(filepath.Join(fixture.repo, "advance-after"), []byte("advance\n"), 0o600))
	runGit(t, fixture.repo, "add", "advance-after")
	runGit(t, fixture.repo, "-c", "user.name=Test", "-c", "user.email=test@example.invalid",
		"commit", "--quiet", "-m", "advance after dispatch")
	runGit(t, fixture.repo, "push", "--quiet", "origin", "HEAD:refs/heads/"+testTarget)
	inspection = runner.run(context.Background(), fixture.campaign, inspectOptions{
		repository: "formancehq/ledger", jiraProject: "EN", remote: fixture.remote,
		target: testTarget, claimant: testClaimantA,
	})
	require.Equal(t, "STALE_AT_DISPATCH", inspection.Findings[0].State)
	require.Equal(t, "BASE_UPDATE_REQUIRED", inspection.Findings[0].NextAction)
}

func TestTrustedAbsoluteLauncherIgnoresCandidateToolingChanges(t *testing.T) {
	_, filename, _, ok := runtime.Caller(0)
	require.True(t, ok)
	repoRoot := filepath.Clean(filepath.Join(filepath.Dir(filename), "..", ".."))
	candidate := t.TempDir()
	runGit(t, candidate, "init", "--quiet")
	for _, name := range []string{"go.mod", "go.sum"} {
		content, err := os.ReadFile(filepath.Join(repoRoot, name))
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(filepath.Join(candidate, name), content, 0o600))
	}
	require.NoError(t, os.MkdirAll(filepath.Join(candidate, "scripts", "aicampaign"), 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(candidate, "scripts", "aicampaign", "main.go"),
		[]byte("this is deliberately invalid candidate tooling\n"), 0o600))
	command := exec.Command("bash", filepath.Join(repoRoot, "scripts", "ai-campaign"), "--help")
	command.Dir = candidate
	output, err := command.CombinedOutput()
	require.NoError(t, err, "%s", output)
	require.Contains(t, string(output), "scripts/ai-campaign dispatch")
}

func TestDispatchWorktreeRejectsTrustedAndValidationPaths(t *testing.T) {
	fixture := newDispatchFixture(t, "CONFIRMED", testClaimantA, "BUGFIX", true)
	identity := workIdentity(fixture.campaign, fixture.finding, "EN-42")
	branch := workBranch("BUGFIX", fixture.finding.ID, "EN-42", identity)
	_, state, err := ensureDispatchWorktree(fixture.repo, "origin", branch, fixture.campaign.AuditedSHA,
		filepath.Join(fixture.repo, "candidate"), false)
	require.ErrorContains(t, err, "outside the trusted checkout")
	require.Equal(t, "FORBIDDEN", state)

	validationPath := filepath.Join(filepath.Dir(fixture.repo), ".repo-ai-worktrees", "pr-42", "validation", "worktree")
	_, state, err = ensureDispatchWorktree(fixture.repo, "origin", branch, fixture.campaign.AuditedSHA,
		validationPath, false)
	require.ErrorContains(t, err, "validation run directory")
	require.Equal(t, "FORBIDDEN", state)
}

func TestSameFindingChangedQualifiedDigestConflictsWithClaimLane(t *testing.T) {
	fixture := newDispatchFixture(t, "CONFIRMED", testClaimantA, "BUGFIX", true)
	fixture.campaign = cloneCampaignWithQualifiedDigest(fixture.campaign,
		"sha256:"+fmt.Sprintf("%064x", 42))
	fixture.finding = fixture.campaign.SourceFacts.Findings[0]
	fixture.observed.ID = fixture.finding.ID
	result := runFixtureDispatch(fixture)
	require.Equal(t, "BROKEN_BINDING", result.Status, result.Reason)
}

func TestTwoSimultaneousDispatchAttemptsAtMostOneBindsClaim(t *testing.T) {
	first := newDispatchFixture(t, "CONFIRMED", testClaimantA, "BUGFIX", true)
	secondRepo := filepath.Join(t.TempDir(), "second")
	runGit(t, "", "clone", "--quiet", first.remote, secondRepo)
	runGit(t, secondRepo, "checkout", "--quiet", "-b", testTarget, "origin/"+testTarget)
	second := first
	second.repo = secondRepo
	second.options.repoRoot = secondRepo
	second.options.worktreePath = filepath.Join(t.TempDir(), "worktree")
	second.options.workItemPath = filepath.Join(t.TempDir(), "work.json")
	second.options.campaignPath = filepath.Join(t.TempDir(), "campaign.json")
	require.NoError(t, writeCampaign(second.options.campaignPath, second.campaign))

	start := make(chan struct{})
	results := make(chan DispatchResult, 2)
	var wait sync.WaitGroup
	for _, fixture := range []dispatchFixture{first, second} {
		wait.Go(func() {
			<-start
			results <- runFixtureDispatch(fixture)
		})
	}
	close(start)
	wait.Wait()
	close(results)
	statuses := []string{}
	for result := range results {
		statuses = append(statuses, result.Status)
	}
	sort.Strings(statuses)
	require.Contains(t, statuses, "DISPATCHED")
	require.NotContains(t, statuses, "CONFLICTING_BRANCH")
	bound := readRemoteClaim(t, first.remote, first.claim.RemoteRef)
	require.NotEmpty(t, bound.Record.WorkIdentity)
}

func TestDispatchClaimBindingTransitionRejectsMutation(t *testing.T) {
	fixture := newDispatchFixture(t, "CONFIRMED", testClaimantA, "BUGFIX", true)
	result := runFixtureDispatch(fixture)
	require.Equal(t, "DISPATCHED", result.Status, result.Reason)
	bound := readRemoteClaim(t, fixture.remote, fixture.claim.RemoteRef)
	mutated := bound.Record
	mutated.InitialWorkSHA = testHead
	renewedAt := fixture.dispatchTime.Add(time.Minute)
	mutated.RenewedAt = &renewedAt
	mutated.RenewalCount++
	mutated.ExpiresAt = renewedAt.Add(defaultClaimLease)
	require.ErrorContains(t, validateClaimTransition(mutated, bound.Record, bound.SHA), "renewal changed dispatch binding")
}

func TestClaimDispatchBindingRequiresExactTargetLineage(t *testing.T) {
	fixture := newDispatchFixture(t, "CONFIRMED", testClaimantA, "BUGFIX", true)
	record := *fixture.claim.Claim
	dispatchedAt := fixture.dispatchTime
	record.WorkIdentity = "work-" + strings.Repeat("a", 64)
	record.WorkBranch = "fix/audit-finding-aaaaaaaaaaaa"
	record.InitialWorkSHA = testHead
	record.TargetBaseSHA = record.TargetSHA
	record.Workflow = "BUGFIX"
	record.JiraKey = "EN-42"
	record.DispatchedAt = &dispatchedAt
	require.ErrorContains(t, record.validate(), "initial work SHA does not match target base SHA")

	record.InitialWorkSHA = testHead
	record.TargetBaseSHA = testHead
	require.ErrorContains(t, record.validate(), "dispatch base does not match immutable target SHA")
}

func TestRenewAndTakeoverCannotRetargetDispatchedClaim(t *testing.T) {
	fixture := newDispatchFixture(t, "CONFIRMED", testClaimantA, "BUGFIX", true)
	result := runFixtureDispatch(fixture)
	require.Equal(t, "DISPATCHED", result.Status, result.Reason)
	bound := readRemoteClaim(t, fixture.remote, fixture.claim.RemoteRef)
	renewed := renewClaim(context.Background(), fixture.campaign, fixture.finding, testClaimantA,
		defaultClaimLease, fixture.remote, testTarget, "fix/other", bound.SHA,
		func() time.Time { return fixture.dispatchTime.Add(time.Hour) })
	require.Equal(t, "CLAIM_CONFLICT", renewed.Status)
	taken := takeoverClaim(context.Background(), fixture.campaign, fixture.finding, testClaimantB,
		defaultClaimLease, fixture.remote, testTarget, "fix/other", bound.SHA,
		func() time.Time { return bound.Record.ExpiresAt.Add(claimClockSkew) })
	require.Equal(t, "CLAIM_CONFLICT", taken.Status)
	require.Equal(t, bound.SHA, remoteRefSHA(t, fixture.remote, fixture.claim.RemoteRef))
}

func newDispatchFixture(
	t *testing.T,
	qualification string,
	claimant string,
	workflow string,
	withJira bool,
) dispatchFixture {
	t.Helper()
	remote, targetSHA := newClaimRemote(t)
	repo := filepath.Join(t.TempDir(), "repo")
	runGit(t, "", "clone", "--quiet", remote, repo)
	runGit(t, repo, "checkout", "--quiet", "-b", testTarget, "origin/"+testTarget)
	campaign := claimCampaignAt(targetSHA, qualification)
	finding := campaign.SourceFacts.Findings[0]
	evidence := dispatchEvidence{
		Audit: auditFinding{ID: finding.ID, Severity: finding.Severity, Title: finding.Title,
			ViolatedInvariant: "Every dispatched finding retains exact provenance", ReproductionPlan: "Reproduce first"},
		Qualified: challengeResult{ID: finding.ID, Severity: finding.Severity, Title: finding.Title,
			Status: qualification, ChallengeSummary: "Independently confirmed", EvidenceFor: []string{"evidence"},
			EvidenceAgainst: []string{}, ReproductionPlan: "Reproduce first"},
	}
	dispatchTime := testNow.Add(time.Minute)
	var claimed ClaimResult
	var claimObservation ClaimObservation
	if claimant != "" && qualification == "CONFIRMED" {
		claimed = createClaim(context.Background(), campaign, finding, claimant, defaultClaimLease,
			remote, testTarget, "", func() time.Time { return testNow })
		require.Equal(t, "CLAIMED", claimed.Status, claimed.Reason)
		claimObservation = observationFromClaim(&observedClaim{
			Record: *claimed.Claim, SHA: claimed.ClaimSHA, Ref: claimed.RemoteRef,
		}, claimant)
	} else {
		claimObservation = emptyClaimObservation(campaign.AuditID, finding.ID, "FRESH")
		if qualification != "CONFIRMED" {
			claimObservation.State = "NON_CLAIMABLE"
		}
	}
	jira := JiraObservation{Status: "UNBOUND", BindingBasis: "AI_AUDIT_MARKER", Issues: []JiraIssue{}}
	if withJira {
		jira.Status = "BOUND"
		jira.Issues = []JiraIssue{{Key: "EN-42", URL: "https://jira.example/EN-42", Status: "Open"}}
	}
	campaignPath := filepath.Join(t.TempDir(), "campaign.json")
	require.NoError(t, writeCampaign(campaignPath, campaign))

	return dispatchFixture{
		remote: remote, repo: repo, campaign: campaign, finding: finding, evidence: evidence, claim: claimed,
		observed: InspectionFinding{
			ID: finding.ID, Qualification: qualification, Dispatchable: qualification == "CONFIRMED",
			Freshness: "FRESH", Jira: jira, PR: PRObservation{Status: "UNBOUND", Matches: []PRMatch{}},
			Claim: claimObservation, ObservedTargetSHA: targetSHA,
		},
		options: dispatchOptions{
			repository: "formancehq/ledger", remote: "origin", target: testTarget, claimant: claimant,
			workflow: workflow, repoRoot: repo, campaignPath: campaignPath,
			worktreePath: filepath.Join(t.TempDir(), "worktree"), workItemPath: filepath.Join(t.TempDir(), "work.json"),
		},
		dispatchTime: dispatchTime,
	}
}

func runFixtureDispatch(fixture dispatchFixture) DispatchResult {
	return dispatchFinding(context.Background(), fixture.campaign, fixture.finding, fixture.observed,
		fixture.evidence, fixture.options, func() time.Time { return fixture.dispatchTime })
}

func observationFromClaim(claim *observedClaim, claimant string) ClaimObservation {
	return ClaimObservation{
		State: "CLAIMED", Claimant: claim.Record.Claimant, CreatedAt: &claim.Record.CreatedAt,
		ExpiresAt: &claim.Record.ExpiresAt, RemoteRef: claim.Ref, WorkBranch: claim.Record.WorkBranch,
		WorkIdentity: claim.Record.WorkIdentity, InitialWorkSHA: claim.Record.InitialWorkSHA,
		TargetBaseSHA: claim.Record.TargetBaseSHA, Workflow: claim.Record.Workflow, JiraKey: claim.Record.JiraKey,
		DispatchedAt: claim.Record.DispatchedAt, ObservedClaimSHA: claim.SHA, Freshness: "FRESH",
		OwnedBySession: claim.Record.Claimant == claimant,
	}
}

func readRemoteClaim(t *testing.T, remote, ref string) *observedClaim {
	t.Helper()
	repository, err := openClaimRepository(context.Background(), remote)
	require.NoError(t, err)
	t.Cleanup(repository.close)
	sha := remoteRefSHA(t, remote, ref)
	require.NotEmpty(t, sha)
	claim, err := repository.readClaim(context.Background(), ref, sha)
	require.NoError(t, err)

	return claim
}

func claimParent(t *testing.T, remote, sha string) string {
	t.Helper()

	return strings.TrimSpace(runGit(t, "", "--git-dir", remote, "show", "-s", "--format=%P", sha))
}
