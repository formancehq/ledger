package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/scripts/internal/testenv"
)

const (
	testClaimantA = "agent@machine#session-a"
	testClaimantB = "agent@machine#session-b"
)

func TestMain(testingMain *testing.M) {
	if err := testenv.SanitizeProcess(); err != nil {
		panic(err)
	}
	os.Exit(testingMain.Run())
}

func TestClaimRefIsDeterministicAndDigestIndependent(t *testing.T) {
	t.Parallel()

	first := claimRef("audit-domain", "audit-domain/finding-one")
	second := claimRef("audit-domain", "audit-domain/finding-one")
	require.Equal(t, first, second)
	require.Regexp(t, `^refs/heads/ai-claims/v1/[0-9a-f]{64}$`, first)
	require.NotEqual(t, first, claimRef("audit-domain", "audit-domain/finding-two"))
}

func TestClaimCommandsEmitStructuredStatuses(t *testing.T) {
	remote, targetSHA := newClaimRemote(t)
	campaign := claimCampaignAt(targetSHA, "CONFIRMED")
	path := filepath.Join(t.TempDir(), "campaign.json")
	require.NoError(t, writeCampaign(path, campaign))
	config := filepath.Join(t.TempDir(), "gitconfig")
	require.NoError(t, os.WriteFile(config, []byte("[remote \"claimtest\"]\n\turl = "+remote+"\n"), 0o600))
	t.Setenv("GIT_CONFIG_GLOBAL", config)

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	require.NoError(t, run([]string{
		"claim", path, "--finding", campaign.SourceFacts.Findings[0].ID,
		"--claimant", testClaimantA, "--remote", "claimtest",
	}, &stdout, &stderr))
	var claimed ClaimResult
	require.NoError(t, json.Unmarshal(stdout.Bytes(), &claimed))
	require.Equal(t, "CLAIMED", claimed.Status, claimed.Reason)
	require.Contains(t, stderr.String(), "CLAIMED")

	stdout.Reset()
	stderr.Reset()
	require.NoError(t, run([]string{
		"release", path, "--finding", campaign.SourceFacts.Findings[0].ID,
		"--claimant", testClaimantA, "--remote", "claimtest", "--expected-claim-sha", claimed.ClaimSHA,
	}, &stdout, &stderr))
	var released ClaimResult
	require.NoError(t, json.Unmarshal(stdout.Bytes(), &released))
	require.Equal(t, "RELEASED", released.Status)
}

func TestTwoSimultaneousClaimersExactlyOneWins(t *testing.T) {
	remote, targetSHA := newClaimRemote(t)
	campaign := claimCampaignAt(targetSHA, "CONFIRMED")
	finding := campaign.SourceFacts.Findings[0]
	start := make(chan struct{})
	results := make(chan ClaimResult, 2)
	var wait sync.WaitGroup
	for _, claimant := range []string{testClaimantA, testClaimantB} {
		wait.Go(func() {
			<-start
			results <- createClaim(context.Background(), campaign, finding, claimant, defaultClaimLease,
				remote, testTarget, "", func() time.Time { return testNow })
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
	require.Equal(t, []string{"ALREADY_CLAIMED", "CLAIMED"}, statuses)
}

func TestDifferentFindingsCanBeClaimedConcurrently(t *testing.T) {
	remote, targetSHA := newClaimRemote(t)
	campaign := claimCampaignAt(targetSHA, "CONFIRMED", "CONFIRMED")
	results := make(chan ClaimResult, 2)
	var wait sync.WaitGroup
	for index, finding := range campaign.SourceFacts.Findings {
		wait.Go(func() {
			claimant := []string{testClaimantA, testClaimantB}[index]
			results <- createClaim(context.Background(), campaign, finding, claimant, defaultClaimLease,
				remote, testTarget, "", func() time.Time { return testNow })
		})
	}
	wait.Wait()
	close(results)
	for result := range results {
		require.Equal(t, "CLAIMED", result.Status)
	}
}

func TestRenewRaceRejectsTheStaleObservedSHA(t *testing.T) {
	remote, targetSHA := newClaimRemote(t)
	campaign := claimCampaignAt(targetSHA, "CONFIRMED")
	finding := campaign.SourceFacts.Findings[0]
	claimed := createClaim(context.Background(), campaign, finding, testClaimantA, defaultClaimLease,
		remote, testTarget, "", func() time.Time { return testNow })
	require.Equal(t, "CLAIMED", claimed.Status)

	start := make(chan struct{})
	results := make(chan ClaimResult, 2)
	var wait sync.WaitGroup
	for index := range 2 {
		wait.Go(func() {
			<-start
			renewedAt := testNow.Add(time.Duration(index+1) * time.Minute)
			results <- renewClaim(context.Background(), campaign, finding, testClaimantA, defaultClaimLease,
				remote, testTarget, "", claimed.ClaimSHA, func() time.Time { return renewedAt })
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
	require.Equal(t, []string{"CLAIM_CHANGED", "RENEWED"}, statuses)
}

func TestReleaseRaceDoesNotDeleteChangedClaim(t *testing.T) {
	remote, targetSHA := newClaimRemote(t)
	campaign := claimCampaignAt(targetSHA, "CONFIRMED")
	finding := campaign.SourceFacts.Findings[0]
	claimed := createClaim(context.Background(), campaign, finding, testClaimantA, defaultClaimLease,
		remote, testTarget, "", func() time.Time { return testNow })
	renewed := renewClaim(context.Background(), campaign, finding, testClaimantA, defaultClaimLease,
		remote, testTarget, "", claimed.ClaimSHA, func() time.Time { return testNow.Add(time.Hour) })
	require.Equal(t, "RENEWED", renewed.Status)

	released := releaseClaim(context.Background(), campaign, finding, testClaimantA, remote, testTarget, claimed.ClaimSHA)
	require.Equal(t, "CLAIM_CHANGED", released.Status)
	require.Equal(t, renewed.ClaimSHA, remoteRefSHA(t, remote, claimed.RemoteRef))
}

func TestExpiredClaimTakeoverUsesExactCASAndPreservesPredecessor(t *testing.T) {
	remote, targetSHA := newClaimRemote(t)
	campaign := claimCampaignAt(targetSHA, "CONFIRMED")
	finding := campaign.SourceFacts.Findings[0]
	createdAt := testNow.Add(-2 * time.Hour)
	claimed := createClaim(context.Background(), campaign, finding, testClaimantA, time.Hour,
		remote, testTarget, "work/old", func() time.Time { return createdAt })
	require.Equal(t, "CLAIMED", claimed.Status)

	taken := takeoverClaim(context.Background(), campaign, finding, testClaimantB, defaultClaimLease,
		remote, testTarget, "", claimed.ClaimSHA, func() time.Time { return testNow })
	require.Equal(t, "TAKEN_OVER", taken.Status)
	require.NotNil(t, taken.Claim.Predecessor)
	require.Equal(t, claimed.ClaimSHA, taken.Claim.Predecessor.ClaimSHA)
	require.Equal(t, testClaimantA, taken.Claim.Predecessor.Claimant)
	require.Equal(t, "work/old", taken.Claim.WorkBranch)
}

func TestLiveClaimTakeoverIsRefused(t *testing.T) {
	remote, targetSHA := newClaimRemote(t)
	campaign := claimCampaignAt(targetSHA, "CONFIRMED")
	finding := campaign.SourceFacts.Findings[0]
	claimed := createClaim(context.Background(), campaign, finding, testClaimantA, defaultClaimLease,
		remote, testTarget, "", func() time.Time { return testNow })

	taken := takeoverClaim(context.Background(), campaign, finding, testClaimantB, defaultClaimLease,
		remote, testTarget, "", claimed.ClaimSHA, func() time.Time { return testNow.Add(time.Hour) })
	require.Equal(t, "CLAIM_CONFLICT", taken.Status)
	require.Contains(t, taken.Reason, "live claims cannot be taken over")
}

func TestClaimExpiryAndOwnershipStatuses(t *testing.T) {
	remote, targetSHA := newClaimRemote(t)
	campaign := claimCampaignAt(targetSHA, "CONFIRMED")
	finding := campaign.SourceFacts.Findings[0]
	claimed := createClaim(context.Background(), campaign, finding, testClaimantA, defaultClaimLease,
		remote, testTarget, "", func() time.Time { return testNow })
	require.Equal(t, "CLAIMED", claimed.Status)

	alreadyExpired := createClaim(context.Background(), campaign, finding, testClaimantB, defaultClaimLease,
		remote, testTarget, "", func() time.Time { return testNow.Add(defaultClaimLease + claimClockSkew) })
	require.Equal(t, "CLAIM_EXPIRED", alreadyExpired.Status)
	notRenewOwner := renewClaim(context.Background(), campaign, finding, testClaimantB, defaultClaimLease,
		remote, testTarget, "", claimed.ClaimSHA, func() time.Time { return testNow.Add(time.Hour) })
	require.Equal(t, "NOT_OWNER", notRenewOwner.Status)
	notReleaseOwner := releaseClaim(context.Background(), campaign, finding, testClaimantB,
		remote, testTarget, claimed.ClaimSHA)
	require.Equal(t, "NOT_OWNER", notReleaseOwner.Status)

	inspection := inspectAgainstRemote(campaign, remote, testClaimantB, testNow.Add(time.Hour))
	require.Equal(t, "WAIT_OR_COORDINATE", inspection.Findings[0].NextAction)
}

func TestClaimRefusesAdvancedTarget(t *testing.T) {
	remote, targetSHA := newClaimRemote(t)
	campaign := claimCampaignAt(targetSHA, "CONFIRMED")
	finding := campaign.SourceFacts.Findings[0]
	advancedSHA := writeRemoteClaimContent(t, remote, []byte("advanced\n"), "")
	runGit(t, "", "--git-dir", remote, "update-ref", "refs/heads/"+testTarget, advancedSHA)

	result := createClaim(context.Background(), campaign, finding, testClaimantA, defaultClaimLease,
		remote, testTarget, "", func() time.Time { return testNow })
	require.Equal(t, "BROKEN_CAMPAIGN_BINDING", result.Status)
}

func TestRemoteUnavailableAllowsNoClaimMutation(t *testing.T) {
	campaign := claimCampaignAt(testHead, "CONFIRMED")
	finding := campaign.SourceFacts.Findings[0]
	remote := filepath.Join(t.TempDir(), "missing.git")
	claim := createClaim(context.Background(), campaign, finding, testClaimantA, defaultClaimLease,
		remote, testTarget, "", func() time.Time { return testNow })
	renew := renewClaim(context.Background(), campaign, finding, testClaimantA, defaultClaimLease,
		remote, testTarget, "", "", func() time.Time { return testNow })
	release := releaseClaim(context.Background(), campaign, finding, testClaimantA, remote, testTarget, "")
	takeover := takeoverClaim(context.Background(), campaign, finding, testClaimantB, defaultClaimLease,
		remote, testTarget, "", "", func() time.Time { return testNow })
	for _, result := range []ClaimResult{claim, renew, release, takeover} {
		require.Equal(t, "REMOTE_UNAVAILABLE", result.Status)
	}
}

func TestManuallyDeletedClaimIsNotSilentlyUnassigned(t *testing.T) {
	remote, targetSHA := newClaimRemote(t)
	campaign := claimCampaignAt(targetSHA, "CONFIRMED")
	finding := campaign.SourceFacts.Findings[0]
	claimed := createClaim(context.Background(), campaign, finding, testClaimantA, defaultClaimLease,
		remote, testTarget, "work/in-progress", func() time.Time { return testNow })
	require.Equal(t, "CLAIMED", claimed.Status)
	runGit(t, "", "--git-dir", remote, "update-ref", "refs/heads/work/in-progress", targetSHA)
	first := inspectAgainstRemote(campaign, remote, testClaimantA, testNow)
	require.Equal(t, "CLAIMED", first.Findings[0].State)
	runGit(t, "", "--git-dir", remote, "update-ref", "-d", claimed.RemoteRef)

	second := inspectAgainstRemote(campaign, remote, testClaimantA, testNow.Add(time.Minute))
	require.Equal(t, "CLAIM_HISTORY_MISSING", second.Findings[0].Claim.State)
	require.Equal(t, claimed.ClaimSHA, second.Findings[0].Claim.ObservedClaimSHA)
	require.Equal(t, "AMBIGUOUS", second.Findings[0].State)
	require.Equal(t, "REFRESH_REQUIRED", second.Findings[0].NextAction)

	third := inspectAgainstRemote(campaign, remote, testClaimantA, testNow.Add(2*time.Minute))
	require.Equal(t, "CLAIM_HISTORY_MISSING", third.Findings[0].Claim.State)
	require.Equal(t, claimed.ClaimSHA, third.Findings[0].Claim.ObservedClaimSHA)
	require.Equal(t, "AMBIGUOUS", third.Findings[0].State)
	require.Equal(t, "REFRESH_REQUIRED", third.Findings[0].NextAction)
}

func TestMalformedClaimRecordFailsClosed(t *testing.T) {
	remote, targetSHA := newClaimRemote(t)
	campaign := claimCampaignAt(targetSHA, "CONFIRMED")
	finding := campaign.SourceFacts.Findings[0]
	ref := claimRef(campaign.AuditID, finding.ID)
	malformedSHA := writeRemoteClaimContent(t, remote, []byte(`{"schemaVersion":"ai-claim/v1","claimant":"spoof"}`+"\n"), "")
	runGit(t, "", "--git-dir", remote, "update-ref", ref, malformedSHA)

	inspection := inspectAgainstRemote(campaign, remote, testClaimantA, testNow)
	require.Equal(t, "AMBIGUOUS", inspection.Findings[0].Claim.State)
	require.Equal(t, "AMBIGUOUS", inspection.Findings[0].State)
	require.Contains(t, inspection.Findings[0].Claim.Problem, "INVALID_CLAIM_RECORD")
}

func TestClaimReadValidatesEveryLineageGeneration(t *testing.T) {
	remote, targetSHA := newClaimRemote(t)
	campaign := claimCampaignAt(targetSHA, "CONFIRMED")
	finding := campaign.SourceFacts.Findings[0]
	claimed := createClaim(context.Background(), campaign, finding, testClaimantA, defaultClaimLease,
		remote, testTarget, "", func() time.Time { return testNow })
	require.Equal(t, "CLAIMED", claimed.Status)

	forgedParent := *claimed.Claim
	firstRenewedAt := testNow.Add(time.Hour)
	forgedParent.RenewedAt = &firstRenewedAt
	forgedParent.RenewalCount = 1
	forgedParent.ExpiresAt = firstRenewedAt.Add(defaultClaimLease)
	forgedParentSHA := writeRemoteClaimRecord(t, remote, forgedParent, "")

	forgedTip := forgedParent
	secondRenewedAt := testNow.Add(2 * time.Hour)
	forgedTip.RenewedAt = &secondRenewedAt
	forgedTip.RenewalCount = 2
	forgedTip.ExpiresAt = secondRenewedAt.Add(defaultClaimLease)
	forgedTipSHA := writeRemoteClaimRecord(t, remote, forgedTip, forgedParentSHA)
	runGit(t, "", "--git-dir", remote, "update-ref", claimed.RemoteRef, forgedTipSHA, claimed.ClaimSHA)

	inspection := inspectAgainstRemote(campaign, remote, testClaimantA, secondRenewedAt)
	require.Equal(t, "AMBIGUOUS", inspection.Findings[0].State)
	require.Equal(t, "AMBIGUOUS", inspection.Findings[0].Claim.State)
	require.Contains(t, inspection.Findings[0].Claim.Problem, "INVALID_CLAIM_RECORD")
}

func TestValidButForceRewrittenClaimHistoryFailsClosed(t *testing.T) {
	remote, targetSHA := newClaimRemote(t)
	campaign := claimCampaignAt(targetSHA, "CONFIRMED")
	finding := campaign.SourceFacts.Findings[0]
	claimed := createClaim(context.Background(), campaign, finding, testClaimantA, defaultClaimLease,
		remote, testTarget, "", func() time.Time { return testNow })
	require.Equal(t, "CLAIMED", claimed.Status)
	first := inspectAgainstRemote(campaign, remote, testClaimantA, testNow)
	require.Equal(t, "CLAIMED", first.Findings[0].State)
	createdAt := testNow.Add(time.Hour)
	forged := ClaimRecord{
		SchemaVersion: claimSchemaVersion, CampaignID: campaign.CampaignID, AuditID: campaign.AuditID,
		FindingID: finding.ID, FindingIdentityDigest: finding.IdentityDigest,
		SourceQualifiedDigest: campaign.SourceDigests.Qualified, AuditedSHA: campaign.AuditedSHA,
		Qualification: "CONFIRMED", Claimant: testClaimantB, CreatedAt: createdAt,
		ExpiresAt: createdAt.Add(defaultClaimLease), TargetBranch: testTarget, TargetSHA: targetSHA,
	}
	content, err := json.MarshalIndent(forged, "", "  ")
	require.NoError(t, err)
	content = append(content, '\n')
	forgedSHA := writeRemoteClaimContent(t, remote, content, "")
	runGit(t, "", "--git-dir", remote, "update-ref", claimed.RemoteRef, forgedSHA, claimed.ClaimSHA)

	inspection := inspectAgainstRemote(campaign, remote, testClaimantA, createdAt)
	require.Equal(t, "AMBIGUOUS", inspection.Findings[0].State)
	require.Equal(t, "CLAIM_HISTORY_REWRITTEN", inspection.Findings[0].Claim.Problem)
}

func TestSameFindingIDDifferentQualifiedDigestConflicts(t *testing.T) {
	remote, targetSHA := newClaimRemote(t)
	first := claimCampaignAt(targetSHA, "CONFIRMED")
	finding := first.SourceFacts.Findings[0]
	claimed := createClaim(context.Background(), first, finding, testClaimantA, defaultClaimLease,
		remote, testTarget, "", func() time.Time { return testNow })
	require.Equal(t, "CLAIMED", claimed.Status)
	second := cloneCampaignWithQualifiedDigest(first, "sha256:"+strings.Repeat("c", 64))

	conflict := createClaim(context.Background(), second, second.SourceFacts.Findings[0], testClaimantB,
		defaultClaimLease, remote, testTarget, "", func() time.Time { return testNow.Add(time.Minute) })
	require.Equal(t, "CLAIM_CONFLICT", conflict.Status)
	require.Contains(t, conflict.Reason, "identity does not match")
}

func TestClaimBoundToDifferentTargetSHAFailsClosed(t *testing.T) {
	remote, targetSHA := newClaimRemote(t)
	campaign := claimCampaignAt(targetSHA, "CONFIRMED")
	finding := campaign.SourceFacts.Findings[0]
	forged := ClaimRecord{
		SchemaVersion: claimSchemaVersion, CampaignID: campaign.CampaignID, AuditID: campaign.AuditID,
		FindingID: finding.ID, FindingIdentityDigest: finding.IdentityDigest,
		SourceQualifiedDigest: campaign.SourceDigests.Qualified, AuditedSHA: campaign.AuditedSHA,
		Qualification: "CONFIRMED", Claimant: testClaimantA, CreatedAt: testNow,
		ExpiresAt: testNow.Add(defaultClaimLease), TargetBranch: testTarget,
		TargetSHA: strings.Repeat("e", 40),
	}
	ref := claimRef(campaign.AuditID, finding.ID)
	forgedSHA := writeRemoteClaimRecord(t, remote, forged, "")
	runGit(t, "", "--git-dir", remote, "update-ref", ref, forgedSHA)

	claimed := createClaim(context.Background(), campaign, finding, testClaimantB, defaultClaimLease,
		remote, testTarget, "", func() time.Time { return testNow })
	renewed := renewClaim(context.Background(), campaign, finding, testClaimantA, defaultClaimLease,
		remote, testTarget, "", forgedSHA, func() time.Time { return testNow.Add(time.Hour) })
	released := releaseClaim(context.Background(), campaign, finding, testClaimantA, remote, testTarget, forgedSHA)
	taken := takeoverClaim(context.Background(), campaign, finding, testClaimantB, defaultClaimLease,
		remote, testTarget, "", forgedSHA, func() time.Time { return testNow.Add(defaultClaimLease + claimClockSkew) })
	require.Equal(t, "CLAIM_CONFLICT", claimed.Status)
	require.Equal(t, "CLAIM_CONFLICT", renewed.Status)
	require.Equal(t, "CLAIM_CHANGED", released.Status)
	require.Equal(t, "CLAIM_CONFLICT", taken.Status)

	inspection := inspectAgainstRemote(campaign, remote, testClaimantA, testNow)
	require.Equal(t, "AMBIGUOUS", inspection.Findings[0].State)
	require.Equal(t, "CLAIM_IDENTITY_CONFLICT", inspection.Findings[0].Claim.Problem)
}

func TestClaimBoundToDifferentTargetBranchFailsClosed(t *testing.T) {
	remote, targetSHA := newClaimRemote(t)
	campaign := claimCampaignAt(targetSHA, "CONFIRMED")
	finding := campaign.SourceFacts.Findings[0]
	const otherTarget = "release/other"
	runGit(t, "", "--git-dir", remote, "update-ref", "refs/heads/"+otherTarget, targetSHA)
	claimed := createClaim(context.Background(), campaign, finding, testClaimantA, defaultClaimLease,
		remote, testTarget, "", func() time.Time { return testNow })
	require.Equal(t, "CLAIMED", claimed.Status)

	otherClaim := createClaim(context.Background(), campaign, finding, testClaimantB, defaultClaimLease,
		remote, otherTarget, "", func() time.Time { return testNow })
	otherRenew := renewClaim(context.Background(), campaign, finding, testClaimantA, defaultClaimLease,
		remote, otherTarget, "", claimed.ClaimSHA, func() time.Time { return testNow.Add(time.Hour) })
	otherRelease := releaseClaim(context.Background(), campaign, finding, testClaimantA,
		remote, otherTarget, claimed.ClaimSHA)
	otherTakeover := takeoverClaim(context.Background(), campaign, finding, testClaimantB, defaultClaimLease,
		remote, otherTarget, "", claimed.ClaimSHA, func() time.Time { return testNow.Add(time.Hour) })
	require.Equal(t, "CLAIM_CONFLICT", otherClaim.Status)
	require.Equal(t, "CLAIM_CONFLICT", otherRenew.Status)
	require.Equal(t, "CLAIM_CHANGED", otherRelease.Status)
	require.Equal(t, "CLAIM_CONFLICT", otherTakeover.Status)

	inspection := inspectAgainstRemoteTarget(campaign, remote, otherTarget, testClaimantA, testNow)
	require.Equal(t, "AMBIGUOUS", inspection.Findings[0].State)
	require.Equal(t, "CLAIM_IDENTITY_CONFLICT", inspection.Findings[0].Claim.Problem)
}

func TestNonConfirmedAndRejectedFindingsCannotBeClaimed(t *testing.T) {
	remote, targetSHA := newClaimRemote(t)
	campaign := claimCampaignAt(targetSHA, "LIKELY", "QUESTION", "REJECTED")
	for _, finding := range campaign.SourceFacts.Findings {
		result := createClaim(context.Background(), campaign, finding, testClaimantA, defaultClaimLease,
			remote, testTarget, "", func() time.Time { return testNow })
		require.Equal(t, "FINDING_NOT_CLAIMABLE", result.Status)
		require.Empty(t, remoteRefSHA(t, remote, claimRef(campaign.AuditID, finding.ID)))
	}
}

func TestRenewCanBindWorkBranchAndInspectDetectsItsDeletion(t *testing.T) {
	remote, targetSHA := newClaimRemote(t)
	campaign := claimCampaignAt(targetSHA, "CONFIRMED")
	finding := campaign.SourceFacts.Findings[0]
	claimed := createClaim(context.Background(), campaign, finding, testClaimantA, defaultClaimLease,
		remote, testTarget, "", func() time.Time { return testNow })
	runGit(t, "", "--git-dir", remote, "update-ref", "refs/heads/work/finding", targetSHA)
	renewed := renewClaim(context.Background(), campaign, finding, testClaimantA, defaultClaimLease,
		remote, testTarget, "work/finding", claimed.ClaimSHA, func() time.Time { return testNow.Add(time.Hour) })
	require.Equal(t, "RENEWED", renewed.Status)
	inspection := inspectAgainstRemote(campaign, remote, testClaimantA, testNow.Add(time.Hour))
	require.Equal(t, "CLAIMED", inspection.Findings[0].State)
	require.Equal(t, "DISPATCH", inspection.Findings[0].NextAction)
	runGit(t, "", "--git-dir", remote, "update-ref", "-d", "refs/heads/work/finding")

	inspection = inspectAgainstRemote(campaign, remote, testClaimantA, testNow.Add(time.Hour))
	require.Equal(t, "BROKEN_BINDING", inspection.Findings[0].Claim.State)
	require.Equal(t, "REPAIR_BINDING", inspection.Findings[0].NextAction)
}

func TestClaimProviderUnavailableFailsClosed(t *testing.T) {
	campaign := claimCampaignAt(testHead, "CONFIRMED")
	missing := filepath.Join(t.TempDir(), "missing.git")
	inspection := inspectAgainstRemote(campaign, missing, testClaimantA, testNow)
	require.Equal(t, "UNKNOWN", inspection.Findings[0].Claim.State)
	require.Equal(t, "REFRESH_REQUIRED", inspection.Findings[0].NextAction)
	require.NotEqual(t, "CONFIRMED_UNASSIGNED", inspection.Findings[0].State)
}

func TestLiveInspectSupportsZeroFindingCampaign(t *testing.T) {
	remote, targetSHA := newClaimRemote(t)
	campaign := claimCampaignAt(targetSHA)
	inspection := inspectAgainstRemote(campaign, remote, testClaimantA, testNow)
	require.Equal(t, "FRESH", inspection.Freshness)
	require.Empty(t, inspection.Findings)
}

func newClaimRemote(t *testing.T) (string, string) {
	t.Helper()
	remote := filepath.Join(t.TempDir(), "remote.git")
	runGit(t, "", "init", "--bare", "--quiet", remote)
	seed := filepath.Join(t.TempDir(), "seed")
	runGit(t, "", "init", "--quiet", seed)
	require.NoError(t, os.WriteFile(filepath.Join(seed, "README"), []byte("seed\n"), 0o600))
	runGit(t, seed, "add", "README")
	runGit(t, seed, "-c", "user.name=Test", "-c", "user.email=test@example.invalid", "commit", "--quiet", "-m", "seed")
	targetSHA := strings.TrimSpace(runGit(t, seed, "rev-parse", "HEAD"))
	runGit(t, seed, "push", "--quiet", remote, targetSHA+":refs/heads/"+testTarget)

	return remote, targetSHA
}

func claimCampaignAt(targetSHA string, qualifications ...string) *Campaign {
	campaign := testCampaign(qualifications...)
	campaign.AuditedSHA = targetSHA
	seen := map[string]int{}
	for index := range campaign.SourceFacts.Findings {
		id := campaign.SourceFacts.Findings[index].ID
		seen[id]++
		if seen[id] > 1 {
			campaign.SourceFacts.Findings[index].ID += fmt.Sprintf("-%d", seen[id])
		}
	}
	for index := range campaign.SourceFacts.Findings {
		campaign.SourceFacts.Findings[index].IdentityDigest = sourceFindingDigest(campaign, campaign.SourceFacts.Findings[index])
	}
	campaign.SourceFacts.IdentityDigest = sourceFactsDigest(campaign)
	campaign.CampaignID = campaignID(campaign)

	return campaign
}

func cloneCampaignWithQualifiedDigest(source *Campaign, digest string) *Campaign {
	content, err := json.Marshal(source)
	if err != nil {
		panic(err)
	}
	var clone Campaign
	if err := json.Unmarshal(content, &clone); err != nil {
		panic(err)
	}
	clone.Observations = nil
	clone.SourceDigests.Qualified = digest
	for index := range clone.SourceFacts.Findings {
		clone.SourceFacts.Findings[index].IdentityDigest = sourceFindingDigest(&clone, clone.SourceFacts.Findings[index])
	}
	clone.SourceFacts.IdentityDigest = sourceFactsDigest(&clone)
	clone.CampaignID = campaignID(&clone)

	return &clone
}

func inspectAgainstRemote(campaign *Campaign, remote, claimant string, now time.Time) *Inspection {
	return inspectAgainstRemoteTarget(campaign, remote, testTarget, claimant, now)
}

func inspectAgainstRemoteTarget(
	campaign *Campaign,
	remote string,
	target string,
	claimant string,
	now time.Time,
) *Inspection {
	return (inspector{
		now: func() time.Time { return now }, jira: fakeJiraProvider{}, github: fakeGitHubProvider{},
		git: commandGitProvider{}, claims: commandClaimProvider{},
	}).run(context.Background(), campaign, inspectOptions{
		repository: "formancehq/ledger", jiraProject: "EN", remote: remote, target: target, claimant: claimant,
	})
}

func remoteRefSHA(t *testing.T, remote, ref string) string {
	t.Helper()
	command := testenv.Command(t, "git", "ls-remote", "--heads", remote, ref)
	output, err := command.Output()
	require.NoError(t, err)
	fields := strings.Fields(string(output))
	if len(fields) == 0 {
		return ""
	}
	require.Len(t, fields, 2)

	return fields[0]
}

func writeRemoteClaimContent(t *testing.T, remote string, content []byte, parent string) string {
	t.Helper()
	blob := strings.TrimSpace(runGitInput(t, content, "", "--git-dir", remote, "hash-object", "-w", "--stdin"))
	tree := strings.TrimSpace(runGitInput(t, []byte("100644 blob "+blob+"\tclaim.json\n"), "",
		"--git-dir", remote, "mktree"))
	arguments := []string{"--git-dir", remote, "commit-tree", tree, "-m", "tampered claim"}
	if parent != "" {
		arguments = append(arguments, "-p", parent)
	}

	return strings.TrimSpace(runGitInput(t, nil, "", arguments...))
}

func writeRemoteClaimRecord(t *testing.T, remote string, record ClaimRecord, parent string) string {
	t.Helper()
	content, err := json.MarshalIndent(record, "", "  ")
	require.NoError(t, err)

	return writeRemoteClaimContent(t, remote, append(content, '\n'), parent)
}

func runGit(t *testing.T, directory string, arguments ...string) string {
	t.Helper()

	return runGitInput(t, nil, directory, arguments...)
}

func runGitInput(t *testing.T, input []byte, directory string, arguments ...string) string {
	t.Helper()
	command := testenv.Command(t, "git", arguments...)
	command.Dir = directory
	command.Env = testenv.Environment(
		"GIT_AUTHOR_NAME=Test", "GIT_AUTHOR_EMAIL=test@example.invalid",
		"GIT_COMMITTER_NAME=Test", "GIT_COMMITTER_EMAIL=test@example.invalid",
	)
	command.Stdin = bytes.NewReader(input)
	output, err := command.CombinedOutput()
	require.NoError(t, err, "%s", output)

	return string(output)
}
