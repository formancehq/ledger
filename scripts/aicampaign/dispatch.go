package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"time"
)

type dispatchOptions struct {
	repository         string
	remote             string
	target             string
	claimant           string
	workflow           string
	repoRoot           string
	campaignPath       string
	worktreePath       string
	workItemPath       string
	beforeClaimBinding func()
	afterClaimBinding  func()
	writeWorkItem      func(string, *CampaignWork) error
	findWorkPRs        func(context.Context, string, string) ([]PRMatch, error)
}

type dispatchEvidence struct {
	Audit     auditFinding
	Qualified challengeResult
}

type DispatchResult struct {
	SchemaVersion string        `json:"schemaVersion"`
	Status        string        `json:"status"`
	CampaignID    string        `json:"campaignId"`
	FindingID     string        `json:"findingId"`
	Workflow      string        `json:"workflowClassification"`
	Reason        string        `json:"reason"`
	NextAction    string        `json:"nextAction"`
	ExistingPR    *PRMatch      `json:"existingPr"`
	Resources     WorkResources `json:"resources"`
	WorkItem      *CampaignWork `json:"workItem"`
}

type WorkResources struct {
	WorkIdentity     string `json:"workIdentity"`
	WorkBranch       string `json:"workBranch"`
	WorktreePath     string `json:"worktreePath"`
	WorkItemPath     string `json:"workItemPath"`
	TargetBaseSHA    string `json:"targetBaseSha"`
	InitialWorkSHA   string `json:"initialWorkSha"`
	ClaimRef         string `json:"claimRef"`
	ObservedClaimSHA string `json:"observedClaimSha"`
	BoundClaimSHA    string `json:"boundClaimSha"`
	BranchState      string `json:"branchState"`
	WorktreeState    string `json:"worktreeState"`
}

type CampaignWork struct {
	SchemaVersion          string             `json:"schemaVersion"`
	Status                 string             `json:"status"`
	CreatedAt              time.Time          `json:"createdAt"`
	WorkflowClassification string             `json:"workflowClassification"`
	RemoteIdentity         WorkRemoteIdentity `json:"remoteIdentity"`
	Finding                WorkFinding        `json:"finding"`
	LocalState             WorkLocalState     `json:"localState"`
	RequiredGates          []string           `json:"requiredGates"`
	CanonicalNextAction    string             `json:"canonicalNextAction"`
	EngineeringTask        string             `json:"engineeringTask"`
}

type WorkRemoteIdentity struct {
	Repository            string    `json:"repository"`
	CampaignID            string    `json:"campaignId"`
	AuditID               string    `json:"auditId"`
	FindingID             string    `json:"findingId"`
	WorkIdentity          string    `json:"workIdentity"`
	FindingIdentityDigest string    `json:"findingIdentityDigest"`
	SourceAuditDigest     string    `json:"sourceAuditDigest"`
	QualifiedDigest       string    `json:"qualifiedDigest"`
	SourceAuditSHA        string    `json:"sourceAuditSha"`
	JiraKey               string    `json:"jiraKey"`
	JiraURL               string    `json:"jiraUrl"`
	ClaimRef              string    `json:"claimRef"`
	ClaimSHA              string    `json:"claimSha"`
	Claimant              string    `json:"claimant"`
	LeaseExpiry           time.Time `json:"leaseExpiry"`
	TargetBranch          string    `json:"targetBranch"`
	TargetBaseSHA         string    `json:"targetBaseSha"`
	WorkBranch            string    `json:"workBranch"`
	InitialWorkSHA        string    `json:"initialWorkSha"`
	PRMarkers             []string  `json:"prMarkers"`
}

type WorkFinding struct {
	Qualification          string   `json:"qualification"`
	Severity               string   `json:"severity"`
	Title                  string   `json:"title"`
	Invariant              string   `json:"invariant"`
	ChallengeSummary       string   `json:"challengeSummary"`
	AuditFindingReference  string   `json:"auditFindingReference"`
	QualificationReference string   `json:"qualificationReference"`
	EvidenceFor            []string `json:"evidenceFor"`
	EvidenceAgainst        []string `json:"evidenceAgainst"`
	ReproductionPlan       string   `json:"reproductionPlan"`
}

type WorkLocalState struct {
	WorktreePath string `json:"worktreePath"`
	WorkItemPath string `json:"workItemPath"`
}

func loadDispatchEvidence(auditPath, qualifiedPath string, campaign *Campaign, findingID string) (dispatchEvidence, error) {
	var source auditReport
	auditContent, err := readStrictJSON(auditPath, &source)
	if err != nil {
		return dispatchEvidence{}, fmt.Errorf("invalid source audit result: %w", err)
	}
	var qualified challengeReport
	qualifiedContent, err := readStrictJSON(qualifiedPath, &qualified)
	if err != nil {
		return dispatchEvidence{}, fmt.Errorf("invalid qualified result: %w", err)
	}
	if err := validateImportReports(&source, &qualified, auditContent); err != nil {
		return dispatchEvidence{}, err
	}
	if source.AuditID != campaign.AuditID || source.Head != campaign.AuditedSHA ||
		digestBytes(auditContent) != campaign.SourceDigests.Audit ||
		digestBytes(qualifiedContent) != campaign.SourceDigests.Qualified {
		return dispatchEvidence{}, errors.New("dispatch evidence does not match campaign provenance")
	}
	var evidence dispatchEvidence
	for _, finding := range source.Findings {
		if finding.ID == findingID {
			evidence.Audit = finding
		}
	}
	for _, result := range qualified.Results {
		if result.ID == findingID {
			evidence.Qualified = result
		}
	}
	if evidence.Audit.ID == "" || evidence.Qualified.ID == "" {
		return dispatchEvidence{}, fmt.Errorf("finding %q is not present exactly once in dispatch evidence", findingID)
	}

	return evidence, nil
}

func baseDispatchResult(campaign *Campaign, finding SourceFinding, options dispatchOptions) DispatchResult {
	return DispatchResult{
		SchemaVersion: dispatchSchemaVersion,
		CampaignID:    campaign.CampaignID,
		FindingID:     finding.ID,
		Workflow:      options.workflow,
		Resources: WorkResources{
			ClaimRef:       claimRef(campaign.AuditID, finding.ID),
			TargetBaseSHA:  campaign.AuditedSHA,
			InitialWorkSHA: campaign.AuditedSHA,
		},
	}
}

func dispatchFinding(
	ctx context.Context,
	campaign *Campaign,
	finding SourceFinding,
	observed InspectionFinding,
	evidence dispatchEvidence,
	options dispatchOptions,
	now func() time.Time,
) DispatchResult {
	result := baseDispatchResult(campaign, finding, options)
	if finding.Qualification != "CONFIRMED" || !finding.Dispatchable {
		return dispatchStopped(result, "FINDING_NOT_CONFIRMED", "qualification is "+finding.Qualification, "NO_ACTION")
	}
	if observed.Freshness != "FRESH" {
		status := "STALE_CAMPAIGN"
		if observed.Freshness == "UNAVAILABLE" || campaign.Observations != nil && (campaign.Observations.Providers.Git.Status == "UNAVAILABLE" ||
			campaign.Observations.Providers.Claims.Status == "UNAVAILABLE") {
			status = "REMOTE_UNAVAILABLE"
		}

		return dispatchStopped(result, status, "dispatch requires fresh GitHub, Jira, Git, and claim observations", "REFRESH_REQUIRED")
	}
	if observed.Jira.Status == "AMBIGUOUS" || observed.PR.Status == "AMBIGUOUS" {
		return dispatchStopped(result, "AMBIGUOUS_BINDING", "multiple exact Jira or PR bindings exist", "RESOLVE_BINDING")
	}
	if len(observed.PR.Matches) == 1 {
		match := observed.PR.Matches[0]
		result.ExistingPR = &match
		if match.BaseRef != options.target {
			return dispatchStopped(result, "BROKEN_BINDING", "the existing PR targets another branch", "REPAIR_BINDING")
		}
		if match.Merged && match.BindingBasis == "AI_AUDIT_MARKER" {
			return dispatchStopped(result, "ALREADY_MERGED", "an exact-marker PR already merged this finding", "VERIFY_RESOLUTION")
		}
		if match.Merged {
			return dispatchStopped(result, "BROKEN_BINDING", "the merged Jira-only PR is not exact resolution proof", "VERIFY_RESOLUTION")
		}
		if !match.BranchExists {
			return dispatchStopped(result, "BROKEN_BINDING", "the existing PR branch or target binding is broken", "REPAIR_BINDING")
		}
		switch {
		case strings.EqualFold(match.State, "OPEN"):
			return dispatchStopped(result, "EXISTING_PR", "an exact active PR binding already exists", "CONTINUE_PR")
		case strings.EqualFold(match.State, "CLOSED"):
			return dispatchStopped(result, "EXISTING_PR", "an exact closed PR binding already exists", "REVIEW_CLOSED_PR")
		default:
			return dispatchStopped(result, "BROKEN_BINDING", "the existing PR state is unknown", "REPAIR_BINDING")
		}
	}
	if observed.ObservedTargetSHA == "" {
		return dispatchStopped(result, "REMOTE_UNAVAILABLE", "target branch could not be resolved", "REFRESH_REQUIRED")
	}
	if observed.ObservedTargetSHA != campaign.AuditedSHA {
		return dispatchStopped(result, "BASE_UPDATE_REQUIRED", "target branch advanced beyond the qualified audit SHA", "BASE_UPDATE_REQUIRED")
	}
	if options.workflow == "" || !validWorkflow(options.workflow) {
		return dispatchStopped(result, "HUMAN_DECISION_REQUIRED", "workflow classification must be selected explicitly", "HUMAN_DECISION_REQUIRED")
	}
	if observed.Jira.Status == "UNKNOWN" || observed.PR.Status == "UNKNOWN" {
		return dispatchStopped(result, "STALE_CAMPAIGN", "exact external bindings are not fresh", "REFRESH_REQUIRED")
	}
	var jira JiraIssue
	if len(observed.Jira.Issues) == 1 {
		jira = observed.Jira.Issues[0]
	}
	if options.workflow == "BUGFIX" && jira.Key == "" {
		return dispatchStopped(result, "JIRA_REQUIRED", "runtime bugfix dispatch requires the explicitly published Jira handoff", "PUBLISH_JIRA_WITH_AUTHORIZATION")
	}
	identity := workIdentity(campaign, finding, jira.Key)
	branch := workBranch(options.workflow, finding.ID, jira.Key, identity)
	result.Resources.WorkIdentity = identity
	result.Resources.WorkBranch = branch
	if stopped, stop := stopForCampaignWorkPR(ctx, result, options, identity); stop {
		return stopped
	}
	if options.worktreePath == "" {
		options.worktreePath = defaultDispatchWorktree(options.repoRoot, identity)
	}
	if options.workItemPath == "" {
		options.workItemPath = defaultWorkItemPath(options.campaignPath, identity)
	}
	validatedWorkItemPath, err := validateCampaignDestination(options.repoRoot, options.workItemPath, true)
	if err != nil {
		return dispatchStopped(result, "BROKEN_BINDING", conciseError(err), "REPAIR_BINDING")
	}
	options.workItemPath = validatedWorkItemPath
	result.Resources.WorktreePath = options.worktreePath
	result.Resources.WorkItemPath = options.workItemPath

	switch observed.Claim.State {
	case "UNCLAIMED", "NON_CLAIMABLE":
		return dispatchStopped(result, "CLAIM_REQUIRED", "dispatch requires an active remote claim", "CLAIM")
	case "CLAIM_EXPIRED":
		return dispatchStopped(result, "CLAIM_EXPIRED", "remote claim lease is expired", "REVIEW_EXPIRED_CLAIM")
	case "BROKEN_BINDING", "AMBIGUOUS", "CLAIM_HISTORY_MISSING":
		return dispatchStopped(result, "BROKEN_BINDING", "remote claim binding is not mechanically safe", "REPAIR_BINDING")
	case "UNKNOWN":
		return dispatchStopped(result, "REMOTE_UNAVAILABLE", "remote claim could not be verified", "REFRESH_REQUIRED")
	}
	if !observed.Claim.OwnedBySession || observed.Claim.Claimant != options.claimant {
		return dispatchStopped(result, "CLAIM_NOT_OWNER", "remote claim belongs to another claimant", "WAIT_OR_COORDINATE")
	}
	if observed.Claim.ExpiresAt == nil || isObservationExpired(*observed.Claim.ExpiresAt, now().UTC()) {
		return dispatchStopped(result, "CLAIM_EXPIRED", "remote claim lease is expired", "REVIEW_EXPIRED_CLAIM")
	}
	result.Resources.ObservedClaimSHA = observed.Claim.ObservedClaimSHA

	repository, err := openClaimRepositoryAt(ctx, options.remote, options.repoRoot)
	if err != nil {
		return dispatchStopped(result, "REMOTE_UNAVAILABLE", conciseError(err), "REFRESH_REQUIRED")
	}
	defer repository.close()
	refs, err := repository.listRefs(ctx, result.Resources.ClaimRef, "refs/heads/"+options.target, "refs/heads/"+branch)
	if err != nil {
		return dispatchStopped(result, "REMOTE_UNAVAILABLE", conciseError(err), "REFRESH_REQUIRED")
	}
	claimSHA := refs[result.Resources.ClaimRef]
	if claimSHA == "" {
		return dispatchStopped(result, "CLAIM_REQUIRED", "remote claim disappeared during dispatch", "CLAIM")
	}
	if claimSHA != observed.Claim.ObservedClaimSHA {
		return dispatchStopped(result, "CLAIM_CHANGED", "remote claim changed after eligibility observation", "REFRESH_REQUIRED")
	}
	claimed, err := repository.readClaim(ctx, result.Resources.ClaimRef, claimSHA)
	if err != nil {
		return dispatchStopped(result, "CLAIM_CHANGED", conciseError(err), "REFRESH_REQUIRED")
	}
	if !claimed.Record.matches(campaign, finding, options.target) {
		return dispatchStopped(result, "BROKEN_BINDING", "claim source identity does not match the campaign finding", "REPAIR_BINDING")
	}
	if claimed.Record.Claimant != options.claimant {
		return dispatchStopped(result, "CLAIM_NOT_OWNER", "remote claim belongs to another claimant", "WAIT_OR_COORDINATE")
	}
	if isExpired(claimed.Record, now().UTC()) {
		return dispatchStopped(result, "CLAIM_EXPIRED", "remote claim expired during dispatch", "REVIEW_EXPIRED_CLAIM")
	}

	if claimed.Record.DispatchedAt != nil {
		if !sameDispatchBinding(claimed.Record, identity, branch, campaign.AuditedSHA, options.workflow, jira.Key) {
			return dispatchStopped(result, "CONFLICTING_DISPATCH", "claim is bound to a different canonical work item", "REPAIR_BINDING")
		}

		return resumeDispatched(ctx, repository, result, campaign, finding, evidence, jira, claimed.Record,
			claimSHA, refs["refs/heads/"+branch], options)
	}
	if refs["refs/heads/"+options.target] != campaign.AuditedSHA {
		return dispatchStopped(result, "BASE_UPDATE_REQUIRED", "target advanced during dispatch", "BASE_UPDATE_REQUIRED")
	}
	if claimed.Record.WorkBranch != "" && claimed.Record.WorkBranch != branch {
		return dispatchStopped(result, "CONFLICTING_BRANCH", "claim carries a different work-branch binding", "REPAIR_BINDING")
	}

	branchSHA := refs["refs/heads/"+branch]
	switch {
	case branchSHA == "":
		created, createErr := repository.createBranch(ctx, "refs/heads/"+options.target, campaign.AuditedSHA, "refs/heads/"+branch)
		if createErr != nil {
			return dispatchStopped(result, created, conciseError(createErr), "REFRESH_REQUIRED")
		}
		result.Resources.BranchState = created
		branchSHA = campaign.AuditedSHA
	case branchSHA != campaign.AuditedSHA:
		result.Resources.BranchState = "CONFLICTING_BRANCH"

		return dispatchStopped(result, "CONFLICTING_BRANCH", "canonical work branch exists at a different SHA", "REPAIR_BINDING")
	default:
		result.Resources.BranchState = "EXPECTED_EXISTING_WORK"
	}

	worktreePath, worktreeState, err := ensureDispatchWorktree(options.repoRoot, options.remote, branch, branchSHA,
		options.worktreePath, false)
	if err != nil {
		result.Resources.WorktreeState = worktreeState

		return dispatchStopped(result, "WORKTREE_CREATION_FAILED", conciseError(err), "RESUME_DISPATCH")
	}
	result.Resources.WorktreePath = worktreePath
	result.Resources.WorktreeState = worktreeState
	if options.beforeClaimBinding != nil {
		options.beforeClaimBinding()
	}

	refs, err = repository.listRefs(ctx, result.Resources.ClaimRef, "refs/heads/"+options.target, "refs/heads/"+branch)
	if err != nil {
		return dispatchStopped(result, "REMOTE_UNAVAILABLE", conciseError(err), "REFRESH_REQUIRED")
	}
	if refs[result.Resources.ClaimRef] != claimSHA {
		return dispatchStopped(result, "CLAIM_CHANGED", "claim changed between worktree creation and binding", "REFRESH_REQUIRED")
	}
	if refs["refs/heads/"+options.target] != campaign.AuditedSHA {
		return dispatchStopped(result, "BASE_UPDATE_REQUIRED", "target advanced before claim binding", "BASE_UPDATE_REQUIRED")
	}
	if refs["refs/heads/"+branch] != campaign.AuditedSHA {
		return dispatchStopped(result, "CONFLICTING_BRANCH", "work branch changed before claim binding", "REPAIR_BINDING")
	}

	dispatchedAt := now().UTC()
	if isExpired(claimed.Record, dispatchedAt) {
		return dispatchStopped(result, "CLAIM_EXPIRED", "remote claim expired before binding", "REVIEW_EXPIRED_CLAIM")
	}
	if stopped, stop := stopForCampaignWorkPR(ctx, result, options, identity); stop {
		return stopped
	}
	record := claimed.Record
	record.WorkBranch = branch
	record.WorkIdentity = identity
	record.InitialWorkSHA = campaign.AuditedSHA
	record.TargetBaseSHA = campaign.AuditedSHA
	record.Workflow = options.workflow
	record.JiraKey = jira.Key
	record.DispatchedAt = &dispatchedAt
	boundSHA, err := repository.writeClaim(ctx, result.Resources.ClaimRef, claimSHA, record)
	if err != nil {
		current, refreshErr := repository.listRefs(ctx, result.Resources.ClaimRef)
		if refreshErr != nil {
			return dispatchStopped(result, "REMOTE_UNAVAILABLE", conciseError(refreshErr), "REFRESH_REQUIRED")
		}
		result.Resources.ObservedClaimSHA = current[result.Resources.ClaimRef]

		return dispatchStopped(result, "CLAIM_CHANGED", "claim binding compare-and-swap failed", "REFRESH_REQUIRED")
	}
	result.Resources.BoundClaimSHA = boundSHA
	result.Resources.ObservedClaimSHA = boundSHA
	if options.afterClaimBinding != nil {
		options.afterClaimBinding()
	}

	status := "DISPATCHED"
	nextAction := "START_ENGINEERING_AGENT"
	refs, err = repository.listRefs(ctx, "refs/heads/"+options.target, "refs/heads/"+branch)
	if err != nil {
		return dispatchStopped(result, "REMOTE_UNAVAILABLE", "claim was bound but final remote state is unavailable", "REFRESH_REQUIRED")
	}
	if refs["refs/heads/"+branch] != campaign.AuditedSHA {
		return dispatchStopped(result, "BROKEN_BINDING", "work branch changed while dispatch completed", "REPAIR_BINDING")
	}
	if refs["refs/heads/"+options.target] != campaign.AuditedSHA {
		status = "STALE_AT_DISPATCH"
		nextAction = "BASE_UPDATE_REQUIRED"
	}
	work := buildCampaignWork(status, campaign, finding, evidence, jira, record, boundSHA,
		options.repository, worktreePath, options.workItemPath)
	writer := options.writeWorkItem
	if writer == nil {
		writer = writeWorkItem
	}
	if err := writer(options.workItemPath, work); err != nil {
		return dispatchStopped(result, "WORK_ITEM_WRITE_FAILED", conciseError(err), "RESUME_DISPATCH")
	}
	result.Status = status
	result.NextAction = nextAction
	result.WorkItem = work

	return result
}

func resumeDispatched(
	ctx context.Context,
	repository *claimRepository,
	result DispatchResult,
	campaign *Campaign,
	finding SourceFinding,
	evidence dispatchEvidence,
	jira JiraIssue,
	record ClaimRecord,
	claimSHA string,
	branchSHA string,
	options dispatchOptions,
) DispatchResult {
	if branchSHA == "" {
		return dispatchStopped(result, "BROKEN_BINDING", "claim-bound work branch is missing", "REPAIR_BINDING")
	}
	if err := repository.fetchBranch(ctx, "refs/heads/"+record.WorkBranch); err != nil {
		return dispatchStopped(result, "REMOTE_UNAVAILABLE", conciseError(err), "REFRESH_REQUIRED")
	}
	if !repository.isAncestor(ctx, record.InitialWorkSHA, branchSHA) {
		return dispatchStopped(result, "BROKEN_BINDING", "work branch no longer descends from the immutable initial work SHA", "REPAIR_BINDING")
	}
	worktreePath, worktreeState, err := ensureDispatchWorktree(options.repoRoot, options.remote, record.WorkBranch,
		branchSHA, options.worktreePath, true)
	if err != nil {
		result.Resources.WorktreeState = worktreeState

		return dispatchStopped(result, "CONFLICTING_DISPATCH", conciseError(err), "REPAIR_BINDING")
	}
	result.Resources.WorktreePath = worktreePath
	result.Resources.WorktreeState = worktreeState
	result.Resources.BranchState = "EXPECTED_EXISTING_WORK"
	result.Resources.BoundClaimSHA = claimSHA
	result.Resources.ObservedClaimSHA = claimSHA
	refs, err := repository.listRefs(ctx, result.Resources.ClaimRef, "refs/heads/"+options.target,
		"refs/heads/"+record.WorkBranch)
	if err != nil {
		return dispatchStopped(result, "REMOTE_UNAVAILABLE", conciseError(err), "REFRESH_REQUIRED")
	}
	if refs[result.Resources.ClaimRef] != claimSHA {
		return dispatchStopped(result, "CLAIM_CHANGED", "claim changed while reconstructing dispatch", "REFRESH_REQUIRED")
	}
	if refs["refs/heads/"+record.WorkBranch] != branchSHA {
		return dispatchStopped(result, "BROKEN_BINDING", "work branch changed while reconstructing dispatch", "REPAIR_BINDING")
	}
	workStatus := "DISPATCHED"
	resultStatus := "ALREADY_DISPATCHED"
	nextAction := "START_ENGINEERING_AGENT"
	if refs["refs/heads/"+options.target] != record.TargetBaseSHA {
		workStatus = "STALE_AT_DISPATCH"
		nextAction = "BASE_UPDATE_REQUIRED"
	}
	work := buildCampaignWork(workStatus, campaign, finding, evidence, jira, record, claimSHA,
		options.repository, worktreePath, options.workItemPath)
	writer := options.writeWorkItem
	if writer == nil {
		writer = writeWorkItem
	}
	if err := writer(options.workItemPath, work); err != nil {
		return dispatchStopped(result, "WORK_ITEM_WRITE_FAILED", conciseError(err), "RESUME_DISPATCH")
	}
	result.Status = resultStatus
	result.NextAction = nextAction
	result.WorkItem = work

	return result
}

func dispatchStopped(result DispatchResult, status, reason, nextAction string) DispatchResult {
	result.Status = status
	result.Reason = reason
	result.NextAction = nextAction

	return result
}

func stopForCampaignWorkPR(
	ctx context.Context,
	result DispatchResult,
	options dispatchOptions,
	identity string,
) (DispatchResult, bool) {
	if options.findWorkPRs == nil {
		return result, false
	}
	matches, err := options.findWorkPRs(ctx, options.repository, "AI-CAMPAIGN-WORK:"+identity)
	if err != nil {
		return dispatchStopped(result, "REMOTE_UNAVAILABLE", conciseError(err), "REFRESH_REQUIRED"), true
	}
	if len(matches) > 1 {
		return dispatchStopped(result, "AMBIGUOUS_BINDING", "multiple exact campaign-work PR bindings exist", "RESOLVE_BINDING"), true
	}
	if len(matches) == 0 {
		return result, false
	}
	match := matches[0]
	result.ExistingPR = &match
	if match.BaseRef != options.target {
		return dispatchStopped(result, "BROKEN_BINDING", "the campaign-work PR targets another branch", "REPAIR_BINDING"), true
	}
	if match.Merged {
		return dispatchStopped(result, "ALREADY_MERGED", "an exact campaign-work PR already merged this work identity", "VERIFY_RESOLUTION"), true
	}
	switch {
	case strings.EqualFold(match.State, "OPEN"):
		return dispatchStopped(result, "EXISTING_PR", "an exact campaign-work PR binding already exists", "CONTINUE_PR"), true
	case strings.EqualFold(match.State, "CLOSED"):
		return dispatchStopped(result, "EXISTING_PR", "an exact closed campaign-work PR binding already exists", "REVIEW_CLOSED_PR"), true
	default:
		return dispatchStopped(result, "BROKEN_BINDING", "the campaign-work PR state is unknown", "REPAIR_BINDING"), true
	}
}

func workIdentity(campaign *Campaign, finding SourceFinding, jiraKey string) string {
	payload := strings.Join([]string{
		workSchemaVersion,
		campaign.CampaignID,
		finding.ID,
		campaign.SourceDigests.Qualified,
		jiraKey,
	}, "\x00")
	sum := sha256.Sum256([]byte(payload))

	return "work-" + hex.EncodeToString(sum[:])
}

func workBranch(workflow, findingID, jiraKey, identity string) string {
	prefix := map[string]string{
		"BUGFIX":        "fix/",
		"TEST_GAP":      "test/",
		"TOOLING_FIX":   "fix/",
		"DOCUMENTATION": "docs/",
	}[workflow]
	slug := strings.TrimPrefix(findingID, strings.SplitN(findingID, "/", 2)[0]+"/")
	if jiraKey != "" {
		slug = strings.ToLower(jiraKey) + "-" + slug
	}
	if workflow == "TOOLING_FIX" {
		slug = "tooling-" + slug
	}

	return prefix + slug + "-" + strings.TrimPrefix(identity, "work-")[:12]
}

func sameDispatchBinding(record ClaimRecord, identity, branch, baseSHA, workflow, jiraKey string) bool {
	return record.WorkIdentity == identity && record.WorkBranch == branch && record.InitialWorkSHA == baseSHA &&
		record.TargetBaseSHA == baseSHA && record.Workflow == workflow && record.JiraKey == jiraKey
}

func isObservationExpired(expiresAt, now time.Time) bool {
	return !now.Before(expiresAt.Add(claimClockSkew))
}

func defaultDispatchWorktree(repoRoot, identity string) string {
	common := gitOutput(repoRoot, "rev-parse", "--path-format=absolute", "--git-common-dir")
	mainRoot := repoRoot
	if common != "" && filepath.Base(common) == ".git" {
		mainRoot = filepath.Dir(common)
	}

	return filepath.Join(filepath.Dir(mainRoot), "."+filepath.Base(mainRoot)+"-ai-worktrees",
		"campaign-"+strings.TrimPrefix(identity, "work-")[:16], "worktree")
}

func defaultWorkItemPath(campaignPath, identity string) string {
	return filepath.Join(filepath.Dir(campaignPath), "work-items", identity+".json")
}

func buildCampaignWork(
	status string,
	campaign *Campaign,
	finding SourceFinding,
	evidence dispatchEvidence,
	jira JiraIssue,
	record ClaimRecord,
	claimSHA string,
	repository string,
	worktreePath string,
	workItemPath string,
) *CampaignWork {
	markers := []string{"AI-AUDIT:" + finding.ID, "AI-CAMPAIGN-WORK:" + record.WorkIdentity}
	if jira.Key != "" {
		markers = append(markers, "Jira: "+jira.Key)
	}
	work := &CampaignWork{
		SchemaVersion:          workSchemaVersion,
		Status:                 status,
		CreatedAt:              *record.DispatchedAt,
		WorkflowClassification: record.Workflow,
		RemoteIdentity: WorkRemoteIdentity{
			Repository: repository, CampaignID: campaign.CampaignID, AuditID: campaign.AuditID,
			FindingID: finding.ID, WorkIdentity: record.WorkIdentity,
			FindingIdentityDigest: finding.IdentityDigest, SourceAuditDigest: campaign.SourceDigests.Audit,
			QualifiedDigest: campaign.SourceDigests.Qualified, SourceAuditSHA: campaign.AuditedSHA,
			JiraKey: jira.Key, JiraURL: jira.URL, ClaimRef: claimRef(campaign.AuditID, finding.ID),
			ClaimSHA: claimSHA, Claimant: record.Claimant, LeaseExpiry: record.ExpiresAt,
			TargetBranch: record.TargetBranch, TargetBaseSHA: record.TargetBaseSHA,
			WorkBranch: record.WorkBranch, InitialWorkSHA: record.InitialWorkSHA, PRMarkers: markers,
		},
		Finding: WorkFinding{
			Qualification: finding.Qualification, Severity: finding.Severity, Title: finding.Title,
			Invariant: evidence.Audit.ViolatedInvariant, ChallengeSummary: evidence.Qualified.ChallengeSummary,
			AuditFindingReference:  finding.References.AuditFinding,
			QualificationReference: finding.References.QualificationResult,
			EvidenceFor:            nonNilStrings(evidence.Qualified.EvidenceFor),
			EvidenceAgainst:        nonNilStrings(evidence.Qualified.EvidenceAgainst),
			ReproductionPlan:       evidence.Qualified.ReproductionPlan,
		},
		LocalState:          WorkLocalState{WorktreePath: worktreePath, WorkItemPath: workItemPath},
		RequiredGates:       requiredGates(record.Workflow),
		CanonicalNextAction: "START_ENGINEERING_AGENT",
	}
	if status == "STALE_AT_DISPATCH" {
		work.CanonicalNextAction = "BASE_UPDATE_REQUIRED"
	}
	work.EngineeringTask = engineeringTask(work)

	return work
}

func requiredGates(workflow string) []string {
	common := []string{"SEARCH_AND_RECONCILE_EXISTING_WORK", "EFFECTIVE_NIX_TOOLCHAIN", "NORMALIZE_TO_FIXPOINT", "EXACT_REVIEW", "GUARDED_PUBLICATION"}
	switch workflow {
	case "BUGFIX":
		return append([]string{"DISCOVERY", "BEFORE_FIX", "AFTER_FIX", "REGRESSION_SENSITIVITY"}, common...)
	case "TEST_GAP":
		return append([]string{"TEST_GAP_EVIDENCE", "REGRESSION_SENSITIVITY"}, common...)
	case "TOOLING_FIX":
		return append([]string{"TOOLING_FAILURE_BEFORE_FIX", "TOOLING_AFTER_FIX", "REGRESSION_SENSITIVITY"}, common...)
	case "DOCUMENTATION":
		return append([]string{"DOCUMENTATION_VALIDATION"}, common...)
	default:
		return common
	}
}

func engineeringTask(work *CampaignWork) string {
	remote := work.RemoteIdentity
	finding := work.Finding
	jira := "none (optional for this workflow)"
	if remote.JiraKey != "" {
		jira = remote.JiraKey
	}

	return fmt.Sprintf(`Work only in the bound candidate worktree below. This task is orchestration context, not proof of a defect or permission to merge.

Repository: %s
Target branch: %s
Exact dispatch base SHA: %s
Finding: %s
Jira: %s
Severity: %s
Qualification: %s
Workflow: %s
Invariant: %s
Audit provenance: %s at %s
Qualified provenance: %s
Work identity: %s
Work branch: %s
Worktree: %s
Claim: %s at %s, claimant %s, lease expiry %s
Required future PR body lines:
%s

Engineering sequence:
1. Search and reconcile existing work; record the repository discovery classification.
2. Produce the workflow-appropriate BEFORE_FIX evidence before changing production or test behavior. Runtime BUGFIX work must use the native bugfix evidence contract; TEST_GAP and TOOLING_FIX work must use their own evidence semantics without fabricating a runtime reproduction.
3. Preserve the evidence and determine the root cause; do not treat the qualified finding as an implementation prescription.
4. Implement the minimum justified fix.
5. Add regression coverage and challenge its sensitivity with a meaningful temporary mutation, then restore the correct implementation.
6. Validate using the effective pinned Nix toolchain and hermetic writable caches outside trusted/candidate worktrees.
7. Run normalization to a fixpoint and keep the worktree clean.
8. Obtain an exact review of the final candidate SHA.
9. Use guarded publication with last-mile target and remote-head revalidation. Do not merge automatically.

The work branch started at the immutable dispatch base. If the target has advanced, stop with BASE_UPDATE_REQUIRED; never silently move this branch. Invoke orchestration/review policy from a clean base-pinned trusted-tool worktree, not from candidate-modified tooling.
`, remote.Repository, remote.TargetBranch, remote.TargetBaseSHA, remote.FindingID, jira, finding.Severity,
		finding.Qualification, work.WorkflowClassification, finding.Invariant, remote.SourceAuditDigest,
		remote.SourceAuditSHA, remote.QualifiedDigest, remote.WorkIdentity, remote.WorkBranch,
		work.LocalState.WorktreePath, remote.ClaimRef, remote.ClaimSHA, remote.Claimant,
		remote.LeaseExpiry.Format(time.RFC3339Nano), strings.Join(remote.PRMarkers, "\n"))
}

func writeWorkItem(path string, work *CampaignWork) error {
	if err := validateCampaignWork(work); err != nil {
		return fmt.Errorf("refusing malformed work item: %w", err)
	}
	content, err := json.MarshalIndent(work, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal work item: %w", err)
	}

	return atomicWrite(path, append(content, '\n'))
}

func validateCampaignWork(work *CampaignWork) error {
	if work == nil || work.SchemaVersion != workSchemaVersion || !workIdentityPattern.MatchString(work.RemoteIdentity.WorkIdentity) ||
		!validWorkflow(work.WorkflowClassification) || work.CreatedAt.IsZero() ||
		work.RemoteIdentity.CampaignID == "" || work.RemoteIdentity.FindingID == "" ||
		work.RemoteIdentity.ClaimRef == "" || !shaPattern.MatchString(work.RemoteIdentity.ClaimSHA) ||
		!shaPattern.MatchString(work.RemoteIdentity.TargetBaseSHA) || !shaPattern.MatchString(work.RemoteIdentity.InitialWorkSHA) ||
		work.RemoteIdentity.WorkBranch == "" || work.LocalState.WorktreePath == "" || work.LocalState.WorkItemPath == "" ||
		work.Finding.Qualification != "CONFIRMED" || work.Finding.Invariant == "" || work.EngineeringTask == "" {
		return errors.New("incomplete canonical work item")
	}
	if len(work.RemoteIdentity.PRMarkers) < 2 || !slices.Equal(work.RemoteIdentity.PRMarkers[:2], []string{
		"AI-AUDIT:" + work.RemoteIdentity.FindingID,
		"AI-CAMPAIGN-WORK:" + work.RemoteIdentity.WorkIdentity,
	}) {
		return errors.New("invalid PR marker contract")
	}

	return nil
}

func ensureDispatchWorktree(
	repoRoot string,
	remote string,
	branch string,
	expectedSHA string,
	requestedPath string,
	allowLocalAhead bool,
) (string, string, error) {
	if !shaPattern.MatchString(expectedSHA) || execCheckBranch(branch) != nil || strings.HasPrefix(branch, "ai-claims/") {
		return requestedPath, "INVALID", errors.New("invalid worktree branch binding")
	}
	canonicalRoot, err := filepath.EvalSymlinks(repoRoot)
	if err != nil {
		return requestedPath, "INVALID", fmt.Errorf("resolve repository root: %w", err)
	}
	absolutePath, err := filepath.Abs(requestedPath)
	if err != nil {
		return requestedPath, "INVALID", fmt.Errorf("resolve worktree path: %w", err)
	}
	absolutePath, err = resolvePhysicalCandidate(absolutePath)
	if err != nil {
		return requestedPath, "INVALID", err
	}
	if pathWithin(absolutePath, canonicalRoot) {
		return absolutePath, "FORBIDDEN", errors.New("candidate worktree must be outside the trusted checkout")
	}
	entries, err := listWorktrees(repoRoot)
	if err != nil {
		return absolutePath, "UNKNOWN", err
	}
	for _, entry := range entries {
		if entry.Branch == "refs/heads/"+branch {
			physicalEntry, physicalErr := filepath.EvalSymlinks(entry.Path)
			if physicalErr != nil {
				return absolutePath, "UNKNOWN", fmt.Errorf("resolve discovered worktree: %w", physicalErr)
			}
			if pathWithin(physicalEntry, canonicalRoot) || looksLikeValidationPath(physicalEntry) {
				return physicalEntry, "FORBIDDEN", errors.New("discovered worktree violates the isolation boundary")
			}
			localAhead := allowLocalAhead && gitExitSuccess(repoRoot, "merge-base", "--is-ancestor", expectedSHA, entry.Head)
			if entry.Head != expectedSHA && !localAhead {
				return absolutePath, "CONFLICT", errors.New("existing worktree HEAD does not match remote work branch")
			}
			if !allowLocalAhead {
				clean, cleanErr := worktreeIsClean(entry.Path)
				if cleanErr != nil {
					return absolutePath, "UNKNOWN", cleanErr
				}
				if !clean {
					return entry.Path, "CONFLICT", errors.New("existing initial-dispatch worktree is dirty")
				}
			}

			return entry.Path, "DISCOVERED", nil
		}
	}
	for _, entry := range entries {
		if samePath(entry.Path, absolutePath) || pathWithin(absolutePath, entry.Path) || pathWithin(entry.Path, absolutePath) {
			return absolutePath, "CONFLICT", errors.New("candidate path overlaps another registered worktree")
		}
	}
	if looksLikeValidationPath(absolutePath) {
		return absolutePath, "FORBIDDEN", errors.New("candidate worktree cannot overlap a validation run directory")
	}
	if output := gitOutput(repoRoot, "fetch", "--quiet", "--no-tags", remote,
		"+refs/heads/"+branch+":refs/remotes/"+remote+"/"+branch); output == "" {
		if gitOutput(repoRoot, "rev-parse", "--verify", "refs/remotes/"+remote+"/"+branch) != expectedSHA {
			return absolutePath, "REMOTE_UNAVAILABLE", errors.New("fetch work branch failed")
		}
	}
	localSHA := gitOutput(repoRoot, "show-ref", "--verify", "--hash", "refs/heads/"+branch)
	if localSHA != "" && localSHA != expectedSHA {
		switch {
		case allowLocalAhead && gitExitSuccess(repoRoot, "merge-base", "--is-ancestor", expectedSHA, localSHA):
			// Preserve useful unpushed local engineering commits.
		case allowLocalAhead && gitExitSuccess(repoRoot, "merge-base", "--is-ancestor", localSHA, expectedSHA):
			command := exec.Command("git", "-C", repoRoot, "update-ref", "refs/heads/"+branch, expectedSHA, localSHA)
			if output, updateErr := command.CombinedOutput(); updateErr != nil {
				return absolutePath, "CONFLICT", fmt.Errorf("fast-forward local work branch: %s",
					conciseError(errors.New(string(output))))
			}
			localSHA = expectedSHA
		default:
			return absolutePath, "CONFLICT", errors.New("local work branch differs from remote work branch")
		}
	}
	if err := os.MkdirAll(filepath.Dir(absolutePath), 0o700); err != nil {
		return absolutePath, "CREATE_FAILED", fmt.Errorf("create worktree parent: %w", err)
	}
	arguments := []string{"-C", repoRoot, "worktree", "add"}
	if localSHA == "" {
		arguments = append(arguments, "-b", branch, absolutePath, expectedSHA)
	} else {
		arguments = append(arguments, absolutePath, branch)
	}
	command := exec.Command("git", arguments...)
	if output, commandErr := command.CombinedOutput(); commandErr != nil {
		return absolutePath, "CREATE_FAILED", fmt.Errorf("create candidate worktree: %s", conciseError(errors.New(string(output))))
	}
	entries, err = listWorktrees(repoRoot)
	if err != nil {
		return absolutePath, "CREATE_FAILED", err
	}
	for _, entry := range entries {
		localAhead := allowLocalAhead && gitExitSuccess(repoRoot, "merge-base", "--is-ancestor", expectedSHA, entry.Head)
		if samePath(entry.Path, absolutePath) && entry.Branch == "refs/heads/"+branch &&
			(entry.Head == expectedSHA || localAhead) {
			if !allowLocalAhead {
				clean, cleanErr := worktreeIsClean(entry.Path)
				if cleanErr != nil {
					return absolutePath, "CREATE_FAILED", cleanErr
				}
				if !clean {
					return entry.Path, "CONFLICT", errors.New("created initial-dispatch worktree is dirty")
				}
			}

			return entry.Path, "CREATED", nil
		}
	}

	return absolutePath, "CREATE_FAILED", errors.New("created worktree failed exact binding verification")
}

func worktreeIsClean(path string) (bool, error) {
	command := exec.Command("git", "-C", path, "status", "--porcelain=v1", "--untracked-files=all")
	output, err := command.Output()
	if err != nil {
		return false, fmt.Errorf("inspect worktree cleanliness: %w", err)
	}

	return len(output) == 0, nil
}

func resolvePhysicalCandidate(path string) (string, error) {
	probe := filepath.Clean(path)
	missing := []string{}
	for {
		info, err := os.Stat(probe)
		if err == nil {
			if !info.IsDir() {
				return "", errors.New("worktree path has a non-directory ancestor")
			}

			break
		}
		if !errors.Is(err, os.ErrNotExist) {
			return "", fmt.Errorf("inspect worktree ancestor: %w", err)
		}
		next := filepath.Dir(probe)
		if next == probe {
			return "", errors.New("worktree path has no existing ancestor")
		}
		missing = append([]string{filepath.Base(probe)}, missing...)
		probe = next
	}
	physical, err := filepath.EvalSymlinks(probe)
	if err != nil {
		return "", fmt.Errorf("resolve worktree ancestor: %w", err)
	}
	for _, component := range missing {
		physical = filepath.Join(physical, component)
	}

	return physical, nil
}

func gitExitSuccess(directory string, arguments ...string) bool {
	command := exec.Command("git", append([]string{"-C", directory}, arguments...)...)

	return command.Run() == nil
}

func looksLikeValidationPath(path string) bool {
	components := strings.Split(filepath.Clean(path), string(filepath.Separator))
	seenWorktreeRoot := false
	for _, component := range components {
		if strings.HasSuffix(component, "-ai-worktrees") {
			seenWorktreeRoot = true
		}
		if seenWorktreeRoot && component == "validation" {
			return true
		}
	}

	return false
}

type worktreeEntry struct {
	Path   string
	Head   string
	Branch string
}

func listWorktrees(repoRoot string) ([]worktreeEntry, error) {
	command := exec.Command("git", "-C", repoRoot, "worktree", "list", "--porcelain")
	output, err := command.Output()
	if err != nil {
		return nil, fmt.Errorf("list worktrees: %w", err)
	}
	var entries []worktreeEntry
	var current worktreeEntry
	for line := range strings.SplitSeq(string(output), "\n") {
		switch {
		case strings.HasPrefix(line, "worktree "):
			if current.Path != "" {
				entries = append(entries, current)
			}
			current = worktreeEntry{Path: strings.TrimPrefix(line, "worktree ")}
		case strings.HasPrefix(line, "HEAD "):
			current.Head = strings.TrimPrefix(line, "HEAD ")
		case strings.HasPrefix(line, "branch "):
			current.Branch = strings.TrimPrefix(line, "branch ")
		}
	}
	if current.Path != "" {
		entries = append(entries, current)
	}

	return entries, nil
}

func samePath(left, right string) bool {
	leftAbsolute, leftErr := filepath.Abs(left)
	rightAbsolute, rightErr := filepath.Abs(right)
	if leftErr != nil || rightErr != nil {
		return false
	}
	if physical, err := filepath.EvalSymlinks(leftAbsolute); err == nil {
		leftAbsolute = physical
	}
	if physical, err := filepath.EvalSymlinks(rightAbsolute); err == nil {
		rightAbsolute = physical
	}

	return filepath.Clean(leftAbsolute) == filepath.Clean(rightAbsolute)
}

func gitOutput(directory string, arguments ...string) string {
	command := exec.Command("git", append([]string{"-C", directory}, arguments...)...)
	output, err := command.Output()
	if err != nil {
		return ""
	}

	return strings.TrimSpace(string(output))
}
