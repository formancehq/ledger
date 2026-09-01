package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"time"
)

const (
	campaignSchemaVersion   = "ai-campaign/v1"
	inspectionSchemaVersion = "ai-campaign-inspection/v1"
	nextSchemaVersion       = "ai-campaign-next/v1"
	sourceFactType          = "SOURCE_FACT"
	observationFactType     = "DERIVED_OBSERVATION"
)

var (
	auditIDPattern     = regexp.MustCompile(`^[a-z0-9][a-z0-9-]*$`)
	findingIDPattern   = regexp.MustCompile(`^[a-z0-9][a-z0-9-]*/[a-z0-9]+(?:-[a-z0-9]+)*$`)
	shaPattern         = regexp.MustCompile(`^[0-9a-f]{40,64}$`)
	digestPattern      = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
	jiraKeyPattern     = regexp.MustCompile(`^[A-Z][A-Z0-9_]*-[1-9][0-9]*$`)
	jiraProjectPattern = regexp.MustCompile(`^[A-Z][A-Z0-9_]*$`)
	githubRepoPattern  = regexp.MustCompile(`^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$`)
	gitRemotePattern   = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._/-]*$`)
)

type Campaign struct {
	SchemaVersion string          `json:"schemaVersion"`
	CampaignID    string          `json:"campaignId"`
	AuditID       string          `json:"auditId"`
	AuditedSHA    string          `json:"auditedSha"`
	ImportedAt    time.Time       `json:"importedAt"`
	SourceDigests SourceDigests   `json:"sourceDigests"`
	SourceFacts   SourceFacts     `json:"sourceFacts"`
	Observations  *ObservationSet `json:"observations"`
}

type SourceDigests struct {
	Audit     string `json:"audit"`
	Qualified string `json:"qualified"`
}

type SourceFacts struct {
	FactType       string          `json:"factType"`
	IdentityDigest string          `json:"identityDigest"`
	Findings       []SourceFinding `json:"findings"`
}

type SourceFinding struct {
	FactType       string            `json:"factType"`
	ID             string            `json:"id"`
	Title          string            `json:"title"`
	Severity       string            `json:"severity"`
	Qualification  string            `json:"qualification"`
	Dispatchable   bool              `json:"dispatchable"`
	References     FindingReferences `json:"references"`
	IdentityDigest string            `json:"identityDigest"`
}

type FindingReferences struct {
	AuditFinding        string `json:"auditFinding"`
	Invariant           string `json:"invariant"`
	QualificationResult string `json:"qualificationResult"`
	ChallengeSummary    string `json:"challengeSummary"`
}

type ObservationSet struct {
	FactType    string               `json:"factType"`
	RefreshedAt *time.Time           `json:"refreshedAt"`
	Freshness   string               `json:"freshness"`
	Providers   ProviderObservations `json:"providers"`
	Target      TargetObservation    `json:"target"`
	Findings    []FindingObservation `json:"findings"`
}

type ProviderObservations struct {
	GitHub ProviderObservation `json:"github"`
	Jira   ProviderObservation `json:"jira"`
	Git    ProviderObservation `json:"git"`
}

type ProviderObservation struct {
	Status     string     `json:"status"`
	ObservedAt *time.Time `json:"observedAt"`
	Error      string     `json:"error"`
}

type TargetObservation struct {
	FactType    string `json:"factType"`
	Branch      string `json:"branch"`
	ObservedSHA string `json:"observedSha"`
}

type FindingObservation struct {
	FactType             string          `json:"factType"`
	ID                   string          `json:"id"`
	State                string          `json:"state"`
	Jira                 JiraObservation `json:"jira"`
	PR                   PRObservation   `json:"pr"`
	ObservedCandidateSHA string          `json:"observedCandidateSha"`
	ObservedTargetSHA    string          `json:"observedTargetSha"`
	Blockers             []string        `json:"blockers"`
	NextAction           string          `json:"nextAction"`
	Freshness            string          `json:"freshness"`
	Confidence           string          `json:"confidence"`
}

type JiraObservation struct {
	Status       string      `json:"status"`
	BindingBasis string      `json:"bindingBasis"`
	Issues       []JiraIssue `json:"issues"`
}

type JiraIssue struct {
	Key      string `json:"key"`
	URL      string `json:"url"`
	Status   string `json:"status"`
	Assignee string `json:"assignee"`
}

type PRObservation struct {
	Status       string    `json:"status"`
	BindingBasis string    `json:"bindingBasis"`
	Matches      []PRMatch `json:"matches"`
}

type PRMatch struct {
	Number          int    `json:"number"`
	URL             string `json:"url"`
	State           string `json:"state"`
	HeadRef         string `json:"headRef"`
	HeadSHA         string `json:"headSha"`
	BaseRef         string `json:"baseRef"`
	BaseSHA         string `json:"baseSha"`
	Merged          bool   `json:"merged"`
	BranchExists    bool   `json:"branchExists"`
	CrossRepository bool   `json:"crossRepository"`
	ReviewDecision  string `json:"reviewDecision"`
	Checks          string `json:"checks"`
	BindingBasis    string `json:"bindingBasis"`
}

type Inspection struct {
	SchemaVersion string               `json:"schemaVersion"`
	CampaignID    string               `json:"campaignId"`
	AuditID       string               `json:"auditId"`
	AuditedSHA    string               `json:"auditedSha"`
	SourceDigests SourceDigests        `json:"sourceDigests"`
	RefreshedAt   *time.Time           `json:"refreshedAt"`
	Freshness     string               `json:"freshness"`
	Providers     ProviderObservations `json:"providers"`
	Findings      []InspectionFinding  `json:"findings"`
}

type InspectionFinding struct {
	ID                   string          `json:"id"`
	Title                string          `json:"title"`
	Severity             string          `json:"severity"`
	Qualification        string          `json:"qualification"`
	Dispatchable         bool            `json:"dispatchable"`
	State                string          `json:"state"`
	Jira                 JiraObservation `json:"jira"`
	PR                   PRObservation   `json:"pr"`
	ObservedCandidateSHA string          `json:"observedCandidateSha"`
	ObservedTargetSHA    string          `json:"observedTargetSha"`
	Blockers             []string        `json:"blockers"`
	NextAction           string          `json:"nextAction"`
	Freshness            string          `json:"freshness"`
	Confidence           string          `json:"confidence"`
}

type NextResult struct {
	SchemaVersion      string        `json:"schemaVersion"`
	CampaignID         string        `json:"campaignId"`
	AuditID            string        `json:"auditId"`
	AuditedSHA         string        `json:"auditedSha"`
	BasedOnRefreshedAt *time.Time    `json:"basedOnRefreshedAt"`
	Freshness          string        `json:"freshness"`
	Findings           []NextFinding `json:"findings"`
}

type NextFinding struct {
	ID            string   `json:"id"`
	Qualification string   `json:"qualification"`
	State         string   `json:"state"`
	NextAction    string   `json:"nextAction"`
	Blockers      []string `json:"blockers"`
	Freshness     string   `json:"freshness"`
}

func (campaign *Campaign) validate() error {
	if campaign.SchemaVersion != campaignSchemaVersion {
		return fmt.Errorf("unsupported schema version %q", campaign.SchemaVersion)
	}
	if !auditIDPattern.MatchString(campaign.AuditID) || !shaPattern.MatchString(campaign.AuditedSHA) {
		return errors.New("invalid campaign audit identity")
	}
	if campaign.ImportedAt.IsZero() || !digestPattern.MatchString(campaign.SourceDigests.Audit) ||
		!digestPattern.MatchString(campaign.SourceDigests.Qualified) {
		return errors.New("invalid campaign provenance")
	}
	if campaign.SourceFacts.FactType != sourceFactType || campaign.SourceFacts.Findings == nil {
		return errors.New("invalid source facts")
	}
	ids := make(map[string]SourceFinding, len(campaign.SourceFacts.Findings))
	for index := range campaign.SourceFacts.Findings {
		finding := &campaign.SourceFacts.Findings[index]
		if finding.FactType != sourceFactType || !findingIDPattern.MatchString(finding.ID) ||
			!strings.HasPrefix(finding.ID, campaign.AuditID+"/") || finding.Title == "" ||
			!validSeverity(finding.Severity) || !validQualification(finding.Qualification) ||
			finding.Dispatchable != (finding.Qualification == "CONFIRMED") {
			return fmt.Errorf("invalid source finding %q", finding.ID)
		}
		if _, exists := ids[finding.ID]; exists {
			return fmt.Errorf("duplicate finding id %q", finding.ID)
		}
		ids[finding.ID] = *finding
		if finding.References.AuditFinding == "" || finding.References.Invariant == "" ||
			finding.References.QualificationResult == "" || finding.References.ChallengeSummary == "" {
			return fmt.Errorf("incomplete source references for %q", finding.ID)
		}
		if finding.IdentityDigest != sourceFindingDigest(campaign, *finding) {
			return fmt.Errorf("source identity digest mismatch for %q", finding.ID)
		}
	}
	if !slices.IsSortedFunc(campaign.SourceFacts.Findings, func(left, right SourceFinding) int {
		return strings.Compare(left.ID, right.ID)
	}) {
		return errors.New("source findings are not in deterministic id order")
	}
	if campaign.SourceFacts.IdentityDigest != sourceFactsDigest(campaign) {
		return errors.New("source facts digest mismatch")
	}
	if campaign.CampaignID != campaignID(campaign) {
		return errors.New("campaign id mismatch")
	}
	if campaign.Observations != nil {
		if err := campaign.validateObservations(ids); err != nil {
			return err
		}
	}

	return nil
}

func (campaign *Campaign) validateObservations(sourceFindings map[string]SourceFinding) error {
	observations := campaign.Observations
	if observations.FactType != observationFactType || observations.Findings == nil ||
		!validFreshness(observations.Freshness) {
		return errors.New("invalid observation envelope")
	}
	if observations.Freshness != combinedFreshness(observations.Providers) {
		return errors.New("observation freshness does not match provider freshness")
	}
	for name, provider := range map[string]ProviderObservation{
		"github": observations.Providers.GitHub,
		"jira":   observations.Providers.Jira,
		"git":    observations.Providers.Git,
	} {
		if !validProviderStatus(provider.Status) ||
			(provider.Status != "UNAVAILABLE" && provider.ObservedAt == nil) ||
			(provider.ObservedAt != nil && provider.ObservedAt.IsZero()) {
			return fmt.Errorf("invalid %s provider observation", name)
		}
	}
	if (observations.Freshness == "FRESH" || observations.Freshness == "PARTIAL") && observations.RefreshedAt == nil {
		return errors.New("refreshed observations require refreshedAt")
	}
	if observations.RefreshedAt != nil && observations.RefreshedAt.IsZero() {
		return errors.New("invalid refreshedAt")
	}
	if observations.Target.FactType != observationFactType || observations.Target.Branch == "" ||
		(observations.Target.ObservedSHA != "" && !shaPattern.MatchString(observations.Target.ObservedSHA)) {
		return errors.New("invalid target observation")
	}
	if len(observations.Findings) != len(sourceFindings) {
		return errors.New("incomplete finding observations")
	}
	seen := make(map[string]struct{}, len(observations.Findings))
	for index, finding := range observations.Findings {
		if finding.FactType != observationFactType {
			return fmt.Errorf("invalid observation for %q", finding.ID)
		}
		source, exists := sourceFindings[finding.ID]
		if !exists {
			return fmt.Errorf("observation for unknown finding %q", finding.ID)
		}
		if campaign.SourceFacts.Findings[index].ID != finding.ID {
			return errors.New("finding observations are not in deterministic source order")
		}
		if _, exists := seen[finding.ID]; exists {
			return fmt.Errorf("duplicate finding observation %q", finding.ID)
		}
		seen[finding.ID] = struct{}{}
		if !validFreshness(finding.Freshness) || finding.Freshness != observations.Freshness ||
			!validState(finding.State) || !validNextAction(finding.NextAction) || !validConfidence(finding.Confidence) ||
			(finding.ObservedCandidateSHA != "" && !shaPattern.MatchString(finding.ObservedCandidateSHA)) ||
			finding.ObservedTargetSHA != observations.Target.ObservedSHA || !uniqueNonEmpty(finding.Blockers) {
			return fmt.Errorf("incomplete finding observation %q", finding.ID)
		}
		if err := validateJiraObservation(finding.Jira); err != nil {
			return fmt.Errorf("invalid Jira observation for %q: %w", finding.ID, err)
		}
		if err := validatePRObservation(finding.PR); err != nil {
			return fmt.Errorf("invalid PR observation for %q: %w", finding.ID, err)
		}
		expected := projectFinding(source, finding.Jira, finding.PR, campaign.AuditedSHA,
			observations.Target.Branch, observations.Target.ObservedSHA, observations.Freshness)
		if finding.State != expected.State || finding.ObservedCandidateSHA != expected.ObservedCandidateSHA ||
			finding.NextAction != expected.NextAction || finding.Confidence != expected.Confidence ||
			!slices.Equal(finding.Blockers, expected.Blockers) {
			return fmt.Errorf("derived projection mismatch for %q", finding.ID)
		}
	}
	if len(seen) != len(sourceFindings) {
		return errors.New("incomplete finding observations")
	}

	return nil
}

func validateJiraObservation(observation JiraObservation) error {
	if !validBindingStatus(observation.Status, len(observation.Issues)) || observation.BindingBasis != "AI_AUDIT_MARKER" {
		return errors.New("invalid binding envelope")
	}
	seen := map[string]struct{}{}
	for _, issue := range observation.Issues {
		if !jiraKeyPattern.MatchString(issue.Key) {
			return fmt.Errorf("invalid Jira key %q", issue.Key)
		}
		if _, exists := seen[issue.Key]; exists {
			return fmt.Errorf("duplicate Jira issue %q", issue.Key)
		}
		seen[issue.Key] = struct{}{}
	}

	return nil
}

func validatePRObservation(observation PRObservation) error {
	if !validBindingStatus(observation.Status, len(observation.Matches)) {
		return errors.New("invalid binding envelope")
	}
	if len(observation.Matches) == 0 && observation.BindingBasis != "" {
		return errors.New("unbound PR observation has a binding basis")
	}
	seen := map[int]struct{}{}
	for _, match := range observation.Matches {
		if match.Number <= 0 || match.BindingBasis != "AI_AUDIT_MARKER" && match.BindingBasis != "JIRA_KEY" ||
			observation.BindingBasis != match.BindingBasis || match.HeadRef == "" || match.BaseRef == "" ||
			(match.HeadSHA != "" && !shaPattern.MatchString(match.HeadSHA)) ||
			(match.BaseSHA != "" && !shaPattern.MatchString(match.BaseSHA)) {
			return fmt.Errorf("invalid PR match #%d", match.Number)
		}
		if _, exists := seen[match.Number]; exists {
			return fmt.Errorf("duplicate PR #%d", match.Number)
		}
		seen[match.Number] = struct{}{}
	}

	return nil
}

func validBindingStatus(status string, count int) bool {
	switch status {
	case "UNKNOWN":
		return true
	case "UNBOUND":
		return count == 0
	case "BOUND":
		return count == 1
	case "AMBIGUOUS":
		return count > 1
	default:
		return false
	}
}

func uniqueNonEmpty(values []string) bool {
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		if value == "" {
			return false
		}
		if _, exists := seen[value]; exists {
			return false
		}
		seen[value] = struct{}{}
	}

	return true
}

func validProviderStatus(value string) bool {
	return value == "FRESH" || value == "STALE" || value == "UNAVAILABLE"
}

func validState(value string) bool {
	return slices.Contains([]string{
		"CONFIRMED_UNASSIGNED", "TRACKED", "PR_OPEN", "PR_CLOSED", "MERGED", "BLOCKED",
		"SUPERSEDED", "AMBIGUOUS", "BROKEN_BINDING", "NON_DISPATCHABLE",
	}, value)
}

func validNextAction(value string) bool {
	return slices.Contains([]string{
		"PUBLISH_JIRA_OR_REVIEW_POLICY", "READY_FOR_CLAIM_OR_DISPATCH", "CONTINUE_PR", "VERIFY_RESOLUTION",
		"NO_ACTION", "HUMAN_DECISION_REQUIRED", "RESOLVE_BINDING", "REFRESH_REQUIRED", "REPAIR_BINDING",
		"REVIEW_CLOSED_PR", "REQUALIFY_ON_CURRENT_TARGET",
	}, value)
}

func validSeverity(value string) bool {
	return value == "P0" || value == "P1" || value == "P2" || value == "P3"
}

func validQualification(value string) bool {
	return value == "CONFIRMED" || value == "LIKELY" || value == "QUESTION" || value == "REJECTED"
}

func validFreshness(value string) bool {
	return value == "FRESH" || value == "PARTIAL" || value == "STALE" || value == "UNAVAILABLE"
}

func campaignID(campaign *Campaign) string {
	payload := strings.Join([]string{
		campaignSchemaVersion,
		campaign.AuditID,
		campaign.AuditedSHA,
		campaign.SourceDigests.Audit,
		campaign.SourceDigests.Qualified,
	}, "\x00")
	sum := sha256.Sum256([]byte(payload))

	return "campaign-" + hex.EncodeToString(sum[:])
}

func sourceFindingDigest(campaign *Campaign, finding SourceFinding) string {
	payload := struct {
		AuditID       string            `json:"auditId"`
		AuditedSHA    string            `json:"auditedSha"`
		SourceDigests SourceDigests     `json:"sourceDigests"`
		ID            string            `json:"id"`
		Title         string            `json:"title"`
		Severity      string            `json:"severity"`
		Qualification string            `json:"qualification"`
		References    FindingReferences `json:"references"`
	}{campaign.AuditID, campaign.AuditedSHA, campaign.SourceDigests, finding.ID, finding.Title,
		finding.Severity, finding.Qualification, finding.References}

	return digestJSON(payload)
}

func sourceFactsDigest(campaign *Campaign) string {
	payload := struct {
		AuditID       string          `json:"auditId"`
		AuditedSHA    string          `json:"auditedSha"`
		SourceDigests SourceDigests   `json:"sourceDigests"`
		Findings      []SourceFinding `json:"findings"`
	}{campaign.AuditID, campaign.AuditedSHA, campaign.SourceDigests, campaign.SourceFacts.Findings}

	return digestJSON(payload)
}

func digestJSON(value any) string {
	content, err := json.Marshal(value)
	if err != nil {
		panic(fmt.Sprintf("marshal deterministic digest input: %v", err))
	}
	sum := sha256.Sum256(content)

	return "sha256:" + hex.EncodeToString(sum[:])
}
