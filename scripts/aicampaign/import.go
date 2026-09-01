package main

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"strings"
	"time"
)

type auditReport struct {
	AuditID        string          `json:"audit_id"`
	Head           string          `json:"head"`
	Summary        string          `json:"summary"`
	InspectedAreas []string        `json:"inspected_areas"`
	Findings       []auditFinding  `json:"findings"`
	Questions      []auditQuestion `json:"questions"`
	ResidualRisk   string          `json:"residual_risk"`
}

type auditFinding struct {
	ID                string `json:"id"`
	Severity          string `json:"severity"`
	Title             string `json:"title"`
	Location          string `json:"location"`
	ViolatedInvariant string `json:"violated_invariant"`
	FailurePath       string `json:"failure_path"`
	Impact            string `json:"impact"`
	Evidence          string `json:"evidence"`
	ReproductionPlan  string `json:"reproduction_plan"`
	TestGap           string `json:"test_gap"`
	Confidence        string `json:"confidence"`
}

type auditQuestion struct {
	Location     string `json:"location"`
	Question     string `json:"question"`
	WhyItMatters string `json:"why_it_matters"`
}

type challengeReport struct {
	AuditID           string            `json:"audit_id"`
	Head              string            `json:"head"`
	SourceAuditDigest string            `json:"sourceAuditDigest"`
	Summary           string            `json:"summary"`
	Results           []challengeResult `json:"results"`
	Questions         []auditQuestion   `json:"questions"`
	ResidualRisk      string            `json:"residual_risk"`
}

type challengeResult struct {
	ID                     string   `json:"id"`
	Severity               string   `json:"severity"`
	Title                  string   `json:"title"`
	Status                 string   `json:"status"`
	ChallengeSummary       string   `json:"challenge_summary"`
	EvidenceFor            []string `json:"evidence_for"`
	EvidenceAgainst        []string `json:"evidence_against"`
	InvariantAssessment    string   `json:"invariant_assessment"`
	ReachabilityAssessment string   `json:"reachability_assessment"`
	ExistingTests          []string `json:"existing_tests"`
	ReproductionPlan       string   `json:"reproduction_plan"`
	RecommendedNextAction  string   `json:"recommended_next_action"`
}

type importer struct {
	now func() time.Time
}

func (runner importer) run(auditPath, qualifiedPath string) (*Campaign, error) {
	var source auditReport
	auditContent, err := readStrictJSON(auditPath, &source)
	if err != nil {
		return nil, fmt.Errorf("invalid source audit result: %w", err)
	}
	var qualified challengeReport
	qualifiedContent, err := readStrictJSON(qualifiedPath, &qualified)
	if err != nil {
		return nil, fmt.Errorf("invalid qualified result: %w", err)
	}
	if err := validateImportReports(&source, &qualified, auditContent); err != nil {
		return nil, err
	}

	campaign := &Campaign{
		SchemaVersion: campaignSchemaVersion,
		AuditID:       source.AuditID,
		AuditedSHA:    source.Head,
		ImportedAt:    runner.now().UTC(),
		SourceDigests: SourceDigests{
			Audit:     digestBytes(auditContent),
			Qualified: digestBytes(qualifiedContent),
		},
		SourceFacts: SourceFacts{FactType: sourceFactType},
	}
	sourceIndexes := make(map[string]int, len(source.Findings))
	for index, finding := range source.Findings {
		sourceIndexes[finding.ID] = index
	}
	qualifiedIndexes := make(map[string]int, len(qualified.Results))
	qualifiedByID := make(map[string]challengeResult, len(qualified.Results))
	for index, finding := range qualified.Results {
		qualifiedIndexes[finding.ID] = index
		qualifiedByID[finding.ID] = finding
	}
	for _, original := range source.Findings {
		qualification := qualifiedByID[original.ID]
		finding := SourceFinding{
			FactType:      sourceFactType,
			ID:            original.ID,
			Title:         original.Title,
			Severity:      original.Severity,
			Qualification: qualification.Status,
			Dispatchable:  qualification.Status == "CONFIRMED",
			References: FindingReferences{
				AuditFinding:        fmt.Sprintf("audit:%s#/findings/%d", campaign.SourceDigests.Audit, sourceIndexes[original.ID]),
				Invariant:           fmt.Sprintf("audit:%s#/findings/%d/violated_invariant", campaign.SourceDigests.Audit, sourceIndexes[original.ID]),
				QualificationResult: fmt.Sprintf("qualified:%s#/results/%d", campaign.SourceDigests.Qualified, qualifiedIndexes[original.ID]),
				ChallengeSummary:    fmt.Sprintf("qualified:%s#/results/%d/challenge_summary", campaign.SourceDigests.Qualified, qualifiedIndexes[original.ID]),
			},
		}
		campaign.SourceFacts.Findings = append(campaign.SourceFacts.Findings, finding)
	}
	slices.SortFunc(campaign.SourceFacts.Findings, func(left, right SourceFinding) int {
		return strings.Compare(left.ID, right.ID)
	})
	for index := range campaign.SourceFacts.Findings {
		campaign.SourceFacts.Findings[index].IdentityDigest = sourceFindingDigest(campaign, campaign.SourceFacts.Findings[index])
	}
	campaign.SourceFacts.IdentityDigest = sourceFactsDigest(campaign)
	campaign.CampaignID = campaignID(campaign)
	if err := campaign.validate(); err != nil {
		return nil, fmt.Errorf("constructed invalid campaign: %w", err)
	}

	return campaign, nil
}

func validateImportReports(source *auditReport, qualified *challengeReport, auditContent []byte) error {
	if !auditIDPattern.MatchString(source.AuditID) || !shaPattern.MatchString(source.Head) ||
		len(source.Findings) == 0 || source.InspectedAreas == nil || source.Questions == nil ||
		!validResidualRisk(source.ResidualRisk) || !nonEmptyStrings(source.InspectedAreas) ||
		!validQuestions(source.Questions) {
		return errors.New("malformed source audit schema")
	}
	if qualified.AuditID != source.AuditID || qualified.Head != source.Head {
		return errors.New("mismatched audit/challenge provenance")
	}
	if !digestPattern.MatchString(qualified.SourceAuditDigest) || qualified.SourceAuditDigest != digestBytes(auditContent) {
		return errors.New("source audit digest mismatch")
	}
	if !validResidualRisk(qualified.ResidualRisk) || len(qualified.Results) == 0 || qualified.Questions == nil ||
		!validQuestions(qualified.Questions) {
		return errors.New("malformed qualification schema")
	}
	originals := make(map[string]auditFinding, len(source.Findings))
	for _, finding := range source.Findings {
		if !findingIDPattern.MatchString(finding.ID) || !strings.HasPrefix(finding.ID, source.AuditID+"/") ||
			!validSeverity(finding.Severity) || finding.Title == "" || finding.ViolatedInvariant == "" ||
			finding.FailurePath == "" || finding.Impact == "" || finding.Evidence == "" ||
			finding.ReproductionPlan == "" || !validConfidence(finding.Confidence) {
			return fmt.Errorf("malformed source finding %q", finding.ID)
		}
		if _, exists := originals[finding.ID]; exists {
			return fmt.Errorf("duplicate finding id %q", finding.ID)
		}
		originals[finding.ID] = finding
	}
	qualifiedIDs := make(map[string]struct{}, len(qualified.Results))
	for _, finding := range qualified.Results {
		if _, exists := qualifiedIDs[finding.ID]; exists {
			return fmt.Errorf("duplicate finding id %q", finding.ID)
		}
		qualifiedIDs[finding.ID] = struct{}{}
		original, exists := originals[finding.ID]
		if !exists {
			return fmt.Errorf("qualification contains unknown finding %q", finding.ID)
		}
		if finding.Severity != original.Severity || finding.Title != original.Title {
			return fmt.Errorf("qualification mismatch for %q", finding.ID)
		}
		if !validQualification(finding.Status) || finding.ChallengeSummary == "" ||
			finding.InvariantAssessment == "" || finding.ReachabilityAssessment == "" ||
			finding.ReproductionPlan == "" || finding.RecommendedNextAction == "" ||
			finding.EvidenceFor == nil || finding.EvidenceAgainst == nil || finding.ExistingTests == nil {
			return fmt.Errorf("malformed qualification for %q", finding.ID)
		}
	}
	if len(qualifiedIDs) != len(originals) {
		return errors.New("incomplete qualification set")
	}

	return nil
}

func nonEmptyStrings(values []string) bool {
	return !slices.Contains(values, "")
}

func validQuestions(questions []auditQuestion) bool {
	for _, question := range questions {
		if question.Question == "" || question.WhyItMatters == "" {
			return false
		}
	}

	return true
}

func validConfidence(value string) bool {
	return value == "HIGH" || value == "MEDIUM" || value == "LOW"
}

func validResidualRisk(value string) bool {
	return value == "HIGH" || value == "MEDIUM" || value == "LOW"
}

func digestBytes(content []byte) string {
	sum := sha256.Sum256(content)

	return "sha256:" + hex.EncodeToString(sum[:])
}

func defaultCampaignPath(repoRoot string, campaign *Campaign) string {
	return filepath.Join(repoRoot, "build", "ai-campaign", campaign.CampaignID+".json")
}
