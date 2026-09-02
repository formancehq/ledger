package main

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"
)

const (
	claimRefPrefix    = "refs/heads/ai-claims/v1/"
	defaultClaimLease = 24 * time.Hour
	minimumClaimLease = time.Hour
	maximumClaimLease = 7 * 24 * time.Hour
	claimClockSkew    = 5 * time.Minute
)

type ClaimRecord struct {
	SchemaVersion         string            `json:"schemaVersion"`
	CampaignID            string            `json:"campaignId"`
	AuditID               string            `json:"auditId"`
	FindingID             string            `json:"findingId"`
	FindingIdentityDigest string            `json:"findingIdentityDigest"`
	SourceQualifiedDigest string            `json:"sourceQualifiedDigest"`
	AuditedSHA            string            `json:"auditedSha"`
	Qualification         string            `json:"qualification"`
	Claimant              string            `json:"claimant"`
	CreatedAt             time.Time         `json:"createdAt"`
	ExpiresAt             time.Time         `json:"expiresAt"`
	TargetBranch          string            `json:"targetBranch"`
	TargetSHA             string            `json:"targetSha"`
	WorkBranch            string            `json:"workBranch"`
	RenewedAt             *time.Time        `json:"renewedAt"`
	RenewalCount          int               `json:"renewalCount"`
	Predecessor           *ClaimPredecessor `json:"predecessor"`
}

type ClaimPredecessor struct {
	ClaimSHA  string    `json:"claimSha"`
	Claimant  string    `json:"claimant"`
	CreatedAt time.Time `json:"createdAt"`
	ExpiresAt time.Time `json:"expiresAt"`
	Reason    string    `json:"reason"`
}

type ClaimResult struct {
	SchemaVersion    string       `json:"schemaVersion"`
	Status           string       `json:"status"`
	CampaignID       string       `json:"campaignId"`
	FindingID        string       `json:"findingId"`
	RemoteRef        string       `json:"remoteRef"`
	ObservedClaimSHA string       `json:"observedClaimSha"`
	ClaimSHA         string       `json:"claimSha"`
	Claim            *ClaimRecord `json:"claim"`
	Reason           string       `json:"reason"`
}

func (claim ClaimRecord) validate() error {
	if claim.SchemaVersion != claimSchemaVersion {
		return fmt.Errorf("unsupported claim schema version %q", claim.SchemaVersion)
	}
	if !auditIDPattern.MatchString(claim.AuditID) || !findingIDPattern.MatchString(claim.FindingID) ||
		!strings.HasPrefix(claim.FindingID, claim.AuditID+"/") || !digestPattern.MatchString(claim.FindingIdentityDigest) ||
		!digestPattern.MatchString(claim.SourceQualifiedDigest) || !shaPattern.MatchString(claim.AuditedSHA) ||
		claim.Qualification != "CONFIRMED" || !claimantPattern.MatchString(claim.Claimant) {
		return errors.New("invalid claim identity")
	}
	if !campaignIDPattern.MatchString(claim.CampaignID) {
		return errors.New("invalid campaign identity")
	}
	if claim.CreatedAt.IsZero() || claim.ExpiresAt.IsZero() || !claim.ExpiresAt.After(claim.CreatedAt) ||
		claim.CreatedAt.Location() != time.UTC || claim.ExpiresAt.Location() != time.UTC {
		return errors.New("invalid claim lease")
	}
	if claim.RenewalCount < 0 || claim.RenewalCount == 0 && claim.RenewedAt != nil ||
		claim.RenewalCount > 0 && claim.RenewedAt == nil {
		return errors.New("invalid claim renewal metadata")
	}
	if claim.RenewedAt != nil && (claim.RenewedAt.IsZero() || claim.RenewedAt.Location() != time.UTC ||
		claim.RenewedAt.Before(claim.CreatedAt) || claim.RenewedAt.After(claim.ExpiresAt)) {
		return errors.New("invalid claim renewal time")
	}
	leaseStart := claim.CreatedAt
	if claim.RenewedAt != nil {
		leaseStart = *claim.RenewedAt
	}
	if lease := claim.ExpiresAt.Sub(leaseStart); lease < minimumClaimLease || lease > maximumClaimLease {
		return errors.New("claim lease is outside policy bounds")
	}
	if claim.TargetBranch == "" || exec.Command("git", "check-ref-format", "refs/heads/"+claim.TargetBranch).Run() != nil ||
		strings.HasPrefix("refs/heads/"+claim.TargetBranch, claimRefPrefix) || !shaPattern.MatchString(claim.TargetSHA) {
		return errors.New("invalid claim target")
	}
	if claim.WorkBranch != "" && (exec.Command("git", "check-ref-format", "refs/heads/"+claim.WorkBranch).Run() != nil ||
		strings.HasPrefix("refs/heads/"+claim.WorkBranch, claimRefPrefix) || claim.WorkBranch == claim.TargetBranch) {
		return errors.New("invalid claim work branch")
	}
	if claim.Predecessor != nil {
		predecessor := claim.Predecessor
		if !shaPattern.MatchString(predecessor.ClaimSHA) || !claimantPattern.MatchString(predecessor.Claimant) ||
			predecessor.CreatedAt.IsZero() || predecessor.ExpiresAt.IsZero() ||
			predecessor.CreatedAt.Location() != time.UTC || predecessor.ExpiresAt.Location() != time.UTC ||
			!predecessor.ExpiresAt.After(predecessor.CreatedAt) || predecessor.Reason != "EXPIRED_TAKEOVER" {
			return errors.New("invalid claim predecessor")
		}
	}

	return nil
}

func (claim ClaimRecord) matches(campaign *Campaign, finding SourceFinding) bool {
	return claim.CampaignID == campaign.CampaignID && claim.AuditID == campaign.AuditID &&
		claim.FindingID == finding.ID && claim.FindingIdentityDigest == finding.IdentityDigest &&
		claim.SourceQualifiedDigest == campaign.SourceDigests.Qualified && claim.AuditedSHA == campaign.AuditedSHA &&
		claim.Qualification == finding.Qualification && claim.TargetSHA == campaign.AuditedSHA
}

func claimRef(auditID, findingID string) string {
	payload := strings.Join([]string{"ai-claim-ref/v1", auditID, findingID}, "\x00")
	sum := sha256.Sum256([]byte(payload))

	return claimRefPrefix + hex.EncodeToString(sum[:])
}

func findingByID(campaign *Campaign, findingID string) (SourceFinding, error) {
	for _, finding := range campaign.SourceFacts.Findings {
		if finding.ID == findingID {
			return finding, nil
		}
	}

	return SourceFinding{}, fmt.Errorf("finding %q is not in campaign", findingID)
}

func validateClaimLease(value string) (time.Duration, error) {
	if value == "" {
		return defaultClaimLease, nil
	}
	duration, err := time.ParseDuration(value)
	if err != nil {
		return 0, fmt.Errorf("invalid lease duration: %w", err)
	}
	if duration < minimumClaimLease || duration > maximumClaimLease {
		return 0, fmt.Errorf("lease duration must be between %s and %s", minimumClaimLease, maximumClaimLease)
	}

	return duration, nil
}

func validateWorkBranch(branch string) error {
	if branch == "" {
		return nil
	}
	if exec.Command("git", "check-ref-format", "refs/heads/"+branch).Run() != nil || strings.HasPrefix(branch, "ai-claims/") {
		return errors.New("invalid work branch")
	}

	return nil
}

func resolveClaimant(explicit string, generate bool) (string, error) {
	claimant := explicit
	if claimant == "" {
		claimant = os.Getenv("AI_CAMPAIGN_CLAIMANT")
	}
	if claimant == "" && !generate {
		return "", errors.New("claimant is required (--claimant or AI_CAMPAIGN_CLAIMANT)")
	}
	if claimant == "" {
		random := make([]byte, 16)
		if _, err := rand.Read(random); err != nil {
			return "", fmt.Errorf("generate claimant identity: %w", err)
		}
		user := sanitizeIdentityComponent(os.Getenv("USER"), "agent")
		host, err := os.Hostname()
		if err != nil {
			host = "machine"
		}
		claimant = fmt.Sprintf("%s@%s#%s", user, sanitizeIdentityComponent(host, "machine"), hex.EncodeToString(random))
	}
	if !claimantPattern.MatchString(claimant) {
		return "", errors.New("claimant must be 3-160 non-sensitive ref-safe characters")
	}

	return claimant, nil
}

func sanitizeIdentityComponent(value, fallback string) string {
	var builder strings.Builder
	for _, character := range value {
		if character >= 'a' && character <= 'z' || character >= 'A' && character <= 'Z' ||
			character >= '0' && character <= '9' || strings.ContainsRune("._-", character) {
			builder.WriteRune(character)
		}
	}
	if builder.Len() == 0 {
		return fallback
	}

	return builder.String()
}

func isExpired(claim ClaimRecord, now time.Time) bool {
	return !now.UTC().Before(claim.ExpiresAt.Add(claimClockSkew))
}
