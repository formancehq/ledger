package main

import (
	"bytes"
	"context"
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

var (
	errClaimChanged   = errors.New("remote claim changed during observation")
	errRemoteMutation = errors.New("remote compare-and-swap rejected")
)

type observedClaim struct {
	Record ClaimRecord
	SHA    string
	Ref    string
}

type claimRepository struct {
	directory string
	remoteURL string
}

func openClaimRepository(ctx context.Context, remote string) (*claimRepository, error) {
	remoteURL := remote
	if !filepath.IsAbs(remote) && !strings.Contains(remote, "://") {
		var output []byte
		for _, suffix := range []string{"pushurl", "url"} {
			command := exec.CommandContext(ctx, "git", "config", "--get", "remote."+remote+"."+suffix)
			value, err := command.Output()
			if err == nil && strings.TrimSpace(string(value)) != "" {
				output = value

				break
			}
		}
		if len(output) == 0 {
			return nil, fmt.Errorf("resolve Git remote: remote %q has no URL", remote)
		}
		remoteURL = strings.TrimSpace(string(output))
	}
	if remoteURL == "" {
		return nil, errors.New("git remote has no push URL")
	}
	directory, err := os.MkdirTemp("", "ai-campaign-claim-git-")
	if err != nil {
		return nil, fmt.Errorf("create claim Git directory: %w", err)
	}
	repository := &claimRepository{directory: directory, remoteURL: remoteURL}
	if _, err := repository.git(ctx, nil, "init", "--bare", "--quiet", directory); err != nil {
		repository.close()

		return nil, fmt.Errorf("initialize claim Git directory: %w", err)
	}

	return repository, nil
}

func (repository *claimRepository) close() {
	_ = os.RemoveAll(repository.directory) // Private best-effort temporary object cleanup.
}

func (repository *claimRepository) listRefs(ctx context.Context, refs ...string) (map[string]string, error) {
	arguments := []string{"ls-remote", "--heads", repository.remoteURL}
	arguments = append(arguments, refs...)
	output, err := repository.git(ctx, nil, arguments...)
	if err != nil {
		return nil, fmt.Errorf("query remote claims: %w", err)
	}
	result := make(map[string]string, len(refs))
	for line := range strings.SplitSeq(strings.TrimSpace(string(output)), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		if len(fields) != 2 || !shaPattern.MatchString(fields[0]) || !slices.Contains(refs, fields[1]) {
			return nil, errors.New("malformed remote claim ref response")
		}
		result[fields[1]] = fields[0]
	}

	return result, nil
}

func (repository *claimRepository) readClaim(ctx context.Context, ref, expectedSHA string) (*observedClaim, error) {
	localRef := "refs/ai-claim-cache/" + strings.TrimPrefix(ref, claimRefPrefix)
	if _, err := repository.git(ctx, nil, "fetch", "--quiet", "--no-tags", "--no-write-fetch-head",
		repository.remoteURL, "+"+ref+":"+localRef); err != nil {
		return nil, fmt.Errorf("fetch remote claim: %w", err)
	}
	output, err := repository.git(ctx, nil, "rev-parse", "--verify", localRef)
	if err != nil {
		return nil, fmt.Errorf("resolve fetched claim: %w", err)
	}
	actualSHA := strings.TrimSpace(string(output))
	if expectedSHA != "" && actualSHA != expectedSHA {
		return nil, errClaimChanged
	}
	tree, err := repository.git(ctx, nil, "ls-tree", "--name-only", actualSHA)
	if err != nil {
		return nil, fmt.Errorf("inspect claim tree: %w", err)
	}
	if string(tree) != "claim.json\n" {
		return nil, errors.New("claim commit must contain only claim.json")
	}
	parents, err := repository.git(ctx, nil, "show", "-s", "--format=%P", actualSHA)
	if err != nil {
		return nil, fmt.Errorf("inspect claim parents: %w", err)
	}
	parentSHAs := strings.Fields(string(parents))
	if len(parentSHAs) > 1 {
		return nil, errors.New("claim commit has multiple parents")
	}
	content, err := repository.git(ctx, nil, "show", actualSHA+":claim.json")
	if err != nil {
		return nil, fmt.Errorf("read claim record: %w", err)
	}
	var record ClaimRecord
	if err := decodeStrictJSON(content, &record); err != nil {
		return nil, fmt.Errorf("decode claim record: %w", err)
	}
	if err := record.validate(); err != nil {
		return nil, fmt.Errorf("validate claim record: %w", err)
	}
	if record.RenewalCount == 0 && record.Predecessor == nil && len(parentSHAs) != 0 {
		return nil, errors.New("initial claim commit must be parentless")
	}
	if (record.RenewalCount > 0 || record.Predecessor != nil) && len(parentSHAs) != 1 {
		return nil, errors.New("updated claim commit must have one parent")
	}
	if record.RenewalCount == 0 && record.Predecessor != nil && parentSHAs[0] != record.Predecessor.ClaimSHA {
		return nil, errors.New("takeover predecessor does not match claim parent")
	}
	if len(parentSHAs) == 1 {
		parentContent, err := repository.git(ctx, nil, "show", parentSHAs[0]+":claim.json")
		if err != nil {
			return nil, fmt.Errorf("read parent claim record: %w", err)
		}
		var parentRecord ClaimRecord
		if err := decodeStrictJSON(parentContent, &parentRecord); err != nil {
			return nil, fmt.Errorf("decode parent claim record: %w", err)
		}
		if err := parentRecord.validate(); err != nil {
			return nil, fmt.Errorf("validate parent claim record: %w", err)
		}
		if err := validateClaimTransition(record, parentRecord, parentSHAs[0]); err != nil {
			return nil, err
		}
	}

	return &observedClaim{Record: record, SHA: actualSHA, Ref: ref}, nil
}

func validateClaimTransition(current, previous ClaimRecord, previousSHA string) error {
	if current.CampaignID != previous.CampaignID || current.AuditID != previous.AuditID ||
		current.FindingID != previous.FindingID || current.FindingIdentityDigest != previous.FindingIdentityDigest ||
		current.SourceQualifiedDigest != previous.SourceQualifiedDigest || current.AuditedSHA != previous.AuditedSHA ||
		current.Qualification != previous.Qualification || current.TargetBranch != previous.TargetBranch ||
		current.TargetSHA != previous.TargetSHA {
		return errors.New("claim transition changed immutable identity")
	}
	if current.RenewalCount > 0 {
		if current.Claimant != previous.Claimant || !current.CreatedAt.Equal(previous.CreatedAt) ||
			current.RenewalCount != previous.RenewalCount+1 ||
			!samePredecessor(current.Predecessor, previous.Predecessor) {
			return errors.New("invalid claim renewal transition")
		}
		previousMutation := previous.CreatedAt
		if previous.RenewedAt != nil {
			previousMutation = *previous.RenewedAt
		}
		if current.RenewedAt == nil || current.RenewedAt.Before(previousMutation) {
			return errors.New("claim renewal time moved backwards")
		}

		return nil
	}
	if current.Predecessor == nil || current.Predecessor.ClaimSHA != previousSHA ||
		current.Predecessor.Claimant != previous.Claimant ||
		!current.Predecessor.CreatedAt.Equal(previous.CreatedAt) ||
		!current.Predecessor.ExpiresAt.Equal(previous.ExpiresAt) ||
		current.CreatedAt.Before(previous.ExpiresAt.Add(claimClockSkew)) {
		return errors.New("invalid expired-claim takeover transition")
	}

	return nil
}

func samePredecessor(left, right *ClaimPredecessor) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}

	return left.ClaimSHA == right.ClaimSHA && left.Claimant == right.Claimant &&
		left.CreatedAt.Equal(right.CreatedAt) && left.ExpiresAt.Equal(right.ExpiresAt) && left.Reason == right.Reason
}

func (repository *claimRepository) writeClaim(
	ctx context.Context,
	ref string,
	expectedSHA string,
	record ClaimRecord,
) (string, error) {
	if err := record.validate(); err != nil {
		return "", fmt.Errorf("refusing invalid claim record: %w", err)
	}
	content, err := json.MarshalIndent(record, "", "  ")
	if err != nil {
		return "", fmt.Errorf("marshal claim record: %w", err)
	}
	content = append(content, '\n')
	blob, err := repository.git(ctx, content, "hash-object", "-w", "--stdin")
	if err != nil {
		return "", fmt.Errorf("write claim blob: %w", err)
	}
	treeInput := []byte("100644 blob " + strings.TrimSpace(string(blob)) + "\tclaim.json\n")
	tree, err := repository.git(ctx, treeInput, "mktree")
	if err != nil {
		return "", fmt.Errorf("write claim tree: %w", err)
	}
	arguments := []string{"commit-tree", strings.TrimSpace(string(tree)), "-m", "ai-campaign claim " + record.FindingID}
	if expectedSHA != "" {
		arguments = append(arguments, "-p", expectedSHA)
	}
	commitTime := record.CreatedAt
	if record.RenewedAt != nil {
		commitTime = *record.RenewedAt
	}
	environment := []string{
		"GIT_AUTHOR_NAME=ai-campaign", "GIT_AUTHOR_EMAIL=ai-campaign@formance.invalid",
		"GIT_COMMITTER_NAME=ai-campaign", "GIT_COMMITTER_EMAIL=ai-campaign@formance.invalid",
		"GIT_AUTHOR_DATE=" + commitTime.Format(time.RFC3339Nano),
		"GIT_COMMITTER_DATE=" + commitTime.Format(time.RFC3339Nano),
	}
	commit, err := repository.gitWithEnvironment(ctx, nil, environment, arguments...)
	if err != nil {
		return "", fmt.Errorf("write claim commit: %w", err)
	}
	claimSHA := strings.TrimSpace(string(commit))
	lease := "--force-with-lease=" + ref + ":" + expectedSHA
	if _, err := repository.git(ctx, nil, "push", "--porcelain", lease, repository.remoteURL, claimSHA+":"+ref); err != nil {
		return claimSHA, fmt.Errorf("%w: %w", errRemoteMutation, err)
	}

	return claimSHA, nil
}

func (repository *claimRepository) deleteClaim(ctx context.Context, ref, expectedSHA string) error {
	if expectedSHA == "" || !shaPattern.MatchString(expectedSHA) {
		return errors.New("exact observed claim SHA is required for deletion")
	}
	lease := "--force-with-lease=" + ref + ":" + expectedSHA
	if _, err := repository.git(ctx, nil, "push", "--porcelain", lease, repository.remoteURL, ":"+ref); err != nil {
		return fmt.Errorf("%w: %w", errRemoteMutation, err)
	}

	return nil
}

func (repository *claimRepository) isAncestor(ctx context.Context, ancestor, descendant string) bool {
	if !shaPattern.MatchString(ancestor) || !shaPattern.MatchString(descendant) {
		return false
	}
	_, err := repository.git(ctx, nil, "merge-base", "--is-ancestor", ancestor, descendant)

	return err == nil
}

func (repository *claimRepository) git(ctx context.Context, input []byte, arguments ...string) ([]byte, error) {
	return repository.gitWithEnvironment(ctx, input, nil, arguments...)
}

func (repository *claimRepository) gitWithEnvironment(
	ctx context.Context,
	input []byte,
	environment []string,
	arguments ...string,
) ([]byte, error) {
	if arguments[0] != "init" {
		arguments = append([]string{"--git-dir", repository.directory}, arguments...)
	}
	command := exec.CommandContext(ctx, "git", arguments...)
	command.Env = append(os.Environ(), environment...)
	if input != nil {
		command.Stdin = bytes.NewReader(input)
	}
	var stderr bytes.Buffer
	command.Stderr = &stderr
	output, err := command.Output()
	if err != nil {
		message := strings.TrimSpace(stderr.String())
		if message != "" {
			return nil, errors.New(conciseGitError(message, repository.remoteURL))
		}

		return nil, err
	}

	return output, nil
}

func conciseGitError(message, remoteURL string) string {
	message = strings.ReplaceAll(message, remoteURL, "<remote>")
	message = strings.ReplaceAll(message, filepath.Base(remoteURL), "<remote>")

	return conciseError(errors.New(message))
}
