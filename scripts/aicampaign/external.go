package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os/exec"
	"regexp"
	"slices"
	"strings"
)

type jiraProvider interface {
	Observe(context.Context, string, []SourceFinding) (map[string][]JiraIssue, error)
}

type githubProvider interface {
	Observe(context.Context, string, []SourceFinding, map[string][]JiraIssue) (map[string][]PRMatch, error)
}

type gitProvider interface {
	ObserveRefs(context.Context, string, []string) (map[string]string, error)
}

type commandJiraProvider struct{}

func (commandJiraProvider) Observe(ctx context.Context, project string, findings []SourceFinding) (map[string][]JiraIssue, error) {
	observed := make(map[string][]JiraIssue, len(findings))
	for _, finding := range findings {
		marker := "AI-AUDIT:" + finding.ID
		command := exec.CommandContext(ctx, "acli", "jira", "workitem", "search",
			"--jql", fmt.Sprintf(`project = %s AND text ~ '"%s"'`, project, marker),
			"--fields", "key,status,assignee,description", "--paginate", "--json")
		output, err := command.Output()
		if err != nil {
			return nil, fmt.Errorf("jira lookup for %s: %w", finding.ID, commandError(err))
		}
		issues, err := parseJiraIssues(output, marker)
		if err != nil {
			return nil, fmt.Errorf("jira lookup for %s: %w", finding.ID, err)
		}
		observed[finding.ID] = issues
	}

	return observed, nil
}

type commandGitHubProvider struct{}

type githubPR struct {
	Number            int             `json:"number"`
	Body              string          `json:"body"`
	URL               string          `json:"url"`
	State             string          `json:"state"`
	HeadRefName       string          `json:"headRefName"`
	HeadRefOID        string          `json:"headRefOid"`
	BaseRefName       string          `json:"baseRefName"`
	BaseRefOID        string          `json:"baseRefOid"`
	IsCrossRepository bool            `json:"isCrossRepository"`
	MergedAt          *string         `json:"mergedAt"`
	ReviewDecision    string          `json:"reviewDecision"`
	StatusCheckRollup json.RawMessage `json:"statusCheckRollup"`
}

func (commandGitHubProvider) Observe(
	ctx context.Context,
	repository string,
	findings []SourceFinding,
	jira map[string][]JiraIssue,
) (map[string][]PRMatch, error) {
	observed := make(map[string][]PRMatch, len(findings))
	for _, finding := range findings {
		marker := "AI-AUDIT:" + finding.ID
		markerPRs, err := searchPullRequests(ctx, repository, marker)
		if err != nil {
			return nil, fmt.Errorf("GitHub lookup for %s: %w", finding.ID, err)
		}
		matches := map[int]PRMatch{}
		for _, candidate := range markerPRs {
			if exactLine(candidate.Body, marker) {
				match := candidate.toMatch("AI_AUDIT_MARKER")
				matches[match.Number] = match
			}
		}
		// An explicit audit marker is the strongest available binding. Jira-key
		// references are considered only when no PR carries that marker, so a
		// weaker historical reference cannot make an explicit binding ambiguous.
		if len(matches) == 0 {
			for _, issue := range jira[finding.ID] {
				jiraPRs, err := searchPullRequests(ctx, repository, issue.Key)
				if err != nil {
					return nil, fmt.Errorf("GitHub Jira-key lookup for %s: %w", finding.ID, err)
				}
				for _, candidate := range jiraPRs {
					if exactJiraReference(candidate.Body, issue.Key) {
						match := candidate.toMatch("JIRA_KEY")
						matches[match.Number] = match
					}
				}
			}
		}
		for _, match := range matches {
			observed[finding.ID] = append(observed[finding.ID], match)
		}
		slices.SortFunc(observed[finding.ID], func(left, right PRMatch) int { return left.Number - right.Number })
	}

	return observed, nil
}

func searchPullRequests(ctx context.Context, repository, query string) ([]githubPR, error) {
	const fields = "number,body,url,state,headRefName,headRefOid,baseRefName,baseRefOid,isCrossRepository,mergedAt,reviewDecision,statusCheckRollup"
	command := exec.CommandContext(ctx, "gh", "pr", "list", "--repo", repository,
		"--state", "all", "--limit", "1000", "--search", `"`+query+`"`, "--json", fields)
	output, err := command.Output()
	if err != nil {
		return nil, commandError(err)
	}
	var pullRequests []githubPR
	if err := json.Unmarshal(output, &pullRequests); err != nil {
		return nil, fmt.Errorf("decode GitHub response: %w", err)
	}

	return pullRequests, nil
}

func (pullRequest githubPR) toMatch(bindingBasis string) PRMatch {
	return PRMatch{
		Number:          pullRequest.Number,
		URL:             pullRequest.URL,
		State:           pullRequest.State,
		HeadRef:         pullRequest.HeadRefName,
		HeadSHA:         pullRequest.HeadRefOID,
		BaseRef:         pullRequest.BaseRefName,
		BaseSHA:         pullRequest.BaseRefOID,
		Merged:          pullRequest.MergedAt != nil || strings.EqualFold(pullRequest.State, "MERGED"),
		CrossRepository: pullRequest.IsCrossRepository,
		ReviewDecision:  pullRequest.ReviewDecision,
		Checks:          summarizeChecks(pullRequest.StatusCheckRollup),
		BindingBasis:    bindingBasis,
	}
}

type commandGitProvider struct{}

func (commandGitProvider) ObserveRefs(ctx context.Context, remote string, refs []string) (map[string]string, error) {
	arguments := []string{"ls-remote", "--heads", remote}
	arguments = append(arguments, refs...)
	command := exec.CommandContext(ctx, "git", arguments...)
	output, err := command.Output()
	if err != nil {
		return nil, commandError(err)
	}
	observed := make(map[string]string, len(refs))
	for line := range strings.SplitSeq(strings.TrimSpace(string(output)), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		if len(fields) != 2 || !shaPattern.MatchString(fields[0]) {
			return nil, errors.New("malformed git ls-remote response")
		}
		observed[fields[1]] = fields[0]
	}

	return observed, nil
}

func parseJiraIssues(content []byte, marker string) ([]JiraIssue, error) {
	var document any
	if err := json.Unmarshal(content, &document); err != nil {
		return nil, fmt.Errorf("decode Jira response: %w", err)
	}
	objects := collectObjects(document)
	byKey := map[string]JiraIssue{}
	for _, object := range objects {
		key, _ := object["key"].(string)
		fields, _ := object["fields"].(map[string]any)
		if !jiraKeyPattern.MatchString(key) || fields == nil || !containsExactLine(fields["description"], marker) {
			continue
		}
		issue := JiraIssue{Key: key}
		issue.URL, _ = object["url"].(string)
		if issue.URL == "" {
			issue.URL, _ = object["self"].(string)
		}
		if status, ok := fields["status"].(map[string]any); ok {
			issue.Status, _ = status["name"].(string)
		} else {
			issue.Status, _ = fields["status"].(string)
		}
		if assignee, ok := fields["assignee"].(map[string]any); ok {
			for _, field := range []string{"displayName", "emailAddress", "accountId"} {
				if value, ok := assignee[field].(string); ok && value != "" {
					issue.Assignee = value

					break
				}
			}
		} else {
			issue.Assignee, _ = fields["assignee"].(string)
		}
		byKey[key] = issue
	}
	issues := make([]JiraIssue, 0, len(byKey))
	for _, issue := range byKey {
		issues = append(issues, issue)
	}
	slices.SortFunc(issues, func(left, right JiraIssue) int { return strings.Compare(left.Key, right.Key) })

	return issues, nil
}

func collectObjects(value any) []map[string]any {
	var collected []map[string]any
	var walk func(any)
	walk = func(current any) {
		switch typed := current.(type) {
		case map[string]any:
			collected = append(collected, typed)
			for _, child := range typed {
				walk(child)
			}
		case []any:
			for _, child := range typed {
				walk(child)
			}
		}
	}
	walk(value)

	return collected
}

func containsExactLine(value any, marker string) bool {
	switch typed := value.(type) {
	case string:
		return exactLine(typed, marker)
	case map[string]any:
		for _, child := range typed {
			if containsExactLine(child, marker) {
				return true
			}
		}
	case []any:
		for _, child := range typed {
			if containsExactLine(child, marker) {
				return true
			}
		}
	}

	return false
}

func exactLine(value, marker string) bool {
	for line := range strings.SplitSeq(value, "\n") {
		if strings.TrimSpace(line) == marker {
			return true
		}
	}

	return false
}

func exactJiraReference(body, key string) bool {
	escaped := regexp.QuoteMeta(key)
	patterns := []*regexp.Regexp{
		regexp.MustCompile(`(?i)^\s*` + escaped + `\s*[.]?\s*$`),
		regexp.MustCompile(`(?i)^\s*(?:jira|fixes|fix|resolves|closes):?\s+` + escaped + `\s*[.]?\s*$`),
		regexp.MustCompile(`(?i)^\s*(?:fixes|fix|resolves|closes)\s+\[` + escaped + `\]\([^\s)]+\)\s*[.]?\s*$`),
	}
	for line := range strings.SplitSeq(body, "\n") {
		for _, pattern := range patterns {
			if pattern.MatchString(line) {
				return true
			}
		}
	}

	return false
}

func summarizeChecks(content json.RawMessage) string {
	if len(content) == 0 || string(content) == "null" {
		return "UNKNOWN"
	}
	var checks []map[string]any
	if err := json.Unmarshal(content, &checks); err != nil || len(checks) == 0 {
		return "UNKNOWN"
	}
	result := "SUCCESS"
	for _, check := range checks {
		conclusion := strings.ToUpper(stringField(check, "conclusion"))
		status := strings.ToUpper(stringField(check, "status"))
		state := strings.ToUpper(stringField(check, "state"))
		if slices.Contains([]string{"FAILURE", "ERROR", "CANCELLED", "TIMED_OUT", "ACTION_REQUIRED"}, conclusion) ||
			slices.Contains([]string{"FAILURE", "ERROR"}, state) {
			return "FAILURE"
		}
		if conclusion == "" || slices.Contains([]string{"PENDING", "QUEUED", "IN_PROGRESS", "EXPECTED"}, status) ||
			slices.Contains([]string{"PENDING", "EXPECTED"}, state) {
			result = "PENDING"
		}
	}

	return result
}

func stringField(object map[string]any, key string) string {
	value, _ := object[key].(string)

	return value
}

func commandError(err error) error {
	var exitError *exec.ExitError
	if errors.As(err, &exitError) {
		message := strings.TrimSpace(string(exitError.Stderr))
		if message != "" {
			return errors.New(message)
		}
	}

	return err
}
