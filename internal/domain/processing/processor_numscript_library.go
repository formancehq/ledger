package processing

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/formancehq/ledger-v3-poc/internal/domain"
	"github.com/formancehq/ledger-v3-poc/internal/domain/processing/numscript"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
)

// isValidSemver validates a version string is "major.minor.patch" format.
// The special value "latest" is NOT considered valid semver (handled separately).
func isValidSemver(s string) bool {
	parts := strings.Split(s, ".")
	if len(parts) != 3 {
		return false
	}
	for _, part := range parts {
		if part == "" {
			return false
		}
		if _, err := strconv.Atoi(part); err != nil {
			return false
		}
	}
	return true
}

func (p *RequestProcessor) processSaveNumscript(order *raftcmdpb.SaveNumscriptOrder, s InMemoryStore) (*commonpb.LogPayload, error) {
	if order.Name == "" {
		return nil, domain.ErrNumscriptNameRequired
	}
	if order.Content == "" {
		return nil, domain.ErrNumscriptContentRequired
	}

	// Validate the script parses correctly
	if _, err := p.numscriptCache.GetOrParse(order.Content); err != nil {
		return nil, &numscript.ErrNumscriptParse{Details: err.Error()}
	}

	version := order.Version
	if version == "" {
		// Default: treat as "latest"
		version = "latest"
	}

	var resolvedVersion string

	if version == "latest" {
		// "latest" is its own version slot, always overwritable
		resolvedVersion = "latest"
	} else if isValidSemver(version) {
		// Semver versions are immutable — check the specific version doesn't already exist
		exists, err := s.NumscriptVersionExists(order.Name, version)
		if err != nil {
			return nil, fmt.Errorf("checking numscript version existence: %w", err)
		}
		if exists {
			return nil, &domain.ErrNumscriptVersionAlreadyExists{Name: order.Name, Version: version}
		}
		resolvedVersion = version
	} else {
		return nil, &domain.ErrNumscriptInvalidVersion{Version: version}
	}

	info := &commonpb.NumscriptInfo{
		Name:      order.Name,
		Content:   order.Content,
		Version:   resolvedVersion,
		CreatedAt: s.GetDate(),
	}

	s.PutNumscript(info)

	return &commonpb.LogPayload{
		Type: &commonpb.LogPayload_SavedNumscript{
			SavedNumscript: &commonpb.SavedNumscriptLog{
				Info: info,
			},
		},
	}, nil
}

func (p *RequestProcessor) processDeleteNumscript(order *raftcmdpb.DeleteNumscriptOrder, s InMemoryStore) (*commonpb.LogPayload, error) {
	if order.Name == "" {
		return nil, domain.ErrNumscriptNameRequired
	}

	currentVersion, err := s.GetNumscriptLatestVersion(order.Name)
	if err != nil {
		return nil, fmt.Errorf("getting numscript latest version: %w", err)
	}
	if currentVersion == "" {
		return nil, &domain.ErrNumscriptNotFound{Name: order.Name}
	}

	s.DeleteNumscriptLatest(order.Name)

	return &commonpb.LogPayload{
		Type: &commonpb.LogPayload_DeletedNumscript{
			DeletedNumscript: &commonpb.DeletedNumscriptLog{
				Name: order.Name,
			},
		},
	}, nil
}
