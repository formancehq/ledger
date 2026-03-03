package processing

import (
	"fmt"

	"github.com/formancehq/ledger-v3-poc/internal/domain"
	"github.com/formancehq/ledger-v3-poc/internal/pkg/semver"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
)

func (p *RequestProcessor) processSaveNumscript(order *raftcmdpb.SaveNumscriptOrder, s InMemoryStore) (*commonpb.LogPayload, error) {
	if order.GetName() == "" {
		return nil, domain.ErrNumscriptNameRequired
	}

	if order.GetContent() == "" {
		return nil, domain.ErrNumscriptContentRequired
	}

	// Validate the script parses correctly
	if _, err := p.numscriptCache.GetOrParse(order.GetContent()); err != nil {
		return nil, &domain.ErrNumscriptParse{Details: err.Error()}
	}

	version := order.GetVersion()
	if version == "" {
		// Default: treat as "latest"
		version = "latest"
	}

	var resolvedVersion string

	if version == "latest" {
		// "latest" is its own version slot, always overwritable
		resolvedVersion = "latest"
	} else if _, err := semver.Parse(version); err == nil {
		// Semver versions are immutable — check the specific version doesn't already exist
		exists, err := s.NumscriptVersionExists(order.GetLedger(), order.GetName(), version)
		if err != nil {
			return nil, fmt.Errorf("checking numscript version existence: %w", err)
		}

		if exists {
			return nil, &domain.ErrNumscriptVersionAlreadyExists{Name: order.GetName(), Version: version}
		}

		resolvedVersion = version
	} else {
		return nil, &domain.ErrNumscriptInvalidVersion{Version: version}
	}

	info := &commonpb.NumscriptInfo{
		Name:      order.GetName(),
		Content:   order.GetContent(),
		Version:   resolvedVersion,
		CreatedAt: s.GetDate(),
		Ledger:    order.Ledger,
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
	if order.GetName() == "" {
		return nil, domain.ErrNumscriptNameRequired
	}

	currentVersion, err := s.GetNumscriptLatestVersion(order.GetLedger(), order.GetName())
	if err != nil {
		return nil, fmt.Errorf("getting numscript latest version: %w", err)
	}

	if currentVersion == "" {
		return nil, &domain.ErrNumscriptNotFound{Name: order.GetName()}
	}

	s.DeleteNumscriptLatest(order.GetLedger(), order.GetName())

	return &commonpb.LogPayload{
		Type: &commonpb.LogPayload_DeletedNumscript{
			DeletedNumscript: &commonpb.DeletedNumscriptLog{
				Name:   order.GetName(),
				Ledger: order.GetLedger(),
			},
		},
	}, nil
}
