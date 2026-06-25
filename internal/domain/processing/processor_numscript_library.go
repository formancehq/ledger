package processing

import (
	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/pkg/semver"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

func (p *RequestProcessor) processSaveNumscript(ledger string, order *raftcmdpb.SaveNumscriptOrder, s Scope) (*commonpb.LogPayload, domain.Describable) {
	if err := domain.ValidateNumscriptName(order.GetName()); err != nil {
		return nil, err
	}

	if order.GetContent() == "" {
		return nil, domain.ErrNumscriptContentRequired
	}

	// Validate the script parses correctly (before ledger check, so syntax errors
	// are reported regardless of ledger existence).
	if _, err := p.numscriptCache.GetOrParse(order.GetContent()); err != nil {
		return nil, &domain.ErrNumscriptParse{Details: err.Error()}
	}

	ledgerInfo, loadErr := loadLedger(s, ledger)
	if loadErr != nil {
		return nil, loadErr
	}

	ledgerName := ledgerInfo.GetName()

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
		exists, err := s.NumscriptVersionExists(ledgerName, order.GetName(), version)
		if err != nil {
			return nil, &domain.ErrStorageOperation{Operation: "checking numscript version existence", Cause: err}
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
		CreatedAt: s.GetDate().Mutate(),
		Ledger:    ledger,
	}

	s.PutNumscript(ledgerName, info)

	return &commonpb.LogPayload{
		Type: &commonpb.LogPayload_SavedNumscript{
			SavedNumscript: &commonpb.SavedNumscriptLog{
				Info: info,
			},
		},
	}, nil
}

func (p *RequestProcessor) processDeleteNumscript(ledger string, order *raftcmdpb.DeleteNumscriptOrder, s Scope) (*commonpb.LogPayload, domain.Describable) {
	if err := domain.ValidateNumscriptName(order.GetName()); err != nil {
		return nil, err
	}

	ledgerInfo, loadErr := loadLedger(s, ledger)
	if loadErr != nil {
		return nil, loadErr
	}

	ledgerName := ledgerInfo.GetName()

	currentVersion, err := s.GetNumscriptLatestVersion(ledgerName, order.GetName())
	if err != nil {
		return nil, &domain.ErrStorageOperation{Operation: "getting numscript latest version", Cause: err}
	}

	if currentVersion == "" {
		return nil, &domain.ErrNumscriptNotFound{Name: order.GetName()}
	}

	s.DeleteNumscriptLatest(ledgerName, order.GetName())

	return &commonpb.LogPayload{
		Type: &commonpb.LogPayload_DeletedNumscript{
			DeletedNumscript: &commonpb.DeletedNumscriptLog{
				Name:   order.GetName(),
				Ledger: ledger,
			},
		},
	}, nil
}
