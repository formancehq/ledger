package processing

import (
	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/pkg/semver"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// processSaveNumscript appends an immutable version to the library. The version
// must be an explicit full semver; content entries are immutable, and the
// per-name latest pointer is maintained as the greatest stored semver.
func processSaveNumscript(ledger string, order *raftcmdpb.SaveNumscriptOrder, ctx *Context) (*commonpb.LogPayload, domain.Describable) {
	s := ctx.Scope

	if err := domain.ValidateNumscriptName(order.GetName()); err != nil {
		return nil, err
	}

	if order.GetContent() == "" {
		return nil, domain.ErrNumscriptContentRequired
	}

	// Validate the script parses correctly (before ledger check, so syntax errors
	// are reported regardless of ledger existence).
	if _, err := ctx.NumscriptCache.GetOrParse(order.GetContent()); err != nil {
		return nil, &domain.ErrNumscriptParse{Details: err.Error()}
	}

	// Save requires an explicit full semver — no "latest", no partial selectors.
	version, err := semver.Parse(order.GetVersion())
	if err != nil {
		return nil, &domain.ErrNumscriptInvalidVersion{Version: order.GetVersion()}
	}

	if _, loadErr := loadLedger(s, ledger); loadErr != nil {
		return nil, loadErr
	}

	// Immutable: a stored version can never be overwritten.
	exists, existsErr := s.NumscriptVersionExists(ledger, order.GetName(), order.GetVersion())
	if existsErr != nil {
		return nil, &domain.ErrStorageOperation{Operation: "checking numscript version existence", Cause: existsErr}
	}

	if exists {
		return nil, &domain.ErrNumscriptVersionAlreadyExists{Name: order.GetName(), Version: order.GetVersion()}
	}

	// Read the current greatest BEFORE PutNumscript overwrites the pointer, so
	// the max comparison sees the pre-existing pointer rather than the value we
	// are about to write.
	current, curErr := s.GetNumscriptLatestVersion(ledger, order.GetName())
	if curErr != nil {
		return nil, &domain.ErrStorageOperation{Operation: "getting numscript latest version", Cause: curErr}
	}

	info := &commonpb.NumscriptInfo{
		Name:      order.GetName(),
		Content:   order.GetContent(),
		Version:   order.GetVersion(),
		CreatedAt: s.GetDate().Mutate(),
		Ledger:    ledger,
	}

	// Write the immutable content and advance the latest pointer to the greatest
	// stored semver. Any unused semver may be added out of order, so the pointer
	// is max(current, new): PutNumscript sets it to the new version, then we
	// restore a greater pre-existing pointer if one existed.
	s.PutNumscript(ledger, info)

	if current != "" {
		if cur, parseErr := semver.Parse(current); parseErr == nil && cur.Compare(version) > 0 {
			s.SetNumscriptLatestVersion(ledger, order.GetName(), current)
		}
	}

	return &commonpb.LogPayload{
		Type: &commonpb.LogPayload_SavedNumscript{
			SavedNumscript: &commonpb.SavedNumscriptLog{
				Info: info,
			},
		},
	}, nil
}
