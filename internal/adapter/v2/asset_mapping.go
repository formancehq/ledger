package v2

import (
	"fmt"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

const (
	// MaxAssetMappings bounds a mirror's asset_mappings list. The list lives
	// in LedgerInfo and in the audited CreatedLedgerLog, so it stays small.
	MaxAssetMappings = 256
	// MaxSourceAssetLength bounds a mapping's source asset. The v2 asset
	// grammar caps assets at 41 bytes; 64 leaves room without inviting abuse.
	MaxSourceAssetLength = 64
)

type assetTarget struct {
	asset string
	color string
}

// AssetMapper rewrites v2 posting assets into v3 (asset, color) buckets and
// validates every posting asset that reaches a mirror proposal. A nil mapper
// has no mappings but still validates.
type AssetMapper struct {
	bySource map[string]assetTarget
	// uncoloredTargets holds target assets whose color is empty: a
	// pass-through asset of the same name would land in the same bucket.
	uncoloredTargets map[string]string
}

// NewAssetMapper compiles and validates a mirror's asset mappings. It returns
// nil for an empty list.
func NewAssetMapper(mappings []*commonpb.MirrorAssetMapping) (*AssetMapper, error) {
	if len(mappings) == 0 {
		return nil, nil
	}

	if len(mappings) > MaxAssetMappings {
		return nil, fmt.Errorf("too many asset mappings: %d (max %d)", len(mappings), MaxAssetMappings)
	}

	m := &AssetMapper{
		bySource:         make(map[string]assetTarget, len(mappings)),
		uncoloredTargets: make(map[string]string),
	}
	targets := make(map[assetTarget]string, len(mappings))

	for i, mapping := range mappings {
		source := mapping.GetSourceAsset()
		if source == "" {
			return nil, fmt.Errorf("assetMappings[%d]: sourceAsset must not be empty", i)
		}

		if len(source) > MaxSourceAssetLength {
			return nil, fmt.Errorf("assetMappings[%d]: sourceAsset %q exceeds %d bytes", i, source, MaxSourceAssetLength)
		}

		if _, dup := m.bySource[source]; dup {
			return nil, fmt.Errorf("assetMappings[%d]: duplicate sourceAsset %q", i, source)
		}

		target := assetTarget{asset: mapping.GetAsset(), color: mapping.GetColor()}
		if err := domain.ValidateAsset(target.asset); err != nil {
			return nil, fmt.Errorf("assetMappings[%d]: asset %q: %w", i, target.asset, err)
		}

		if err := domain.ValidateColor(target.color); err != nil {
			return nil, fmt.Errorf("assetMappings[%d]: color %q: %w", i, target.color, err)
		}

		if prev, dup := targets[target]; dup {
			return nil, fmt.Errorf("assetMappings[%d]: target (%q, %q) already used by sourceAsset %q", i, target.asset, target.color, prev)
		}

		targets[target] = source
		m.bySource[source] = target

		if target.color == "" {
			m.uncoloredTargets[target.asset] = source
		}
	}

	return m, nil
}

// AssetMappingError reports a v2 posting asset that cannot be proposed.
type AssetMappingError struct {
	Asset  string
	Reason string
}

func (e *AssetMappingError) Error() string { return e.Reason }

// apply rewrites mapped assets in place and validates every resulting
// (asset, color). seen memoises pass-through assets already validated in
// this batch; callers share one map per batch.
func (m *AssetMapper) apply(postings []*commonpb.Posting, seen map[string]struct{}) error {
	for _, p := range postings {
		asset := p.GetAsset()

		if m != nil {
			if target, ok := m.bySource[asset]; ok {
				p.Asset = target.asset
				p.Color = target.color

				continue
			}

			if source, clash := m.uncoloredTargets[asset]; clash {
				return &AssetMappingError{
					Asset:  asset,
					Reason: fmt.Sprintf("pass-through asset %q collides with mapping target (%q, \"\") of sourceAsset %q; add an assetMappings entry for it", asset, asset, source),
				}
			}
		}

		if _, ok := seen[asset]; ok {
			continue
		}

		if err := domain.ValidateAsset(asset); err != nil {
			return &AssetMappingError{
				Asset:  asset,
				Reason: fmt.Sprintf("no mapping for v2 asset %q; add an assetMappings entry or drop the transaction with a rewrite rule", asset),
			}
		}

		if err := domain.ValidateColor(p.GetColor()); err != nil {
			return &AssetMappingError{Asset: asset, Reason: fmt.Sprintf("v2 asset %q: %s", asset, err.Error())}
		}

		seen[asset] = struct{}{}
	}

	return nil
}
