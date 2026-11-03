package check

import (
	"fmt"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/pkg/semver"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// numscriptProjection owns the expected library content and latest pointers for
// one checker run. Its input is the replay loop's selected log, after any
// audit-derived substitution; stored library rows are only comparison targets.
type numscriptProjection struct {
	content map[domain.NumscriptEntryKey]*commonpb.NumscriptInfo
	latest  map[domain.NumscriptVersionKey]string
}

func newNumscriptProjection() *numscriptProjection {
	return &numscriptProjection{
		content: make(map[domain.NumscriptEntryKey]*commonpb.NumscriptInfo),
		latest:  make(map[domain.NumscriptVersionKey]string),
	}
}

func (p *numscriptProjection) observe(log *commonpb.Log) {
	switch payload := log.GetPayload().GetType().(type) {
	case *commonpb.LogPayload_SavedNumscript:
		// Saves may arrive out of version order. Content is immutable; latest
		// advances only when the saved version is greater.
		if info := payload.SavedNumscript.GetInfo(); info != nil {
			ck := domain.NumscriptEntryKey{LedgerName: info.GetLedger(), Name: info.GetName(), Version: info.GetVersion()}
			p.content[ck] = info
			vk := domain.NumscriptVersionKey{LedgerName: info.GetLedger(), Name: info.GetName()}
			if cur, ok := p.latest[vk]; !ok || numscriptVersionGreater(info.GetVersion(), cur) {
				p.latest[vk] = info.GetVersion()
			}
		}
	case *commonpb.LogPayload_DeleteLedger:
		if payload.DeleteLedger != nil {
			// DeleteLedger purges both library keyspaces in the same apply.
			name := payload.DeleteLedger.GetName()
			for key := range p.content {
				if key.LedgerName == name {
					delete(p.content, key)
				}
			}
			for key := range p.latest {
				if key.LedgerName == name {
					delete(p.latest, key)
				}
			}
		}
	}
}

// compare verifies the numscript library projections against the
// state re-derived from the audit/log chain. SubAttrNumscriptContent holds the
// immutable per-version entries; SubAttrNumscriptVersion holds the per-name
// latest pointer (the greatest stored semver). Both are load-bearing for reads,
// listing, version history, and admission-side reference resolution, so a
// stored content row with no matching SavedNumscript, an altered content, or a
// latest pointer that is not the greatest saved semver is tampering: a stored
// row the replay never produced is a surplus/injected row and is flagged.
func (p *numscriptProjection) compare(
	reader dal.PebbleReader,
	attrs *attributes.Attributes,
	deletedInReplay map[string]struct{},
	callback func(*servicepb.CheckStoreEvent),
) {
	expectedContent, expectedLatest := p.content, p.latest
	mismatch := func(msg, ledger string) {
		callback(errorEvent(servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_NUMSCRIPT_MISMATCH, msg, 0, ledger, "", ""))
	}

	// Content entries (immutable): every stored row must trace to a SavedNumscript.
	contentIter, err := attrs.NumscriptContent.NewStreamingIter(reader, nil)
	if err != nil {
		mismatch(fmt.Sprintf("opening numscript content iterator: %v", err), "")

		return
	}

	seenContent := make(map[domain.NumscriptEntryKey]struct{}, len(expectedContent))

	for contentIter.Next() {
		entry := contentIter.Entry()
		if entry.Value == nil {
			continue
		}

		var key domain.NumscriptEntryKey
		if err := key.Unmarshal(entry.CanonicalKey); err != nil {
			mismatch(fmt.Sprintf("stored numscript content has unparsable canonical key %x: %v", entry.CanonicalKey, err), "")

			continue
		}

		seenContent[key] = struct{}{}

		exp, ok := expectedContent[key]
		if !ok {
			if _, deleted := deletedInReplay[key.LedgerName]; deleted {
				mismatch(fmt.Sprintf("stored numscript content %q@%q for ledger %q survives a replayed DeleteLedger + completed cleanup", key.Name, key.Version, key.LedgerName), key.LedgerName)

				continue
			}

			mismatch(fmt.Sprintf("stored numscript content %q@%q for ledger %q has no matching SavedNumscript in the audit chain", key.Name, key.Version, key.LedgerName), key.LedgerName)

			continue
		}

		// Compare the whole NumscriptInfo, not just Content: reads and listing
		// serve every field (Name, Version, Ledger, CreatedAt) straight from
		// this projection, so each must match the audit-derived value.
		if !entry.Value.EqualVT(exp) {
			mismatch(fmt.Sprintf("numscript %q@%q for ledger %q: stored entry diverges from the audit-derived NumscriptInfo", key.Name, key.Version, key.LedgerName), key.LedgerName)
		}
	}

	if err := contentIter.Err(); err != nil {
		mismatch(fmt.Sprintf("scanning numscript content: %v", err), "")
	}

	_ = contentIter.Close() // Best-effort cleanup after checking the iterator's scan error.

	for key := range expectedContent {
		if _, ok := seenContent[key]; !ok {
			mismatch(fmt.Sprintf("audit chain expects numscript content %q@%q for ledger %q but the store has no matching entry", key.Name, key.Version, key.LedgerName), key.LedgerName)
		}
	}

	// Latest pointers: the stored version string must match the greatest
	// saved semver derived from the SavedNumscript sequence.
	versionIter, err := attrs.NumscriptVersion.NewStreamingIter(reader, nil)
	if err != nil {
		mismatch(fmt.Sprintf("opening numscript version iterator: %v", err), "")

		return
	}

	seenLatest := make(map[domain.NumscriptVersionKey]struct{}, len(expectedLatest))

	for versionIter.Next() {
		entry := versionIter.Entry()
		if entry.Value == nil {
			continue
		}

		var key domain.NumscriptVersionKey
		if err := key.Unmarshal(entry.CanonicalKey); err != nil {
			mismatch(fmt.Sprintf("stored numscript version pointer has unparsable canonical key %x: %v", entry.CanonicalKey, err), "")

			continue
		}

		seenLatest[key] = struct{}{}

		exp, ok := expectedLatest[key]
		if !ok {
			if _, deleted := deletedInReplay[key.LedgerName]; deleted {
				mismatch(fmt.Sprintf("stored numscript latest pointer for %q on ledger %q survives a replayed DeleteLedger + completed cleanup", key.Name, key.LedgerName), key.LedgerName)

				continue
			}

			mismatch(fmt.Sprintf("stored numscript latest pointer for %q on ledger %q has no matching SavedNumscript in the audit chain", key.Name, key.LedgerName), key.LedgerName)

			continue
		}

		if entry.Value.GetVersion() != exp {
			mismatch(fmt.Sprintf("numscript %q on ledger %q: stored latest pointer %q diverges from the audit-derived %q", key.Name, key.LedgerName, entry.Value.GetVersion(), exp), key.LedgerName)
		}
	}

	if err := versionIter.Err(); err != nil {
		mismatch(fmt.Sprintf("scanning numscript version pointers: %v", err), "")
	}

	_ = versionIter.Close() // Best-effort cleanup after checking the iterator's scan error.

	for key := range expectedLatest {
		if _, ok := seenLatest[key]; !ok {
			mismatch(fmt.Sprintf("audit chain expects a numscript latest pointer for %q on ledger %q but the store has no matching entry", key.Name, key.LedgerName), key.LedgerName)
		}
	}
}

// numscriptVersionGreater reports whether a is a strictly greater full semver
// than b. A non-semver b is treated as smaller so a valid version always wins.
func numscriptVersionGreater(a, b string) bool {
	av, aerr := semver.Parse(a)
	bv, berr := semver.Parse(b)
	if aerr != nil {
		return false
	}

	if berr != nil {
		return true
	}

	return av.Compare(bv) > 0
}
