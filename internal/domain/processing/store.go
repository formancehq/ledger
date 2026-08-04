package processing

import (
	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

//go:generate go tool mockgen -write_source_comment=false -write_package_comment=false -source=store.go -destination=store_generated_test.go -typed -package=processing -mock_names=Scope=MockScope

// OrderTagger is an optional capability a Scope implementation may expose to
// receive the zero-based order index before its handler runs. ProcessOrders
// invokes BeginOrder when the scope supports it so the underlying WriteSet
// can attribute volume touches to the order that produced them (used to
// build the per-log purged_volumes list at Merge time). Recovery scopes,
// technical-update scopes, and test mocks intentionally skip this — they
// don't need the per-log accounting.
type OrderTagger interface {
	BeginOrder(orderIndex int)
}

// Scope is the FSM-apply read/write facade — the only surface order
// handlers and technical-update handlers should touch. Two
// implementations:
//
//   - *state.gatedScope (production): every cache-attribute read passes
//     through CheckCoverage before reaching the engine; the coverage map
//     is immutable for the lifetime of the scope.
//   - *state.WriteSet (recovery/tests): bare engine, no coverage gate —
//     CheckCoverage and ResolveProductions are no-ops.
//
// The 9 gated cache-attribute kinds are exposed via per-kind Accessor
// methods (Ledgers, Boundaries, Volumes, AccountMetadata, LedgerMetadata,
// TransactionReferences, TransactionStates, PreparedQueries, Indexes).
// Each accessor's Get returns a Reader view; call Reader.Mutate() to
// obtain a writeable clone before writing back through Put.
//
// IdempotencyKeys is also accessor-shaped but bypasses the coverage gate
// (idempotency lookups are not part of per-order coverage planning) and
// applies TTL filtering on read.
//
// The remaining kinds (sink configs, numscript versions/contents,
// reverted bitset) and the FSM-primitive surface (counters, chapters,
// signing keys, maintenance mode, dates) stay as discrete methods because
// their value/key shapes do not match the generic Accessor trio.
type Scope interface {
	// Per-kind accessors — Get returns a Reader, Put records a write,
	// Delete records a tombstone. All four operate against the in-batch
	// overlay (attributes.DerivedKeyStore); reads fall through to the
	// parent registry on overlay miss.
	Ledgers() Accessor[domain.LedgerKey, *commonpb.LedgerInfo, commonpb.LedgerInfoReader]
	Boundaries() Accessor[domain.LedgerKey, *raftcmdpb.LedgerBoundaries, raftcmdpb.LedgerBoundariesReader]
	Volumes() Accessor[domain.VolumeKey, *raftcmdpb.VolumePair, raftcmdpb.VolumePairReader]
	AccountMetadata() Accessor[domain.MetadataKey, *commonpb.MetadataValue, commonpb.MetadataValueReader]
	LedgerMetadata() Accessor[domain.LedgerMetadataKey, *commonpb.MetadataValue, commonpb.MetadataValueReader]
	TransactionReferences() Accessor[domain.TransactionReferenceKey, *commonpb.TransactionReferenceValue, commonpb.TransactionReferenceValueReader]
	TransactionStates() Accessor[domain.TransactionKey, *commonpb.TransactionState, commonpb.TransactionStateReader]
	PreparedQueries() Accessor[domain.PreparedQueryKey, *commonpb.PreparedQuery, commonpb.PreparedQueryReader]
	Indexes() Accessor[domain.IndexKey, *commonpb.Index, commonpb.IndexReader]

	// Transaction reversion status — bool-valued, no Reader, kept discrete.
	GetReverted(key domain.TransactionKey) (bool, error)
	PutReverted(key domain.TransactionKey, reverted bool)

	// Signing key operations
	AddSigningKey(keyID string, publicKey []byte, parentKeyID string)
	RemoveSigningKey(keyID string)
	GetSigningKeyChildren(keyID string) []string
	SetRequireSignatures(require bool)

	// Maintenance mode operations
	SetMaintenanceMode(enabled bool)

	// Events sink reads (writes moved to the WriteSet sink via Absorb).
	GetSinkConfig(name string) (commonpb.SinkConfigReader, error)

	// Counters and timestamps
	GetNextSequenceID() uint64
	IncrementNextSequenceID() uint64
	GetNextAuditSequenceID() uint64
	GetNextLedgerID() uint32
	IncrementNextLedgerID() uint32
	GetDate() commonpb.TimestampReader

	// Chapter operations
	GetCurrentOpenChapter() (commonpb.ChapterReader, bool)
	GetClosingChapters() []commonpb.ChapterReader
	GetClosingChapterByID(chapterID uint64) (commonpb.ChapterReader, bool)
	SetCurrentOpenChapter(chapter *commonpb.Chapter)
	AddClosingChapter(chapter *commonpb.Chapter)
	RemoveClosingChapter(chapterID uint64)
	GetNextChapterID() uint64
	IncrementNextChapterID() uint64

	// Archive chapter operations (purge range / archive request moved to the WriteSet sink via Absorb).
	GetChapterByID(chapterID uint64) (commonpb.ChapterReader, bool)
	UpdateChapter(chapter *commonpb.Chapter)

	// Numscript library operations — heterogeneous shapes (string/bool returns,
	// multi-arg keys). Kept discrete; the Accessor trio does not fit.
	GetNumscriptLatestVersion(ledgerName string, name string) (string, error)
	NumscriptVersionExists(ledgerName string, name, version string) (bool, error)
	PutNumscript(ledgerName string, info *commonpb.NumscriptInfo)
	SetNumscriptLatestVersion(ledgerName string, name, version string)
	ResolveNumscriptContent(ledgerName string, name, version string) (commonpb.NumscriptInfoReader, error)

	// Query checkpoint operations
	GetNextQueryCheckpointID() uint64
	IncrementNextQueryCheckpointID() uint64
	SaveQueryCheckpoint(cp *raftcmdpb.QueryCheckpointState)
	DeleteQueryCheckpoint(checkpointID uint64)

	// CheckCoverage exposes the gate for paths that read state directly
	// (bypassing the engine overlay) and still want the coverage
	// invariant enforced. Used by ValidateTransientVolumes which reads
	// Derived.Volumes.Parent() directly to fetch the pre-batch base
	// volume. No-op on the bare *WriteSet implementation.
	//
	// The key is passed as a CoverageKey (something that knows how to
	// append its canonical bytes to a scratch buffer) rather than a
	// pre-built []byte. This lets the gate reuse a single scratch buffer
	// across every call in a proposal — key.Bytes() would allocate a
	// fresh slice on every gated read (100 allocs / 50-tx proposal on
	// the world-to-bank benchmark).
	CheckCoverage(kind byte, key CoverageKey) error
}

// CoverageKey is the minimal shape a canonical key type must expose to
// participate in the coverage gate: append its canonical bytes to a
// caller-provided buffer. Every domain.*Key satisfies it via AppendBytes.
type CoverageKey interface {
	AppendBytes(dst []byte) []byte
}

// ScopeFactory builds a per-order Scope from the order's coverage_bits.
// Each call returns an independent Scope with its own immutable coverage
// map; successive calls do not mutate previously returned scopes.
// ProcessOrders invokes the factory once per order so per-order
// isolation is structural — order N's scope cannot reach keys declared
// by order M because the two coverage maps were built from different
// bits over the same ExecutionPlan.
//
// An interface (not a func) so production implementations can carry
// per-proposal state (ExecutionPlan, Resolver, logger, miss counter)
// without each call site re-binding it through a closure — and so test
// doubles can be substituted via the standard mockgen path. The mock
// is generated by the file-level mockgen directive above.
//
// NewScope returns an error when the ExecutionPlan / bits combination
// is structurally inconsistent (e.g. a bit indexes past the
// AttributeCoverage slice, an unknown attr_code is declared). This is
// surfaced as a business-level rejection: detection happens BEFORE any
// cache mutation so the in-memory state stays in lockstep with Pebble.
type ScopeFactory interface {
	// NewScope returns a per-order or per-TU scope narrowed by the
	// caller's coverage_bits. nil or empty bits admits no plan —
	// every CheckCoverage call will miss. Callers that need a
	// proposal-wide scope (admit every declared AttributeCoverage, e.g.
	// for ValidateTransientVolumes) must use NewProposalScope.
	NewScope(coverageBits []byte) (Scope, error)

	// NewProposalScope returns a scope that admits every AttributeCoverage
	// the proposal declared. Distinct from NewScope(nil) so a per-order
	// caller passing empty bits (no declared needs) does not silently
	// inherit coverage from other orders' plans in the same proposal.
	NewProposalScope() (Scope, error)
}

// SignalSink absorbs the per-order (order, log) pair right after a
// processor returns its log. The sink interprets the log payload itself
// and applies whatever cross-order accumulator the framework needs
// (archive queue, purge ranges, sink-config tracking, lifecycle flags
// consumed by applyProposal, …). Processors NEVER receive a
// SignalSink — only ProcessOrders holds the reference. Folding the
// "what to signal" decision into log production guarantees the two
// can never desync.
//
// One method, one dispatch site: the log is the source of truth (it is
// what the audit chain hashes), and signals are strictly derivative.
// Maintaining a separate signal vocabulary would duplicate the log
// schema; the implementer's type switch on log.GetPayload() is the
// authoritative mapping.
//
// On the hot path this avoids both the per-signal allocation of a sum
// type and the 13 virtual calls of a per-signal interface — Absorb
// passes pointers and dispatches once.
type SignalSink interface {
	Absorb(order *raftcmdpb.Order, log *commonpb.Log)
}
