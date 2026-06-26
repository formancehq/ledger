package processing

import (
	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/domain/indexes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// Compile-time guarantee that Scope can be passed to indexes.Find/Put/Remove
// without an adapter — the cache façade IS the Lookup and IndexWriter for the
// FSM hot path.
var (
	_ indexes.Lookup      = (Scope)(nil)
	_ indexes.IndexWriter = (Scope)(nil)
)

//go:generate mockgen -write_source_comment=false -write_package_comment=false -source=store.go -destination=store_generated_test.go -typed -package=processing -mock_names=Scope=MockScope

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
// Cache-backed Get* methods return a Reader view over the cache entry so
// handlers cannot accidentally mutate cached state in place. Use
// Reader.Mutate() to obtain a writeable clone before modifying, then write
// the result back through the matching Put* method.
type Scope interface {
	// Ledger operations
	//
	// Get* methods that read cache-attribute keys return (nil,
	// domain.ErrNotFound) when the key is absent, (nil, *ErrCoverageMiss)
	// when the proposer did not declare the key in this scope's
	// coverage_bits (gatedScope only), or (reader, nil) on a hit.
	GetLedger(name string) (commonpb.LedgerInfoReader, error)
	PutLedger(name string, info *commonpb.LedgerInfo)

	// Boundaries operations
	GetBoundaries(ledger string) (raftcmdpb.LedgerBoundariesReader, error)
	PutBoundaries(ledger string, boundaries *raftcmdpb.LedgerBoundaries)

	// Volume operations (merged Input+Output)
	GetVolume(key domain.VolumeKey) (raftcmdpb.VolumePairReader, error)
	PutVolume(key domain.VolumeKey, value *raftcmdpb.VolumePair)

	// Account metadata operations
	GetAccountMetadata(key domain.MetadataKey) (commonpb.MetadataValueReader, error)
	PutAccountMetadata(key domain.MetadataKey, value *commonpb.MetadataValue)
	DeleteAccountMetadata(key domain.MetadataKey)

	// Ledger metadata operations
	GetLedgerMetadata(key domain.LedgerMetadataKey) (commonpb.MetadataValueReader, error)
	PutLedgerMetadata(key domain.LedgerMetadataKey, value *commonpb.MetadataValue)
	DeleteLedgerMetadata(key domain.LedgerMetadataKey)

	// Transaction reversion status operations
	GetReverted(key domain.TransactionKey) (bool, error)
	PutReverted(key domain.TransactionKey, reverted bool)

	// Idempotency key operations
	GetIdempotencyKey(key domain.IdempotencyKey) (commonpb.IdempotencyKeyValueReader, error)
	PutIdempotencyKey(key domain.IdempotencyKey, value *commonpb.IdempotencyKeyValue)

	// Transaction reference operations
	GetTransactionReference(key domain.TransactionReferenceKey) (commonpb.TransactionReferenceValueReader, error)
	PutTransactionReference(key domain.TransactionReferenceKey, value *commonpb.TransactionReferenceValue)

	// Transaction state operations
	GetTransactionState(key domain.TransactionKey) (commonpb.TransactionStateReader, error)
	PutTransactionState(key domain.TransactionKey, state *commonpb.TransactionState)

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

	// Prepared query operations
	GetPreparedQuery(ledgerName string, name string) (commonpb.PreparedQueryReader, error)
	PutPreparedQuery(ledgerName string, pq *commonpb.PreparedQuery)
	DeletePreparedQuery(ledgerName string, name string)

	// Numscript library operations
	GetNumscriptLatestVersion(ledgerName string, name string) (string, error)
	NumscriptVersionExists(ledgerName string, name, version string) (bool, error)
	PutNumscript(ledgerName string, info *commonpb.NumscriptInfo)
	DeleteNumscriptLatest(ledgerName string, name string)

	// Query checkpoint operations
	GetNextQueryCheckpointID() uint64
	IncrementNextQueryCheckpointID() uint64
	SaveQueryCheckpoint(cp *raftcmdpb.QueryCheckpointState)
	DeleteQueryCheckpoint(checkpointID uint64)

	// Index registry operations (bucket-scoped, keyed by IndexKey{LedgerID, Canonical}).
	// LedgerID == 0 reserves the slot for bucket-scoped indexes (audit).
	//
	// GetIndex returns:
	//   - (idx, nil)              when the entry is present and the proposer declared the key.
	//   - (nil, domain.ErrNotFound) when the entry is legitimately absent (deleted/never created).
	//   - (nil, *ErrCoverageMiss) (gatedScope only) when the proposer's coverage_bits don't
	//     flag this key — the apply path bubbles the error up as a business rejection so a
	//     stale/malformed plan can't read past the gate.
	GetIndex(key domain.IndexKey) (commonpb.IndexReader, error)
	PutIndex(key domain.IndexKey, idx *commonpb.Index)
	DeleteIndex(key domain.IndexKey)

	// Numscript content resolution
	ResolveNumscriptContent(ledgerName string, name, version string) (commonpb.NumscriptInfoReader, error)

	// CheckCoverage exposes the gate for paths that read state directly
	// (bypassing the engine overlay) and still want the coverage
	// invariant enforced. Used by ValidateTransientVolumes which reads
	// Derived.Volumes.Parent() directly to fetch the pre-batch base
	// volume. No-op on the bare *WriteSet implementation.
	CheckCoverage(kind byte, canonical []byte) error
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
// AttributePlan slice, an unknown attr_code is declared). This is
// surfaced as a business-level rejection: detection happens BEFORE any
// cache mutation so the in-memory state stays in lockstep with Pebble.
type ScopeFactory interface {
	// NewScope returns a per-order or per-TU scope narrowed by the
	// caller's coverage_bits. nil or empty bits admits no plan —
	// every CheckCoverage call will miss. Callers that need a
	// proposal-wide scope (admit every declared AttributePlan, e.g.
	// for ValidateTransientVolumes) must use NewProposalScope.
	NewScope(coverageBits []byte) (Scope, error)

	// NewProposalScope returns a scope that admits every AttributePlan
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
