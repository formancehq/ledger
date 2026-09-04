package processing

import (
	"fmt"
	"maps"

	"github.com/antithesishq/antithesis-sdk-go/assert"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// skipSafeScope is a Scope decorator that fails loudly on any mutation
// whose effect the inner overlay does NOT buffer. It exists to make the
// atomicity contract of orderOverlayScope structural rather than
// discipline-based:
//
//	WriteSet → orderOverlayScope (buffers)  → skipSafeScope (enforces)  → sub-processor
//
// Every method of Scope is implemented explicitly here — NO embedding of
// Scope — so:
//
//   - A new method added to the Scope interface FAILS TO COMPILE against
//     skipSafeScope until it is explicitly classified as read-passthrough,
//     buffered-write-passthrough, or non-buffered-write-trap. There is no
//     silent-delegation escape hatch.
//   - A sub-processor that mutates a non-buffered surface (signing keys,
//     maintenance mode, numscript library, query
//     checkpoint state) trips trapUnbuffered — the mutation never reaches
//     the parent, so a subsequent skip cannot leak. Under Antithesis the
//     assert.Unreachable surfaces as a first-class finding; outside
//     Antithesis (the SDK's assert.Unreachable is a no-op) the paired
//     panic() kills the FSM apply goroutine loudly rather than dropping
//     the write silently and letting the sub-processor believe it landed.
//
// The decorator is only wrapped around the overlay when
// len(order.SkippableReasons) > 0 (see ProcessOrders): a non-skip-tolerant
// order pays no indirection cost.
type skipSafeScope struct {
	inner Scope
}

func newSkipSafeScope(inner Scope) *skipSafeScope {
	return &skipSafeScope{inner: inner}
}

// trapUnbuffered fires both the Antithesis assertion (first-class finding
// under a run) AND a panic (loud kill in every other environment). Every
// non-buffered mutator on skipSafeScope routes through this helper so the
// message format stays uniform and the two-tier enforcement is applied
// consistently. The panic MUST run after assert.Unreachable so the
// Antithesis finding is captured even on a run that later crashes.
//
// The assert.Unreachable message must stay a string literal: the
// antithesis-go-instrumentor catalogues assertions by statically resolving
// that argument, and anything else degrades to an anonymous catalog entry.
// The offending method therefore travels in the details map and in the
// panic value.
func trapUnbuffered(method string, details map[string]any) {
	d := make(map[string]any, len(details)+1)
	maps.Copy(d, details)
	d["method"] = method
	assert.Unreachable("skippable order attempted a non-buffered scope mutation", d)
	panic(fmt.Sprintf("skip_safe_scope: skippable order attempted %s — the overlay does not buffer this mutation, and letting it through would leak past a rollback", method))
}

// ──────────────────────────────────────────────────────────────────────────
// Accessors — every kind is buffered by the overlay; pass through so the
// staged accessor handles Get/Put/Delete against the per-order buffer.
// ──────────────────────────────────────────────────────────────────────────

func (s *skipSafeScope) Ledgers() Accessor[domain.LedgerKey, *commonpb.LedgerInfo, commonpb.LedgerInfoReader] {
	return s.inner.Ledgers()
}

func (s *skipSafeScope) Boundaries() Accessor[domain.LedgerKey, *raftcmdpb.LedgerBoundaries, raftcmdpb.LedgerBoundariesReader] {
	return s.inner.Boundaries()
}

func (s *skipSafeScope) Volumes() Accessor[domain.VolumeKey, *raftcmdpb.VolumePair, raftcmdpb.VolumePairReader] {
	return s.inner.Volumes()
}

func (s *skipSafeScope) AccountMetadata() Accessor[domain.MetadataKey, *commonpb.MetadataValue, commonpb.MetadataValueReader] {
	return s.inner.AccountMetadata()
}

func (s *skipSafeScope) LedgerMetadata() Accessor[domain.LedgerMetadataKey, *commonpb.MetadataValue, commonpb.MetadataValueReader] {
	return s.inner.LedgerMetadata()
}

func (s *skipSafeScope) TransactionReferences() Accessor[domain.TransactionReferenceKey, *commonpb.TransactionReferenceValue, commonpb.TransactionReferenceValueReader] {
	return s.inner.TransactionReferences()
}

func (s *skipSafeScope) TransactionStates() Accessor[domain.TransactionKey, *commonpb.TransactionState, commonpb.TransactionStateReader] {
	return s.inner.TransactionStates()
}

func (s *skipSafeScope) PreparedQueries() Accessor[domain.PreparedQueryKey, *commonpb.PreparedQuery, commonpb.PreparedQueryReader] {
	return s.inner.PreparedQueries()
}

func (s *skipSafeScope) Indexes() Accessor[domain.IndexKey, *commonpb.Index, commonpb.IndexReader] {
	return s.inner.Indexes()
}

// ──────────────────────────────────────────────────────────────────────────
// Reverted — buffered by the overlay; pass through.
// ──────────────────────────────────────────────────────────────────────────

func (s *skipSafeScope) GetReverted(key domain.TransactionKey) (bool, error) {
	return s.inner.GetReverted(key)
}

func (s *skipSafeScope) PutReverted(key domain.TransactionKey, reverted bool) {
	s.inner.PutReverted(key, reverted)
}

// ──────────────────────────────────────────────────────────────────────────
// Signing keys — writes are NOT buffered; reads pass through.
// ──────────────────────────────────────────────────────────────────────────

func (s *skipSafeScope) AddSigningKey(keyID string, publicKey []byte, parentKeyID string) {
	trapUnbuffered("AddSigningKey", map[string]any{"keyID": keyID})
}

func (s *skipSafeScope) RemoveSigningKey(keyID string) {
	trapUnbuffered("RemoveSigningKey", map[string]any{"keyID": keyID})
}

func (s *skipSafeScope) GetSigningKeyChildren(keyID string) []string {
	return s.inner.GetSigningKeyChildren(keyID)
}

func (s *skipSafeScope) SetRequireSignatures(require bool) {
	trapUnbuffered("SetRequireSignatures", map[string]any{"require": require})
}

// ──────────────────────────────────────────────────────────────────────────
// Maintenance mode — NOT buffered.
// ──────────────────────────────────────────────────────────────────────────

func (s *skipSafeScope) SetMaintenanceMode(enabled bool) {
	trapUnbuffered("SetMaintenanceMode", map[string]any{"enabled": enabled})
}

// ──────────────────────────────────────────────────────────────────────────
// Cluster policy — writes are NOT buffered; reads pass through.
// ──────────────────────────────────────────────────────────────────────────

func (s *skipSafeScope) GetClusterPolicy() *commonpb.ClusterPolicy {
	return s.inner.GetClusterPolicy()
}

func (s *skipSafeScope) SetClusterPolicy(policy *commonpb.ClusterPolicy) {
	trapUnbuffered("SetClusterPolicy", map[string]any{"revision": policy.GetRevision()})
}

// ──────────────────────────────────────────────────────────────────────────
// Sink read.
// ──────────────────────────────────────────────────────────────────────────

func (s *skipSafeScope) GetSinkConfig(name string) (commonpb.SinkConfigReader, error) {
	return s.inner.GetSinkConfig(name)
}

// ──────────────────────────────────────────────────────────────────────────
// Counters and timestamps — all buffered by the overlay (Increment*),
// or pure reads. Pass through.
// ──────────────────────────────────────────────────────────────────────────

func (s *skipSafeScope) GetNextSequenceID() uint64         { return s.inner.GetNextSequenceID() }
func (s *skipSafeScope) IncrementNextSequenceID() uint64   { return s.inner.IncrementNextSequenceID() }
func (s *skipSafeScope) GetNextAuditSequenceID() uint64    { return s.inner.GetNextAuditSequenceID() }
func (s *skipSafeScope) GetLastAuditHash() []byte          { return s.inner.GetLastAuditHash() }
func (s *skipSafeScope) GetNextLedgerID() uint32           { return s.inner.GetNextLedgerID() }
func (s *skipSafeScope) IncrementNextLedgerID() uint32     { return s.inner.IncrementNextLedgerID() }
func (s *skipSafeScope) GetDate() commonpb.TimestampReader { return s.inner.GetDate() }
func (s *skipSafeScope) GetRaftIndex() uint64              { return s.inner.GetRaftIndex() }

// ──────────────────────────────────────────────────────────────────────────
// Numscript library — reads pass through; Put/Set are NOT buffered.
// ──────────────────────────────────────────────────────────────────────────

func (s *skipSafeScope) GetNumscriptLatestVersion(ledgerName string, name string) (string, error) {
	return s.inner.GetNumscriptLatestVersion(ledgerName, name)
}

func (s *skipSafeScope) NumscriptVersionExists(ledgerName string, name, version string) (bool, error) {
	return s.inner.NumscriptVersionExists(ledgerName, name, version)
}

func (s *skipSafeScope) PutNumscript(ledgerName string, info *commonpb.NumscriptInfo) {
	trapUnbuffered("PutNumscript", map[string]any{"ledger": ledgerName})
}

func (s *skipSafeScope) SetNumscriptLatestVersion(ledgerName string, name, version string) {
	trapUnbuffered("SetNumscriptLatestVersion", map[string]any{"ledger": ledgerName, "name": name, "version": version})
}

func (s *skipSafeScope) ResolveNumscriptContent(ledgerName string, name, version string) (commonpb.NumscriptInfoReader, error) {
	return s.inner.ResolveNumscriptContent(ledgerName, name, version)
}

// ──────────────────────────────────────────────────────────────────────────
// Query checkpoints — GetNext/IncrementNext are buffered by the overlay
// (queryCheckpointDelta). Save/Delete are NOT buffered.
// ──────────────────────────────────────────────────────────────────────────

func (s *skipSafeScope) GetNextQueryCheckpointID() uint64 {
	return s.inner.GetNextQueryCheckpointID()
}

func (s *skipSafeScope) IncrementNextQueryCheckpointID() uint64 {
	return s.inner.IncrementNextQueryCheckpointID()
}

func (s *skipSafeScope) SaveQueryCheckpoint(cp *raftcmdpb.QueryCheckpointState) {
	trapUnbuffered("SaveQueryCheckpoint", nil)
}

func (s *skipSafeScope) DeleteQueryCheckpoint(checkpointID uint64) {
	trapUnbuffered("DeleteQueryCheckpoint", map[string]any{"checkpointID": checkpointID})
}

func (s *skipSafeScope) LiveQueryCheckpointCount() uint64 {
	return s.inner.LiveQueryCheckpointCount()
}

func (s *skipSafeScope) QueryCheckpointExists(id uint64) bool {
	return s.inner.QueryCheckpointExists(id)
}

// ──────────────────────────────────────────────────────────────────────────
// CheckCoverage — pure gate; pass through so declared-set enforcement
// still fires under a skip-tolerant order.
// ──────────────────────────────────────────────────────────────────────────

func (s *skipSafeScope) CheckCoverage(kind byte, key CoverageKey) error {
	return s.inner.CheckCoverage(kind, key)
}

// Compile-time assertion: skipSafeScope satisfies Scope. If Scope gains a
// new method, this line fails to build until the method is explicitly
// classified above — no silent delegation via embedding.
var _ Scope = (*skipSafeScope)(nil)
