# Audit Secondary Index Implementation Plan (EN-1339)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Maintain a persisted, per-replica secondary index over the Audit zone that supports seek-by-field (equality / match-any / range), built by an async worker, with no change to the FSM hot path or the checker.

**Architecture:** A new `auditindexer.Indexer` worker runs on every node, reads `AuditEntry` + `AuditItem` from the main `dal.Store` (read-only, off the hot path), and writes composite index keys into the existing per-replica `readstore` Pebble under the non-ledger-scoped `PrefixInternal` namespace. A local cursor (`last_indexed_audit_sequence`) makes it resumable; rebuild = drop the keyspace + reset the cursor.

**Tech Stack:** Go, Pebble (`cockroachdb/pebble/v2`), `dal.KeyBuilder`, `readstore.Store`, `worker.Worker`, uber/fx, cobra (ledgerctl), OTEL metrics. Spec: `docs/superpowers/specs/2026-06-29-audit-secondary-index-design.md`.

**Key facts (verified against the codebase):**
- `readstore` is a separate Pebble DB; `readStore.NewBatch()` returns a `*dal.WriteSession` on it, committed via `batch.Commit()`. Writing here is NOT subject to invariant #4 (which governs the main store's `OpenWriteSession`).
- `ReadStoreComparer.Split` (`internal/storage/readstore/comparer.go:123`) returns the full key for any key whose first byte is `PrefixInternal` (`0xFE`). Namespacing the audit index under `PrefixInternal` therefore needs no comparer change.
- `AuditEntry` (`internal/proto/auditpb/audit.pb.go`): `Sequence uint64`, `Timestamp *commonpb.Timestamp`, `ProposalId uint64`, `Outcome` oneof (`*AuditEntry_Success` / `*AuditEntry_Failure`), `Ledgers []string`, `CallerSnapshot *commonpb.CallerSnapshot`.
- `CallerSnapshot.Identity *commonpb.CallerIdentity`; `CallerIdentity.Subject string`.
- `AuditItem`: `OrderIndex uint32`, `SerializedOrder []byte` (a marshaled `raftcmdpb.Order`), `LogSequence uint64`.
- Read helpers already exist: `query.ReadLastAuditSequence(reader)`, `query.ReadAuditEntries(ctx, reader, *afterSeq)`, `query.ReadAuditItems(ctx, reader, seq)` (`internal/query/audit.go`).
- `dal.KeyBuilder`: `PutByte`, `PutUint64` (BE), `PutString`, `PutStringNull` (string + `0x00`), `Build`, `Consume`, `Reset` (`internal/storage/dal/key_builder.go`).
- Existing readstore internal subs (`internal/storage/readstore/keys.go`): `SubInternalProgress=0x01`, `SubInternalAppliedProposalProgress=0x02`, `SubInternalBackfill=0x03`, `SubInternalIndexVersion=0x04`.

---

## File Structure

**Create:**
- `internal/domain/audit_order_type.go` — `AuditOrderType(*raftcmdpb.Order) string`: the single canonical order-type token vocabulary (shared with EN-1305).
- `internal/domain/audit_order_type_test.go`
- `internal/application/auditindexer/keys.go` — wait, keys live in readstore; see below.
- `internal/application/auditindexer/indexer.go` — the `Indexer` worker (struct, `New`, `Start`/`Stop`, `loop`, cursor, catch-up, boot rebuild, metrics).
- `internal/application/auditindexer/index_entry.go` — `appendEntryKeys(kb, batch, entry, items)`: derive + emit every field key for one `AuditEntry`.
- `internal/application/auditindexer/index_entry_test.go`
- `internal/application/auditindexer/indexer_test.go` — integration tests.
- `cmd/ledgerctl/store/rebuild_audit_index.go` — offline rebuild command.

**Modify:**
- `internal/storage/readstore/keys.go` — add `SubInternalAuditIndex=0x05`, `SubInternalAuditProgress=0x06`, the `AuditField` byte enum, and key/prefix builders.
- `internal/storage/readstore/store.go` — add `ReadAuditProgress`, `WriteAuditProgress`, `DropAuditIndex`, and the three seek helpers.
- `internal/storage/readstore/audit_index_test.go` (create) — comparer + encoding + seek tests.
- `internal/bootstrap/config.go` — add audit-index config fields.
- `cmd/server/server.go` — add flags + map into `Config`.
- `internal/bootstrap/module.go` — provide `*auditindexer.Indexer` + lifecycle hook.
- `cmd/ledgerctl/store/<root command file>` — register the new subcommand.

---

## Task 1: Readstore key layout for the audit index

**Files:**
- Modify: `internal/storage/readstore/keys.go`
- Test: `internal/storage/readstore/audit_index_test.go` (create)

- [ ] **Step 1: Write the failing test** (encoding + comparer-safety)

Create `internal/storage/readstore/audit_index_test.go`:

```go
package readstore

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

func TestAuditIndexKeysAreInternalNamespaced(t *testing.T) {
	t.Parallel()

	kb := dal.NewKeyBuilder()
	key := AuditIndexStringKey(kb, AuditFieldLedger, "ledger-a", 42)

	// First byte must be PrefixInternal so ReadStoreComparer.Split treats the
	// whole key as the bloom prefix (no ledger-name split).
	require.Equal(t, PrefixInternal, key[0])
	require.Equal(t, SubInternalAuditIndex, key[1])
	require.Equal(t, byte(AuditFieldLedger), key[2])
	require.Equal(t, len(key), readStoreSplit(key), "audit-index key must not be split")
}

func TestAuditIndexUint64KeyOrdersByValueThenSeq(t *testing.T) {
	t.Parallel()

	kb := dal.NewKeyBuilder()
	k1 := AuditIndexUint64Key(kb, AuditFieldProposalID, 5, 100)
	k2 := AuditIndexUint64Key(kb, AuditFieldProposalID, 5, 101)
	k3 := AuditIndexUint64Key(kb, AuditFieldProposalID, 6, 1)

	require.Negative(t, bytesCompare(k1, k2), "same value: lower seq sorts first")
	require.Negative(t, bytesCompare(k2, k3), "higher value sorts after lower value")
}

func bytesCompare(a, b []byte) int {
	for i := 0; i < len(a) && i < len(b); i++ {
		if a[i] != b[i] {
			if a[i] < b[i] {
				return -1
			}
			return 1
		}
	}
	return len(a) - len(b)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `GOROOT= go test ./internal/storage/readstore/ -run TestAuditIndex -v`
Expected: FAIL — `undefined: AuditIndexStringKey`, `AuditFieldLedger`, `SubInternalAuditIndex`.

- [ ] **Step 3: Add the constants and key builders**

Append to `internal/storage/readstore/keys.go` (after the existing `SubInternal*` block — find it near `SubInternalIndexVersion`):

```go
// Audit-index internal sub-prefixes (under PrefixInternal = 0xFE).
const (
	// SubInternalAuditIndex is the keyspace for the audit secondary index.
	// Layout: [PrefixInternal][SubInternalAuditIndex][AuditField][value][audit_seq BE8] -> ∅
	SubInternalAuditIndex byte = 0x05
	// SubInternalAuditProgress holds the per-replica audit indexing cursor.
	// Layout: [PrefixInternal][SubInternalAuditProgress] -> last_indexed_audit_sequence BE8
	SubInternalAuditProgress byte = 0x06
)

// AuditField discriminates the indexed field within the audit-index keyspace.
const (
	AuditFieldOutcome       byte = 0x01 // 1 byte value: 0=failure, 1=success
	AuditFieldLedger        byte = 0x02 // string value (match-any over AuditEntry.Ledgers)
	AuditFieldCallerSubject byte = 0x03 // string value
	AuditFieldOrderType     byte = 0x04 // string token (match-any over items)
	AuditFieldTimestamp     byte = 0x05 // BE uint64 unix nanos (range)
	AuditFieldProposalID    byte = 0x06 // BE uint64 (range)
	AuditFieldLogSeq        byte = 0x07 // BE uint64 (range, match-any over items)
)

// AuditProgressKey returns the full key for the audit indexing cursor.
//
//	[0xFE][0x06]
func AuditProgressKey() []byte {
	return []byte{PrefixInternal, SubInternalAuditProgress}
}

// AuditIndexPrefix returns the global prefix for the whole audit index,
// used for DeleteRange on rebuild.
//
//	[0xFE][0x05]
func AuditIndexPrefix() []byte {
	return []byte{PrefixInternal, SubInternalAuditIndex}
}

// AuditIndexStringKey builds [0xFE][0x05][field][value\x00][seq BE8] for a
// string-valued field (ledger, caller_subject, order_type).
func AuditIndexStringKey(kb *dal.KeyBuilder, field byte, value string, seq uint64) []byte {
	return kb.Reset().
		PutByte(PrefixInternal).
		PutByte(SubInternalAuditIndex).
		PutByte(field).
		PutStringNull(value).
		PutUint64(seq).
		Consume()
}

// AuditIndexUint64Key builds [0xFE][0x05][field][value BE8][seq BE8] for a
// numeric range field (timestamp, proposal_id, log_seq).
func AuditIndexUint64Key(kb *dal.KeyBuilder, field byte, value, seq uint64) []byte {
	return kb.Reset().
		PutByte(PrefixInternal).
		PutByte(SubInternalAuditIndex).
		PutByte(field).
		PutUint64(value).
		PutUint64(seq).
		Consume()
}

// AuditIndexByteKey builds [0xFE][0x05][field][value][seq BE8] for a
// single-byte field (outcome).
func AuditIndexByteKey(kb *dal.KeyBuilder, field, value byte, seq uint64) []byte {
	return kb.Reset().
		PutByte(PrefixInternal).
		PutByte(SubInternalAuditIndex).
		PutByte(field).
		PutByte(value).
		PutUint64(seq).
		Consume()
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `GOROOT= go test ./internal/storage/readstore/ -run TestAuditIndex -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/storage/readstore/keys.go internal/storage/readstore/audit_index_test.go
git commit -m "feat(EN-1339): audit index key layout in readstore"
```

---

## Task 2: Readstore cursor, drop, and seek helpers

**Files:**
- Modify: `internal/storage/readstore/store.go`
- Test: `internal/storage/readstore/audit_index_test.go`

- [ ] **Step 1: Write the failing test** (round-trip through a real store)

Append to `internal/storage/readstore/audit_index_test.go`:

```go
func TestAuditProgressRoundTrip(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)

	got, err := s.ReadAuditProgress()
	require.NoError(t, err)
	require.Zero(t, got, "missing cursor reads as 0")

	batch := s.NewBatch()
	require.NoError(t, s.WriteAuditProgress(batch, 7))
	require.NoError(t, batch.Commit())

	got, err = s.ReadAuditProgress()
	require.NoError(t, err)
	require.Equal(t, uint64(7), got)
}

func TestSeekAuditEqualityAndRangeAndDrop(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)
	kb := dal.NewKeyBuilder()

	batch := s.NewBatch()
	require.NoError(t, batch.SetBytes(AuditIndexStringKey(kb, AuditFieldLedger, "a", 1), nil))
	require.NoError(t, batch.SetBytes(AuditIndexStringKey(kb, AuditFieldLedger, "a", 4), nil))
	require.NoError(t, batch.SetBytes(AuditIndexStringKey(kb, AuditFieldLedger, "b", 2), nil))
	require.NoError(t, batch.SetBytes(AuditIndexUint64Key(kb, AuditFieldProposalID, 10, 1), nil))
	require.NoError(t, batch.SetBytes(AuditIndexUint64Key(kb, AuditFieldProposalID, 20, 5), nil))
	require.NoError(t, batch.Commit())

	seqs, err := s.AuditSeqsByString(AuditFieldLedger, "a")
	require.NoError(t, err)
	require.Equal(t, []uint64{1, 4}, seqs)

	seqs, err = s.AuditSeqsByUint64Range(AuditFieldProposalID, 10, 10)
	require.NoError(t, err)
	require.Equal(t, []uint64{1}, seqs)

	require.NoError(t, s.DropAuditIndex())

	seqs, err = s.AuditSeqsByString(AuditFieldLedger, "a")
	require.NoError(t, err)
	require.Empty(t, seqs)
}
```

If `newTestStore` does not already exist in the package's test files, add it to `audit_index_test.go`:

```go
func newTestStore(t *testing.T) *Store {
	t.Helper()
	s, err := New(t.TempDir(), logging.NopZap(), DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	return s
}
```
(import `logging "github.com/formancehq/go-libs/v5/pkg/observe/log"`). First grep the package's existing tests for a store-construction helper and reuse it if present rather than duplicating.

- [ ] **Step 2: Run test to verify it fails**

Run: `GOROOT= go test ./internal/storage/readstore/ -run 'TestAuditProgress|TestSeekAudit' -v`
Expected: FAIL — `undefined: (*Store).ReadAuditProgress` etc.

- [ ] **Step 3: Implement the store methods**

Create `internal/storage/readstore/audit_index.go`:

```go
package readstore

import (
	"encoding/binary"
	"fmt"

	"github.com/cockroachdb/pebble/v2"

	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// ReadAuditProgress returns the last indexed audit sequence (0 if unset).
func (s *Store) ReadAuditProgress() (uint64, error) {
	v, closer, err := s.db.Get(AuditProgressKey())
	if err == pebble.ErrNotFound {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("reading audit progress: %w", err)
	}
	defer func() { _ = closer.Close() }()

	if len(v) != 8 {
		return 0, fmt.Errorf("audit progress: unexpected length %d", len(v))
	}
	return binary.BigEndian.Uint64(v), nil
}

// WriteAuditProgress persists the audit indexing cursor in the batch.
func (s *Store) WriteAuditProgress(batch *dal.WriteSession, sequence uint64) error {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], sequence)
	return batch.SetBytes(AuditProgressKey(), buf[:])
}

// DropAuditIndex removes every audit-index key (but NOT the cursor) so a
// rebuild can repopulate from scratch.
func (s *Store) DropAuditIndex() error {
	start := AuditIndexPrefix()
	end := append(AuditIndexPrefix(), 0xFF) // [0xFE][0x05] .. [0xFE][0x05][0xFF]
	batch := s.NewBatch()
	if err := batch.DeleteRange(start, end, nil); err != nil {
		return fmt.Errorf("dropping audit index: %w", err)
	}
	return batch.Commit()
}

// auditSeqsForPrefix scans [prefix, prefix+0xFF) and decodes the trailing
// 8-byte audit sequence of every key.
func (s *Store) auditSeqsForPrefix(lower, upper []byte) ([]uint64, error) {
	iter, err := s.db.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		return nil, fmt.Errorf("creating audit index iterator: %w", err)
	}
	defer func() { _ = iter.Close() }()

	var seqs []uint64
	for iter.First(); iter.Valid(); iter.Next() {
		k := iter.Key()
		if len(k) < 8 {
			return nil, fmt.Errorf("audit index key too short: %d", len(k))
		}
		seqs = append(seqs, binary.BigEndian.Uint64(k[len(k)-8:]))
	}
	if err := iter.Error(); err != nil {
		return nil, fmt.Errorf("iterating audit index: %w", err)
	}
	return seqs, nil
}

// AuditSeqsByString returns the audit sequences indexed under a string field
// for an exact value (equality / match-any).
func (s *Store) AuditSeqsByString(field byte, value string) ([]uint64, error) {
	kb := dal.NewKeyBuilder()
	lower := kb.Reset().PutByte(PrefixInternal).PutByte(SubInternalAuditIndex).
		PutByte(field).PutStringNull(value).Consume()
	upper := append(append([]byte{}, lower...), 0xFF)
	return s.auditSeqsForPrefix(lower, upper)
}

// AuditSeqsByOutcome returns the audit sequences for success/failure.
func (s *Store) AuditSeqsByOutcome(success bool) ([]uint64, error) {
	var b byte
	if success {
		b = 1
	}
	kb := dal.NewKeyBuilder()
	lower := kb.Reset().PutByte(PrefixInternal).PutByte(SubInternalAuditIndex).
		PutByte(AuditFieldOutcome).PutByte(b).Consume()
	upper := append(append([]byte{}, lower...), 0xFF)
	return s.auditSeqsForPrefix(lower, upper)
}

// AuditSeqsByUint64Range returns the audit sequences for a numeric field whose
// value falls in [lo, hi] inclusive.
func (s *Store) AuditSeqsByUint64Range(field byte, lo, hi uint64) ([]uint64, error) {
	kb := dal.NewKeyBuilder()
	lower := kb.Reset().PutByte(PrefixInternal).PutByte(SubInternalAuditIndex).
		PutByte(field).PutUint64(lo).Consume()
	// Upper bound is exclusive: [field][hi+1]. Guard hi == MaxUint64.
	ukb := dal.NewKeyBuilder()
	ub := ukb.Reset().PutByte(PrefixInternal).PutByte(SubInternalAuditIndex).PutByte(field)
	var upper []byte
	if hi == ^uint64(0) {
		upper = append(ub.Consume(), 0xFF)
	} else {
		upper = ub.PutUint64(hi + 1).Consume()
	}
	return s.auditSeqsForPrefix(lower, upper)
}
```

Note: confirm `s.db` is the unexported field name (it is, per `store.go:50`). If `pebble.ErrNotFound` import differs, mirror the existing `ReadProgress` error handling in `store.go:196`.

- [ ] **Step 4: Run test to verify it passes**

Run: `GOROOT= go test ./internal/storage/readstore/ -run 'TestAuditProgress|TestSeekAudit' -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/storage/readstore/audit_index.go internal/storage/readstore/audit_index_test.go
git commit -m "feat(EN-1339): audit index cursor, drop, and seek helpers"
```

---

## Task 3: Canonical order-type token vocabulary

**Files:**
- Create: `internal/domain/audit_order_type.go`
- Test: `internal/domain/audit_order_type_test.go`

First confirm the exact oneof accessor/wrapper names: run
`grep -n "func (x \*Order) Get\|Order_LedgerScoped\|Order_SystemScoped\|func (x \*LedgerScopedOrder) Get\|LedgerScopedOrder_\|func (x \*LedgerApplyOrder) Get\|LedgerApplyOrder_\|SystemScopedOrder_" internal/proto/raftcmdpb/raft_cmd.pb.go | head -60`
and adjust the type-switch arms below to the real generated wrapper type names.

- [ ] **Step 1: Write the failing test**

Create `internal/domain/audit_order_type_test.go`:

```go
package domain_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

func TestAuditOrderType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		order *raftcmdpb.Order
		want  string
	}{
		{
			name: "create transaction",
			order: &raftcmdpb.Order{Type: &raftcmdpb.Order_LedgerScoped{
				LedgerScoped: &raftcmdpb.LedgerScopedOrder{Type: &raftcmdpb.LedgerScopedOrder_Apply{
					Apply: &raftcmdpb.LedgerApplyOrder{Type: &raftcmdpb.LedgerApplyOrder_CreateTransaction{}},
				}},
			}},
			want: "create_transaction",
		},
		{
			name:  "nil order",
			order: nil,
			want:  "unknown",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tt.want, domain.AuditOrderType(tt.order))
		})
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `GOROOT= go test ./internal/domain/ -run TestAuditOrderType -v`
Expected: FAIL — `undefined: domain.AuditOrderType`.

- [ ] **Step 3: Implement the mapping**

Create `internal/domain/audit_order_type.go`. Map every `Order` oneof arm to a stable snake_case token. Break `Apply` down by `LedgerApplyOrder` arm because that is the granularity callers filter on (create_transaction / revert_transaction / add_metadata / delete_metadata). This vocabulary is the contract shared with EN-1305 — change it only in lockstep with the filter DSL.

```go
package domain

import "github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"

// AuditOrderType returns the stable token used to index and filter an audit
// item by its order kind. The vocabulary is the contract shared with the
// audit filter DSL (EN-1305); extend it additively, never rename a token.
func AuditOrderType(order *raftcmdpb.Order) string {
	switch t := order.GetType().(type) {
	case *raftcmdpb.Order_LedgerScoped:
		return ledgerScopedOrderType(t.LedgerScoped)
	case *raftcmdpb.Order_SystemScoped:
		return systemScopedOrderType(t.SystemScoped)
	default:
		return "unknown"
	}
}

func ledgerScopedOrderType(o *raftcmdpb.LedgerScopedOrder) string {
	switch a := o.GetType().(type) {
	case *raftcmdpb.LedgerScopedOrder_Apply:
		return ledgerApplyOrderType(a.Apply)
	case *raftcmdpb.LedgerScopedOrder_CreateLedger:
		return "create_ledger"
	case *raftcmdpb.LedgerScopedOrder_DeleteLedger:
		return "delete_ledger"
	case *raftcmdpb.LedgerScopedOrder_MirrorIngest:
		return "mirror_ingest"
	case *raftcmdpb.LedgerScopedOrder_PromoteLedger:
		return "promote_ledger"
	case *raftcmdpb.LedgerScopedOrder_SaveLedgerMetadata:
		return "save_ledger_metadata"
	case *raftcmdpb.LedgerScopedOrder_DeleteLedgerMetadata:
		return "delete_ledger_metadata"
	case *raftcmdpb.LedgerScopedOrder_SaveNumscript:
		return "save_numscript"
	case *raftcmdpb.LedgerScopedOrder_DeleteNumscript:
		return "delete_numscript"
	case *raftcmdpb.LedgerScopedOrder_CreatePreparedQuery:
		return "create_prepared_query"
	case *raftcmdpb.LedgerScopedOrder_UpdatePreparedQuery:
		return "update_prepared_query"
	case *raftcmdpb.LedgerScopedOrder_DeletePreparedQuery:
		return "delete_prepared_query"
	default:
		return "unknown"
	}
}

func ledgerApplyOrderType(o *raftcmdpb.LedgerApplyOrder) string {
	switch o.GetType().(type) {
	case *raftcmdpb.LedgerApplyOrder_CreateTransaction:
		return "create_transaction"
	case *raftcmdpb.LedgerApplyOrder_RevertTransaction:
		return "revert_transaction"
	case *raftcmdpb.LedgerApplyOrder_AddMetadata:
		return "add_metadata"
	case *raftcmdpb.LedgerApplyOrder_DeleteMetadata:
		return "delete_metadata"
	default:
		return "unknown"
	}
}

func systemScopedOrderType(o *raftcmdpb.SystemScopedOrder) string {
	switch o.GetType().(type) {
	case *raftcmdpb.SystemScopedOrder_RegisterSigningKey:
		return "register_signing_key"
	case *raftcmdpb.SystemScopedOrder_RevokeSigningKey:
		return "revoke_signing_key"
	case *raftcmdpb.SystemScopedOrder_SetSigningConfig:
		return "set_signing_config"
	case *raftcmdpb.SystemScopedOrder_AddEventsSink:
		return "add_events_sink"
	case *raftcmdpb.SystemScopedOrder_RemoveEventsSink:
		return "remove_events_sink"
	case *raftcmdpb.SystemScopedOrder_CloseChapter:
		return "close_chapter"
	case *raftcmdpb.SystemScopedOrder_SealChapter:
		return "seal_chapter"
	case *raftcmdpb.SystemScopedOrder_ArchiveChapter:
		return "archive_chapter"
	case *raftcmdpb.SystemScopedOrder_ConfirmArchiveChapter:
		return "confirm_archive_chapter"
	case *raftcmdpb.SystemScopedOrder_SetMaintenanceMode:
		return "set_maintenance_mode"
	case *raftcmdpb.SystemScopedOrder_SetChapterSchedule:
		return "set_chapter_schedule"
	case *raftcmdpb.SystemScopedOrder_DeleteChapterSchedule:
		return "delete_chapter_schedule"
	case *raftcmdpb.SystemScopedOrder_CreateQueryCheckpoint:
		return "create_query_checkpoint"
	case *raftcmdpb.SystemScopedOrder_DeleteQueryCheckpoint:
		return "delete_query_checkpoint"
	case *raftcmdpb.SystemScopedOrder_SetQueryCheckpointSchedule:
		return "set_query_checkpoint_schedule"
	case *raftcmdpb.SystemScopedOrder_DeleteQueryCheckpointSchedule:
		return "delete_query_checkpoint_schedule"
	default:
		return "unknown"
	}
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `GOROOT= go test ./internal/domain/ -run TestAuditOrderType -v`
Expected: PASS. If a wrapper type name is wrong, the compile error names the correct one — fix and re-run.

- [ ] **Step 5: Commit**

```bash
git add internal/domain/audit_order_type.go internal/domain/audit_order_type_test.go
git commit -m "feat(EN-1339): canonical audit order-type token vocabulary"
```

---

## Task 4: Derive index keys for one AuditEntry

**Files:**
- Create: `internal/application/auditindexer/index_entry.go`
- Test: `internal/application/auditindexer/index_entry_test.go`

- [ ] **Step 1: Write the failing test**

Create `internal/application/auditindexer/index_entry_test.go`:

```go
package auditindexer

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/readstore"
)

func mustOrderBytes(t *testing.T, o *raftcmdpb.Order) []byte {
	t.Helper()
	b, err := proto.Marshal(o)
	require.NoError(t, err)
	return b
}

func TestAppendEntryKeys(t *testing.T) {
	t.Parallel()

	entry := &auditpb.AuditEntry{
		Sequence:       9,
		ProposalId:     3,
		Timestamp:      &commonpb.Timestamp{Seconds: 1, Nanos: 0},
		Outcome:        &auditpb.AuditEntry_Success{Success: &auditpb.AuditSuccess{}},
		Ledgers:        []string{"a", "b"},
		CallerSnapshot: &commonpb.CallerSnapshot{Identity: &commonpb.CallerIdentity{Subject: "alice"}},
	}
	items := []*auditpb.AuditItem{
		{OrderIndex: 0, LogSequence: 100, SerializedOrder: mustOrderBytes(t, &raftcmdpb.Order{Type: &raftcmdpb.Order_LedgerScoped{
			LedgerScoped: &raftcmdpb.LedgerScopedOrder{Type: &raftcmdpb.LedgerScopedOrder_Apply{
				Apply: &raftcmdpb.LedgerApplyOrder{Type: &raftcmdpb.LedgerApplyOrder_CreateTransaction{}}}}}})},
		{OrderIndex: 1, LogSequence: 0}, // idempotent/failed → no log_seq key
	}

	var keys [][]byte
	emit := func(k []byte) error { keys = append(keys, append([]byte{}, k...)); return nil }

	require.NoError(t, appendEntryKeys(dal.NewKeyBuilder(), emit, entry, items))

	// Expect: outcome(1) + ledger("a","b")(2) + caller_subject(1) +
	// order_type("create_transaction")(1) + timestamp(1) + proposal_id(1) +
	// log_seq(100)(1) = 8 keys. (order_type deduplicated; log_seq=0 skipped.)
	require.Len(t, keys, 8)

	// Every key must carry seq 9 in its trailing 8 bytes and start with the
	// audit-index prefix.
	for _, k := range keys {
		require.Equal(t, readstore.PrefixInternal, k[0])
		require.Equal(t, readstore.SubInternalAuditIndex, k[1])
	}

	// Sanity: there must be exactly one order_type key.
	var orderTypeKeys int
	for _, k := range keys {
		if k[2] == readstore.AuditFieldOrderType {
			orderTypeKeys++
		}
	}
	_ = sort.Ints
	require.Equal(t, 1, orderTypeKeys)
}

func TestAppendEntryKeysFailureNilCaller(t *testing.T) {
	t.Parallel()

	entry := &auditpb.AuditEntry{
		Sequence:   2,
		ProposalId: 1,
		Timestamp:  &commonpb.Timestamp{Seconds: 1},
		Outcome:    &auditpb.AuditEntry_Failure{Failure: &auditpb.AuditFailure{}},
		Ledgers:    []string{"x"},
		// CallerSnapshot nil (system proposal)
	}

	var keys [][]byte
	emit := func(k []byte) error { keys = append(keys, append([]byte{}, k...)); return nil }
	require.NoError(t, appendEntryKeys(dal.NewKeyBuilder(), emit, entry, nil))

	// outcome + ledger("x") + timestamp + proposal_id = 4 keys; no caller, no items.
	require.Len(t, keys, 4)
	for _, k := range keys {
		require.NotEqual(t, readstore.AuditFieldCallerSubject, k[2])
	}
}
```

Confirm the `AuditSuccess`/`AuditFailure` wrapper type names and `commonpb.Timestamp` fields with:
`grep -n "AuditEntry_Success\|AuditEntry_Failure\|type AuditSuccess\|type AuditFailure" internal/proto/auditpb/audit.pb.go` and
`grep -n "type Timestamp struct" -A4 internal/proto/commonpb/*.go`. Adjust the test/impl to the real names. If `Timestamp` already has a helper to nanos, prefer it (grep `func.*Timestamp.*Nano` / `AsTime`).

- [ ] **Step 2: Run test to verify it fails**

Run: `GOROOT= go test ./internal/application/auditindexer/ -run TestAppendEntryKeys -v`
Expected: FAIL — `undefined: appendEntryKeys`.

- [ ] **Step 3: Implement `appendEntryKeys`**

Create `internal/application/auditindexer/index_entry.go`:

```go
package auditindexer

import (
	"fmt"

	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/readstore"
)

// emitFn receives one fully-built index key. The key slice is only valid until
// the next call (it shares the KeyBuilder buffer via Consume), so emit must
// copy it before retaining (the worker's emit hands it straight to
// batch.SetBytes, which copies into the Pebble batch repr).
type emitFn func(key []byte) error

// appendEntryKeys derives every audit-index key for a single AuditEntry and
// passes each to emit with an empty value. items may be nil (header-only
// fields are still indexed).
func appendEntryKeys(kb *dal.KeyBuilder, emit emitFn, entry *auditpb.AuditEntry, items []*auditpb.AuditItem) error {
	seq := entry.GetSequence()

	// outcome
	var outcome byte
	if _, ok := entry.GetOutcome().(*auditpb.AuditEntry_Success); ok {
		outcome = 1
	}
	if err := emit(readstore.AuditIndexByteKey(kb, readstore.AuditFieldOutcome, outcome, seq)); err != nil {
		return err
	}

	// ledger (match-any)
	for _, ledger := range entry.GetLedgers() {
		if err := emit(readstore.AuditIndexStringKey(kb, readstore.AuditFieldLedger, ledger, seq)); err != nil {
			return err
		}
	}

	// caller_subject (skip when absent)
	if subject := entry.GetCallerSnapshot().GetIdentity().GetSubject(); subject != "" {
		if err := emit(readstore.AuditIndexStringKey(kb, readstore.AuditFieldCallerSubject, subject, seq)); err != nil {
			return err
		}
	}

	// timestamp (range)
	if ts := entry.GetTimestamp(); ts != nil {
		nanos := uint64(ts.GetSeconds())*1_000_000_000 + uint64(ts.GetNanos())
		if err := emit(readstore.AuditIndexUint64Key(kb, readstore.AuditFieldTimestamp, nanos, seq)); err != nil {
			return err
		}
	}

	// proposal_id (range)
	if err := emit(readstore.AuditIndexUint64Key(kb, readstore.AuditFieldProposalID, entry.GetProposalId(), seq)); err != nil {
		return err
	}

	// order_type (match-any, deduplicated) and log_seq (match-any, skip 0)
	seenType := make(map[string]struct{}, len(items))
	for _, item := range items {
		if logSeq := item.GetLogSequence(); logSeq != 0 {
			if err := emit(readstore.AuditIndexUint64Key(kb, readstore.AuditFieldLogSeq, logSeq, seq)); err != nil {
				return err
			}
		}

		order := &raftcmdpb.Order{}
		if err := proto.Unmarshal(item.GetSerializedOrder(), order); err != nil {
			return fmt.Errorf("unmarshaling order for audit seq %d item %d: %w", seq, item.GetOrderIndex(), err)
		}
		token := domain.AuditOrderType(order)
		if _, done := seenType[token]; done {
			continue
		}
		seenType[token] = struct{}{}
		if err := emit(readstore.AuditIndexStringKey(kb, readstore.AuditFieldOrderType, token, seq)); err != nil {
			return err
		}
	}

	return nil
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `GOROOT= go test ./internal/application/auditindexer/ -run TestAppendEntryKeys -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/application/auditindexer/index_entry.go internal/application/auditindexer/index_entry_test.go
git commit -m "feat(EN-1339): derive audit index keys per AuditEntry"
```

---

## Task 5: The Indexer worker (struct, processing, cursor, catch-up)

**Files:**
- Create: `internal/application/auditindexer/indexer.go`
- Test: `internal/application/auditindexer/indexer_test.go`

- [ ] **Step 1: Write the failing integration test** (catch-up + restart resume)

Create `internal/application/auditindexer/indexer_test.go`. It writes `AuditEntry`/`AuditItem` rows directly into a main `dal.Store` (mirroring the key layout used by `query.ReadAuditEntries`), runs `ProcessOnce`, and asserts the index + cursor.

```go
package auditindexer

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"
	"google.golang.org/protobuf/proto"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/readstore"
)

func writeAuditEntry(t *testing.T, store *dal.Store, entry *auditpb.AuditEntry) {
	t.Helper()
	batch := store.OpenWriteSession()
	kb := dal.NewKeyBuilder()
	val, err := proto.Marshal(entry)
	require.NoError(t, err)
	require.NoError(t, batch.SetBytes(
		kb.PutZonePrefix(dal.ZoneCold, dal.SubColdAudit).PutUint64(entry.GetSequence()).Build(), val))
	require.NoError(t, batch.Commit())
}

func newIndexerForTest(t *testing.T) (*Indexer, *dal.Store, *readstore.Store) {
	t.Helper()
	mainStore, err := dal.NewStore(t.TempDir(), logging.NopZap(), noop.Meter{}, dal.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = mainStore.Close() })

	rs, err := readstore.New(t.TempDir(), logging.NopZap(), readstore.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = rs.Close() })

	idx := New(Config{}, mainStore, rs, logging.NopZap(), noop.Meter{})
	return idx, mainStore, rs
}

func TestIndexerCatchUpAndResume(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	idx, mainStore, rs := newIndexerForTest(t)

	writeAuditEntry(t, mainStore, &auditpb.AuditEntry{
		Sequence: 1, ProposalId: 7, Timestamp: &commonpb.Timestamp{Seconds: 1},
		Outcome: &auditpb.AuditEntry_Success{Success: &auditpb.AuditSuccess{}},
		Ledgers: []string{"main"},
	})

	processed, err := idx.ProcessOnce(ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(1), processed)

	seqs, err := rs.AuditSeqsByString(readstore.AuditFieldLedger, "main")
	require.NoError(t, err)
	require.Equal(t, []uint64{1}, seqs)

	cursor, err := rs.ReadAuditProgress()
	require.NoError(t, err)
	require.Equal(t, uint64(1), cursor)

	// Second run with no new entries is a no-op.
	processed, err = idx.ProcessOnce(ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(1), processed)

	// A fresh Indexer over the same readstore resumes from the cursor.
	idx2 := New(Config{}, mainStore, rs, logging.NopZap(), noop.Meter{})
	writeAuditEntry(t, mainStore, &auditpb.AuditEntry{
		Sequence: 2, ProposalId: 8, Timestamp: &commonpb.Timestamp{Seconds: 2},
		Outcome: &auditpb.AuditEntry_Failure{Failure: &auditpb.AuditFailure{}},
		Ledgers: []string{"main"},
	})
	processed, err = idx2.ProcessOnce(ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(2), processed)
	seqs, err = rs.AuditSeqsByString(readstore.AuditFieldLedger, "main")
	require.NoError(t, err)
	require.Equal(t, []uint64{1, 2}, seqs)
}
```

Note: confirm `dal.Store.OpenWriteSession()`, `dal.NewStore` signature, and that `t.TempDir()` works for a main store. If `NewStore` needs more args, mirror `cmd/ledgerctl/store/rebuild_indexes.go` / existing dal store tests. `OpenWriteSession` in a test is an allowed lifecycle use (it is how the FSM writes); for test setup it is fine.

- [ ] **Step 2: Run test to verify it fails**

Run: `GOROOT= go test ./internal/application/auditindexer/ -run TestIndexerCatchUp -v`
Expected: FAIL — `undefined: New`, `Indexer`, `Config`, `ProcessOnce`.

- [ ] **Step 3: Implement the worker core**

Create `internal/application/auditindexer/indexer.go`:

```go
package auditindexer

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/metric"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/pkg/worker"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/readstore"
)

// DefaultBatchSize is the number of audit entries indexed per Pebble batch.
const DefaultBatchSize = 1000

// Config tunes the audit indexer.
type Config struct {
	BatchSize        int    // entries per batch (0 = DefaultBatchSize)
	RebuildThreshold uint64 // boot drop+rebuild when (last - cursor) exceeds this (0 = never)
	Disabled         bool   // ops kill switch
}

// Indexer tails the Audit zone and maintains the readstore audit index.
// It runs on all nodes; progress is per-replica (no Raft).
type Indexer struct {
	cfg       Config
	store     *dal.Store
	readStore *readstore.Store
	logger    logging.Logger
	meter     metric.Meter

	w         worker.Worker
	batchSize int

	lastIndexed atomic.Uint64
	auditLast   atomic.Uint64
	reg         metric.Registration
}

// New builds an Indexer.
func New(cfg Config, store *dal.Store, rs *readstore.Store, logger logging.Logger, meter metric.Meter) *Indexer {
	batchSize := cfg.BatchSize
	if batchSize <= 0 {
		batchSize = DefaultBatchSize
	}
	return &Indexer{
		cfg:       cfg,
		store:     store,
		readStore: rs,
		logger:    logger.WithFields(map[string]any{"cmp": "audit-indexer"}),
		meter:     meter,
		batchSize: batchSize,
	}
}

// lastAuditSequence reads the last audit sequence from the main store.
func (i *Indexer) lastAuditSequence() (uint64, error) {
	handle, err := i.store.NewDirectReadHandle()
	if err != nil {
		return 0, fmt.Errorf("opening read handle: %w", err)
	}
	defer func() { _ = handle.Close() }()

	last, err := query.ReadLastAuditSequence(handle)
	if err != nil {
		return 0, fmt.Errorf("reading last audit sequence: %w", err)
	}
	return last, nil
}

// ProcessOnce indexes all audit entries after the persisted cursor, in
// batches, and returns the cursor it reached. Safe to call repeatedly.
func (i *Indexer) ProcessOnce(ctx context.Context) (uint64, error) {
	cursor, err := i.readStore.ReadAuditProgress()
	if err != nil {
		return 0, err
	}

	for {
		next, advanced, err := i.processBatch(ctx, cursor)
		if err != nil {
			return cursor, err
		}
		cursor = next
		if !advanced {
			break
		}
	}

	i.lastIndexed.Store(cursor)
	return cursor, nil
}

// processBatch indexes up to batchSize entries after `after`, committing one
// readstore batch. Returns the new cursor and whether any entry was processed.
func (i *Indexer) processBatch(ctx context.Context, after uint64) (uint64, bool, error) {
	handle, err := i.store.NewDirectReadHandle()
	if err != nil {
		return after, false, fmt.Errorf("opening read handle: %w", err)
	}
	defer func() { _ = handle.Close() }()

	cur, err := query.ReadAuditEntries(ctx, handle, &after)
	if err != nil {
		return after, false, err
	}
	defer cur.Close()

	batch := i.readStore.NewBatch()
	kb := dal.NewKeyBuilder()
	emit := func(key []byte) error { return batch.SetBytes(key, nil) }

	cursor := after
	count := 0
	for cur.HasMore() && count < i.batchSize {
		entry, err := cur.Next()
		if err != nil {
			return after, false, err
		}

		items, err := query.ReadAuditItems(ctx, handle, entry.GetSequence())
		if err != nil {
			return after, false, err
		}
		if err := appendEntryKeys(kb, emit, entry, items); err != nil {
			return after, false, err
		}

		cursor = entry.GetSequence()
		count++
	}

	if count == 0 {
		return after, false, nil
	}

	if err := i.readStore.WriteAuditProgress(batch, cursor); err != nil {
		return after, false, err
	}
	if err := batch.Commit(); err != nil {
		return after, false, fmt.Errorf("committing audit index batch: %w", err)
	}

	i.lastIndexed.Store(cursor)
	return cursor, true, nil
}
```

Note: confirm the cursor API on `query.ReadAuditEntries`' return type (`cursor.Cursor[*auditpb.AuditEntry]`). Grep `internal/pkg/cursor/*.go` for the method names — this plan assumes `HasMore() bool`, `Next() (*auditpb.AuditEntry, error)`, `Close()`. If the real methods differ (e.g. `iter.First()/Valid()/Next()` style like `dal` iterators), adapt the loop. Also confirm `dal.Store.NewDirectReadHandle()` returns a `dal.PebbleReader` (it is used the same way in `indexbuilder/builder.go:480`).

- [ ] **Step 4: Run test to verify it passes**

Run: `GOROOT= go test ./internal/application/auditindexer/ -run TestIndexerCatchUp -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/application/auditindexer/indexer.go internal/application/auditindexer/indexer_test.go
git commit -m "feat(EN-1339): audit indexer worker core (catch-up + cursor)"
```

---

## Task 6: Rebuild (drop + reset) and boot auto-rebuild

**Files:**
- Modify: `internal/application/auditindexer/indexer.go`
- Test: `internal/application/auditindexer/indexer_test.go`

- [ ] **Step 1: Write the failing test** (rebuild parity + threshold)

Append to `indexer_test.go`:

```go
func TestRebuildYieldsIdenticalIndex(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	idx, mainStore, rs := newIndexerForTest(t)

	for s := uint64(1); s <= 5; s++ {
		writeAuditEntry(t, mainStore, &auditpb.AuditEntry{
			Sequence: s, ProposalId: s, Timestamp: &commonpb.Timestamp{Seconds: int64(s)},
			Outcome: &auditpb.AuditEntry_Success{Success: &auditpb.AuditSuccess{}},
			Ledgers: []string{"main"},
		})
	}

	_, err := idx.ProcessOnce(ctx)
	require.NoError(t, err)
	before := dumpAuditIndex(t, rs)

	require.NoError(t, idx.Rebuild(ctx))
	after := dumpAuditIndex(t, rs)

	require.Equal(t, before, after, "rebuild must yield a byte-identical index")

	cursor, err := rs.ReadAuditProgress()
	require.NoError(t, err)
	require.Equal(t, uint64(5), cursor)
}

// dumpAuditIndex returns every audit-index key as a sorted slice of strings.
func dumpAuditIndex(t *testing.T, rs *readstore.Store) []string {
	t.Helper()
	// Use the same prefix scan DropAuditIndex uses.
	seqsA, err := rs.AuditSeqsByString(readstore.AuditFieldLedger, "main")
	require.NoError(t, err)
	out := make([]string, 0, len(seqsA))
	for _, s := range seqsA {
		out = append(out, fmtSeq(s))
	}
	return out
}

func fmtSeq(s uint64) string { return string(rune('0' + int(s%10))) }
```
(If you want a stricter byte-identical comparison, add a `(*Store).DumpAuditIndexKeys() [][]byte` test helper behind a build tag or in `export_test.go`; the ledger field scan above is a sufficient parity smoke test for the plan.)

- [ ] **Step 2: Run test to verify it fails**

Run: `GOROOT= go test ./internal/application/auditindexer/ -run TestRebuild -v`
Expected: FAIL — `undefined: (*Indexer).Rebuild`.

- [ ] **Step 3: Implement Rebuild + boot decision**

Append to `internal/application/auditindexer/indexer.go`:

```go
// Rebuild drops the audit index and the cursor, then replays from the earliest
// surviving audit entry. Used by ledgerctl and by boot auto-rebuild.
func (i *Indexer) Rebuild(ctx context.Context) error {
	if err := i.readStore.DropAuditIndex(); err != nil {
		return err
	}
	batch := i.readStore.NewBatch()
	if err := i.readStore.WriteAuditProgress(batch, 0); err != nil {
		return err
	}
	if err := batch.Commit(); err != nil {
		return fmt.Errorf("resetting audit cursor: %w", err)
	}
	i.lastIndexed.Store(0)

	_, err := i.ProcessOnce(ctx)
	return err
}

// shouldRebuildOnBoot reports whether boot should drop+rebuild instead of an
// incremental catch-up: cursor missing (0) with entries present, or the gap
// exceeds the configured threshold.
func (i *Indexer) shouldRebuildOnBoot(cursor, last uint64) bool {
	if cursor == 0 && last > 0 {
		return true
	}
	if i.cfg.RebuildThreshold > 0 && last > cursor && last-cursor > i.cfg.RebuildThreshold {
		return true
	}
	return false
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `GOROOT= go test ./internal/application/auditindexer/ -run TestRebuild -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/application/auditindexer/indexer.go internal/application/auditindexer/indexer_test.go
git commit -m "feat(EN-1339): audit index rebuild + boot auto-rebuild decision"
```

---

## Task 7: Background loop + lifecycle + metrics

**Files:**
- Modify: `internal/application/auditindexer/indexer.go`
- Test: `internal/application/auditindexer/indexer_test.go`

- [ ] **Step 1: Write the failing test** (Start indexes, Stop is clean)

Append to `indexer_test.go`:

```go
func TestStartStopIndexes(t *testing.T) {
	t.Parallel()
	idx, mainStore, rs := newIndexerForTest(t)

	writeAuditEntry(t, mainStore, &auditpb.AuditEntry{
		Sequence: 1, ProposalId: 1, Timestamp: &commonpb.Timestamp{Seconds: 1},
		Outcome: &auditpb.AuditEntry_Success{Success: &auditpb.AuditSuccess{}},
		Ledgers: []string{"main"},
	})

	idx.Start()
	t.Cleanup(idx.Stop)

	require.Eventually(t, func() bool {
		seqs, err := rs.AuditSeqsByString(readstore.AuditFieldLedger, "main")
		return err == nil && len(seqs) == 1
	}, 5*time.Second, 20*time.Millisecond)
}
```
(import `time`.)

- [ ] **Step 2: Run test to verify it fails**

Run: `GOROOT= go test ./internal/application/auditindexer/ -run TestStartStop -v`
Expected: FAIL — `undefined: (*Indexer).Start`.

- [ ] **Step 3: Implement Start/Stop/loop + metrics**

Append to `internal/application/auditindexer/indexer.go`:

```go
// TickInterval is the steady-state polling interval. The audit sequence
// advances on every proposal (including failures, which emit no log), so a
// ticker — not a log signal — is what guarantees pickup. Lag is eventual.
const TickInterval = 200 * time.Millisecond

// Start launches the background indexing loop (no-op if disabled).
func (i *Indexer) Start() {
	if i.cfg.Disabled {
		i.logger.Infof("Audit indexer disabled")
		return
	}
	if reg, err := i.registerMetrics(); err == nil {
		i.reg = reg
	}
	i.w = worker.New()
	i.w.RunCtx(i.loop)
}

// Stop halts the loop and unregisters metrics.
func (i *Indexer) Stop() {
	if i.cfg.Disabled {
		return
	}
	i.w.Stop()
	if i.reg != nil {
		_ = i.reg.Unregister()
	}
}

func (i *Indexer) loop(ctx context.Context) {
	cursor, err := i.readStore.ReadAuditProgress()
	if err != nil {
		i.logger.Errorf("read audit cursor: %v", err)
		return
	}
	if last, err := i.lastAuditSequence(); err == nil {
		i.auditLast.Store(last)
		if i.shouldRebuildOnBoot(cursor, last) {
			i.logger.WithFields(map[string]any{"cursor": cursor, "last": last}).Infof("Audit index rebuild on boot")
			if err := i.Rebuild(ctx); err != nil {
				i.logger.Errorf("audit index boot rebuild: %v", err)
			}
		}
	}

	ticker := time.NewTicker(TickInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		if last, err := i.lastAuditSequence(); err == nil {
			i.auditLast.Store(last)
		}
		if _, err := i.ProcessOnce(ctx); err != nil {
			i.logger.Errorf("audit indexing: %v", err)
		}
	}
}

func (i *Indexer) registerMetrics() (metric.Registration, error) {
	indexed, err := i.meter.Int64ObservableGauge("audit_index.last_indexed_sequence",
		metric.WithDescription("Last audit sequence indexed"))
	if err != nil {
		return nil, err
	}
	last, err := i.meter.Int64ObservableGauge("audit_index.audit_last_sequence",
		metric.WithDescription("Last audit sequence in the store"))
	if err != nil {
		return nil, err
	}
	lag, err := i.meter.Int64ObservableGauge("audit_index.lag",
		metric.WithDescription("Audit entries the index is behind"))
	if err != nil {
		return nil, err
	}
	return i.meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		idx := int64(i.lastIndexed.Load())
		al := int64(i.auditLast.Load())
		o.ObserveInt64(indexed, idx)
		o.ObserveInt64(last, al)
		o.ObserveInt64(lag, max(al-idx, 0))
		return nil
	}, indexed, last, lag)
}
```

Confirm `worker.New()` returns a value with `RunCtx(func(context.Context))` and `Stop()` (it is used identically in `indexbuilder/builder.go:372`).

- [ ] **Step 4: Run test to verify it passes**

Run: `GOROOT= go test ./internal/application/auditindexer/ -run TestStartStop -v`
Expected: PASS. Then run the whole package: `GOROOT= go test ./internal/application/auditindexer/ -v`.

- [ ] **Step 5: Commit**

```bash
git add internal/application/auditindexer/indexer.go internal/application/auditindexer/indexer_test.go
git commit -m "feat(EN-1339): audit indexer background loop, lifecycle, metrics"
```

---

## Task 8: Config + server flags

**Files:**
- Modify: `internal/bootstrap/config.go`
- Modify: `cmd/server/server.go`

- [ ] **Step 1: Add config fields**

In `internal/bootstrap/config.go`, add a config struct near `ReadIndexConfig` (line ~103) and a field on the main `Config` (near line 184):

```go
// AuditIndexConfig holds configuration for the audit secondary index worker.
type AuditIndexConfig struct {
	BatchSize        int    // audit entries per Pebble batch (0 = default 1000)
	RebuildThreshold uint64 // boot drop+rebuild when (last - cursor) exceeds this (0 = never)
	Disabled         bool   // ops kill switch
}
```

Add to `Config`:

```go
	AuditIndexConfig AuditIndexConfig
```

- [ ] **Step 2: Register flags**

In `cmd/server/server.go`, near the read-index flags (line ~224):

```go
	runCmd.Flags().Int("audit-index-batch-size", 0, "Audit entries per Pebble batch commit (0 = default 1000)")
	runCmd.Flags().Uint64("audit-index-rebuild-threshold", 0, "Drop+rebuild the audit index on boot when the cursor is this far behind (0 = never)")
	runCmd.Flags().Bool("disable-audit-index", false, "Disable the audit secondary index worker")
```

- [ ] **Step 3: Map flags into Config**

Find where `ReadIndexConfig` is populated from flags (grep `GetString("read-index-dir")` / `ReadIndexConfig{` in `cmd/server/`). Alongside it, populate `AuditIndexConfig`:

```go
	auditBatch, _ := cmd.Flags().GetInt("audit-index-batch-size")
	auditThreshold, _ := cmd.Flags().GetUint64("audit-index-rebuild-threshold")
	auditDisabled, _ := cmd.Flags().GetBool("disable-audit-index")
	cfg.AuditIndexConfig = bootstrap.AuditIndexConfig{
		BatchSize:        auditBatch,
		RebuildThreshold: auditThreshold,
		Disabled:         auditDisabled,
	}
```
(Match the exact `cfg`/package reference used by the surrounding flag-mapping code.)

- [ ] **Step 4: Verify compilation**

Run: `GOROOT= go build ./cmd/server/... ./internal/bootstrap/...`
Expected: builds clean.

- [ ] **Step 5: Commit**

```bash
git add internal/bootstrap/config.go cmd/server/server.go
git commit -m "feat(EN-1339): audit index config + server flags"
```

---

## Task 9: fx wiring (provider + lifecycle)

**Files:**
- Modify: `internal/bootstrap/module.go`

- [ ] **Step 1: Add the provider**

In `internal/bootstrap/module.go`, alongside the `indexbuilder.Builder` provider (line ~611), add:

```go
			// Audit indexer — tails the Audit zone to populate the readstore audit index.
			func(store *dal.Store, rs *readstore.Store, logger logging.Logger, meterProvider metric.MeterProvider, cfg Config) *auditindexer.Indexer {
				return auditindexer.New(
					auditindexer.Config{
						BatchSize:        cfg.AuditIndexConfig.BatchSize,
						RebuildThreshold: cfg.AuditIndexConfig.RebuildThreshold,
						Disabled:         cfg.AuditIndexConfig.Disabled,
					},
					store, rs, logger, meterProvider.Meter("audit.index"),
				)
			},
```

Add the import `"github.com/formancehq/ledger/v3/internal/application/auditindexer"`.

- [ ] **Step 2: Add the lifecycle hook**

Near the index-builder lifecycle hook (line ~1322), add an invoke:

```go
			func(lc fx.Lifecycle, auditIndexer *auditindexer.Indexer) {
				lc.Append(worker.FxHook(auditIndexer))
			},
```

- [ ] **Step 3: Verify compilation + fx graph**

Run: `GOROOT= go build ./...`
Expected: builds clean. (If there is a DI/wiring test such as `bootstrap` fx validation, run it: `GOROOT= go test ./internal/bootstrap/... -run Fx`.)

- [ ] **Step 4: Commit**

```bash
git add internal/bootstrap/module.go
git commit -m "feat(EN-1339): wire audit indexer into fx lifecycle"
```

---

## Task 10: ledgerctl rebuild command

**Files:**
- Create: `cmd/ledgerctl/store/rebuild_audit_index.go`
- Modify: the store command registrar (grep `NewRebuildIndexesCommand` to find where subcommands are added, e.g. `cmd/ledgerctl/store/store.go` or `main.go`).
- Test: `cmd/ledgerctl/store/rebuild_audit_index_test.go` (optional smoke test mirroring existing store command tests)

- [ ] **Step 1: Implement the command** (modeled on `rebuild_indexes.go`)

Create `cmd/ledgerctl/store/rebuild_audit_index.go`:

```go
package store

import (
	"context"
	"fmt"
	"path/filepath"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
	"go.opentelemetry.io/otel/metric/noop"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger/v3/internal/application/auditindexer"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/readstore"
)

// NewRebuildAuditIndexCommand creates the store rebuild-audit-index command.
func NewRebuildAuditIndexCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "rebuild-audit-index",
		Short: "Rebuild the audit secondary index from the Audit zone (offline)",
		Long: `Drop the audit secondary index and replay every audit entry from
the Audit zone to rebuild it from scratch. Purely offline — no server needed.`,
		RunE:              runRebuildAuditIndex,
		Args:              cobra.ExactArgs(0),
		ValidArgsFunction: cobra.NoFileCompletions,
	}

	cmd.Flags().String("data-dir", "", "Pebble data directory (required)")
	cmd.Flags().String("read-index-dir", "", "Read index directory (default: <data-dir>/read-indexes/)")
	cmd.Flags().Int("audit-index-batch-size", 0, "Audit entries per Pebble batch commit (0 = default 1000)")
	_ = cmd.MarkFlagRequired("data-dir")

	return cmd
}

func runRebuildAuditIndex(cmd *cobra.Command, _ []string) error {
	var (
		dataDir, _      = cmd.Flags().GetString("data-dir")
		readIndexDir, _ = cmd.Flags().GetString("read-index-dir")
		batchSize, _    = cmd.Flags().GetInt("audit-index-batch-size")
	)
	if readIndexDir == "" {
		readIndexDir = filepath.Join(dataDir, "read-indexes")
	}

	logger := logging.NopZap()

	spinner, _ := pterm.DefaultSpinner.Start("Opening Pebble store (read-only)...")
	pebbleStore, err := dal.OpenReadOnly(dataDir, logger)
	if err != nil {
		spinner.Fail("Failed to open Pebble store")
		return cmdutil.Displayed(fmt.Errorf("opening Pebble store: %w", err))
	}
	defer func() { _ = pebbleStore.Close() }()
	spinner.Success("Pebble store opened")

	spinner, _ = pterm.DefaultSpinner.Start("Opening read index store...")
	rs, err := readstore.New(readIndexDir, logger, readstore.DefaultConfig())
	if err != nil {
		spinner.Fail("Failed to open read index store")
		return cmdutil.Displayed(fmt.Errorf("opening read index store: %w", err))
	}
	defer func() { _ = rs.Close() }()
	spinner.Success("Read index store opened at " + rs.Path())

	spinner, _ = pterm.DefaultSpinner.Start("Rebuilding audit index...")
	idx := auditindexer.New(auditindexer.Config{BatchSize: batchSize}, pebbleStore, rs, logger, noop.Meter{})
	if err := idx.Rebuild(context.Background()); err != nil {
		spinner.Fail("Rebuild failed")
		return cmdutil.Displayed(fmt.Errorf("rebuilding audit index: %w", err))
	}
	spinner.Success("Audit index rebuild complete")

	return nil
}
```

Note: `dal.OpenReadOnly` returns a read-only store. The worker only reads the main store and writes readstore, so read-only is correct here.

- [ ] **Step 2: Register the subcommand**

Find where `NewRebuildIndexesCommand()` is added to the `store` parent command (grep it) and add `<parent>.AddCommand(NewRebuildAuditIndexCommand())` right next to it.

- [ ] **Step 3: Verify compilation**

Run: `GOROOT= go build ./cmd/ledgerctl/...`
Expected: builds clean.

- [ ] **Step 4: Smoke test the CLI**

Run: `GOROOT= go run ./cmd/ledgerctl store rebuild-audit-index --help`
Expected: prints usage with the three flags.

- [ ] **Step 5: Commit**

```bash
git add cmd/ledgerctl/store/rebuild_audit_index.go cmd/ledgerctl/store/*.go
git commit -m "feat(EN-1339): ledgerctl store rebuild-audit-index command"
```

---

## Task 11: Sustained-load lag integration test

**Files:**
- Modify: `internal/application/auditindexer/indexer_test.go`

- [ ] **Step 1: Write the test**

Append a test that starts the worker, writes a steady stream of audit entries, and asserts the index converges (lag returns to 0) without losing entries:

```go
func TestIndexerKeepsUpUnderLoad(t *testing.T) {
	t.Parallel()
	idx, mainStore, rs := newIndexerForTest(t)
	idx.Start()
	t.Cleanup(idx.Stop)

	const total = 200
	for s := uint64(1); s <= total; s++ {
		writeAuditEntry(t, mainStore, &auditpb.AuditEntry{
			Sequence: s, ProposalId: s, Timestamp: &commonpb.Timestamp{Seconds: int64(s)},
			Outcome: &auditpb.AuditEntry_Success{Success: &auditpb.AuditSuccess{}},
			Ledgers: []string{"main"},
		})
	}

	require.Eventually(t, func() bool {
		c, err := rs.ReadAuditProgress()
		return err == nil && c == total
	}, 10*time.Second, 50*time.Millisecond)

	seqs, err := rs.AuditSeqsByString(readstore.AuditFieldLedger, "main")
	require.NoError(t, err)
	require.Len(t, seqs, total)
}
```

- [ ] **Step 2: Run the test**

Run: `GOROOT= go test ./internal/application/auditindexer/ -run TestIndexerKeepsUp -v`
Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add internal/application/auditindexer/indexer_test.go
git commit -m "test(EN-1339): audit indexer keeps up under sustained load"
```

---

## Task 12: Docs + final gate

**Files:**
- Modify: `docs/ops/cli.md` (document `rebuild-audit-index`), `docs/technical/contributing/api-comparison.md` only if any endpoint changed (it does not — reader wiring is EN-1305).

- [ ] **Step 1: Document the CLI command**

Add a `rebuild-audit-index` entry to `docs/ops/cli.md` under the store commands, mirroring the `rebuild-indexes` entry: purpose (offline drop+replay of the audit index), flags (`--data-dir`, `--read-index-dir`, `--audit-index-batch-size`), and when to use it (corruption / post-restore). Add the new server flags (`--audit-index-batch-size`, `--audit-index-rebuild-threshold`, `--disable-audit-index`) to the server flag reference.

- [ ] **Step 2: Run the full pre-commit gate**

Run: `nix develop --command bash -c "just pre-commit"`
Expected: exit 0 and a clean tree (re-run until idempotent — `go generate`/`go mod tidy`/`golangci-lint --fix` may modify files).

- [ ] **Step 3: Run the test suite**

Run: `nix develop --command bash -c "just test"`
Expected: green. Also explicitly: `GOROOT= go test ./internal/application/auditindexer/... ./internal/storage/readstore/... ./internal/domain/... -v`.

- [ ] **Step 4: Verify the no-change invariants hold**

Run: `git diff --name-only origin/main` and confirm **no** changes to `internal/application/check/checker.go`, the FSM apply path (`internal/infra/state/machine*.go`, `internal/infra/state/write_set*.go`), `internal/infra/preload/`, or `dal.WriteSession` (`internal/storage/dal/batch.go`). If any appear, they are out of scope and must be reverted/justified.

- [ ] **Step 5: Commit**

```bash
git add docs/
git commit -m "docs(EN-1339): document audit index rebuild command and flags"
```

---

## Self-Review

**Spec coverage:**
- New audit-index keyspace + documented layout → Task 1.
- Async worker following Audit zone via persisted cursor → Tasks 5, 7.
- Rebuild via ledgerctl + auto on boot when cursor missing/behind threshold → Tasks 6, 10.
- All listed fields with seek+range semantics → Tasks 1, 2 (helpers), 4 (extraction).
- Integration tests (catch-up, lag, rebuild parity, restart resume) → Tasks 5, 6, 7, 11.
- No checker change / no FSM-path change → Task 12 Step 4 (explicit guard).
- Order-type token vocabulary shared with EN-1305 → Task 3.
- Comparer risk → resolved by `PrefixInternal` namespacing, asserted in Task 1 Step 1.

**Known verification points the implementer MUST confirm against generated code (named inline):**
- `cursor.Cursor` method names used in Task 5 (`HasMore`/`Next`/`Close`).
- `raftcmdpb` oneof wrapper type names in Task 3.
- `auditpb.AuditEntry_Success`/`_Failure`, `AuditSuccess`/`AuditFailure`, `commonpb.Timestamp` fields in Task 4.
- `dal.NewStore`/`OpenWriteSession`/`NewDirectReadHandle`/`OpenReadOnly` signatures in Tasks 5, 10.
- Exact flag-mapping site in `cmd/server` (Task 8 Step 3) and store subcommand registrar (Task 10 Step 2).

These are signature confirmations, not design gaps; each step says what to grep and how to adapt.

**Type consistency:** `AuditField*` byte constants, `appendEntryKeys(kb, emit, entry, items)`, `Indexer`/`Config`/`New`/`ProcessOnce`/`Rebuild`/`Start`/`Stop`, and the readstore `AuditSeqsBy*`/`ReadAuditProgress`/`WriteAuditProgress`/`DropAuditIndex` names are used identically across tasks.
