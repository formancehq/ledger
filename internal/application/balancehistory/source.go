// Package balancehistory builds the rebuildable monetary history used by
// historical balance queries.
package balancehistory

import (
	"bytes"
	"context"

	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// Position identifies a verified, consecutive prefix of the audit and log
// streams. AuditSequence is the builder cursor. LogSequence is the greatest
// freshly created log covered by that prefix and is carried separately because
// proposals that fail or produce no log still advance the audit cursor.
// AuditHash anchors restart verification and detects a restored source that
// diverged at the same audit sequence.
type Position struct {
	AuditSequence uint64
	LogSequence   uint64
	AuditHash     []byte
}

func (p Position) equal(other Position) bool {
	return p.AuditSequence == other.AuditSequence &&
		p.LogSequence == other.LogSequence &&
		bytes.Equal(p.AuditHash, other.AuditHash)
}

// VerifiedProposal is one structurally complete proposal read from the audit
// source. Items are ordered by OrderIndex. Logs has exactly the same length as
// Items: Logs[i] is nil when Items[i].LogSequence is zero, otherwise it is the
// verified log referenced by that item. A non-nil log may be an older
// idempotent reference below AuditSuccess.MinLogSequence; builders validate it
// structurally but only reduce the fresh inclusive min/max range.
type VerifiedProposal struct {
	Entry *auditpb.AuditEntry
	Items []*auditpb.AuditItem
	Logs  []*commonpb.Log
}

// Batch is an immutable, consecutive source slice. Next is safe to persist
// only when Read returned no error. Head belongs to the same source snapshot,
// so callers can tell whether another bounded catch-up slice is required.
type Batch struct {
	Proposals []VerifiedProposal
	Next      Position
	Head      Position
}

// Source supplies complete proposals independently of their physical hot or
// cold location. The current composite adapter stitches archived audit chapters
// with the hot suffix without archiving the projection itself.
//
//go:generate mockgen -typed -write_source_comment=false -write_package_comment=false -source=source.go -destination=source_generated_test.go -package=balancehistory
type Source interface {
	// Head returns the greatest audit/log prefix visible in one source snapshot.
	Head(ctx context.Context) (Position, error)

	// Read returns at most maxProposals proposals strictly after after. A
	// proposal is never split across batches. Any missing or invalid source row
	// fails the whole read, so the returned Next must not be persisted on error.
	Read(ctx context.Context, after Position, maxProposals int) (Batch, error)
}

// ErrSourceMissing means the adapter cannot prove a consecutive source prefix.
// This includes an absent audit entry, item, or referenced log. A hot+cold
// adapter may recover by resolving the missing audit range from a chapter archive.
type ErrSourceMissing struct {
	Detail string
}

func (e *ErrSourceMissing) Error() string {
	if e.Detail == "" {
		return "balance history source is missing"
	}

	return "balance history source is missing: " + e.Detail
}

// ErrSourceInvalid means source rows are present but violate the structural
// audit contract. Retrying the same physical source cannot repair the problem.
type ErrSourceInvalid struct {
	Detail string
}

func (e *ErrSourceInvalid) Error() string {
	if e.Detail == "" {
		return "balance history source is invalid"
	}

	return "balance history source is invalid: " + e.Detail
}
