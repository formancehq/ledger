package http

import (
	"github.com/formancehq/go-libs/v5/pkg/types/time"

	"github.com/formancehq/ledger/v3/internal/domain/indexes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// This file holds the HTTP response DTOs for the index surface. They exist so
// the public wire is owned by this package rather than derived from the .proto
// descriptor by protojson (which emits quoted uint64, {"data":<micros>}
// timestamps and drops meaningful zeros).
//
// The proto types deliberately carry NO MarshalJSON: cmd/ledgerctl/cmdutil
// prefers a custom marshaller when one exists, and misc/operator parses
// `ledgerctl indexes list --json` against the protojson shape in a separate Go
// module that a root `go build ./...` never compiles. Adding one there would
// break the operator silently. See EN-1791 and
// docs/technical/architecture/subsystems/api/http-api.md.
//
// Naming: the EN-1791 response DTOs use the `xxxDTO` type / `newXxxDTO`
// constructor convention, matching the `dto_*.go` filenames. The rest of the
// package predates it and uses `XxxJSON` / `toXxxJSON`; new EN-1791 files
// should follow the DTO convention rather than introduce a third style.

// indexIDDTO renders the IndexID oneof. At most one field is non-nil: an unset
// or unknown kind yields `{}`, which canonicalId still reports as `unknown`.
//
// The enum variants are *string, not string, so that "this variant is set" is
// encoded explicitly by the pointer. A bare string with omitempty would happen
// to work today only because the generated enum stringer never returns "" —
// including for value 0, which is a real member here (TX_BUILTIN_INDEX_REFERENCE).
// Relying on that would couple the wire format to an implementation detail of
// protoc's stringer; the pointer states the intent directly instead.
type indexIDDTO struct {
	TxBuiltin      *string             `json:"txBuiltin,omitempty"`
	LogBuiltin     *string             `json:"logBuiltin,omitempty"`
	AccountBuiltin *string             `json:"accountBuiltin,omitempty"`
	Metadata       *metadataIndexIDDTO `json:"metadata,omitempty"`
}

type metadataIndexIDDTO struct {
	// No omitempty: the target is always emitted so the (target, key) pair is
	// complete. Value 0 is TARGET_TYPE_ACCOUNT, but omitempty would not have
	// dropped it either — the stringer never yields "" — so it is a no-op here.
	Target string `json:"target"`
	// No omitempty: a metadata index is identified by its key, so the key is
	// always emitted — an absent one would make the entry unidentifiable.
	Key string `json:"key"`
}

type indexDTO struct {
	ID *indexIDDTO `json:"id,omitempty"`
	// CanonicalID is the same string the per-index routes accept as
	// {canonicalId}, so a list item carries the address of its own detail route.
	CanonicalID string `json:"canonicalId"`
	// No omitempty: 0 is INDEX_BUILD_STATUS_UNSPECIFIED and clients branch on it.
	BuildStatus string `json:"buildStatus"`
	// No omitempty: "" means bucket-scoped, which differs from absent.
	Ledger string `json:"ledger"`
	// No omitempty: version 0 is a legitimate encoding version, and the field is
	// always emitted so it can sit in the OpenAPI `required` list.
	ForwardEncodingVersion uint32  `json:"forwardEncodingVersion"`
	CreatedAt              *string `json:"createdAt,omitempty"`
}

type indexEntryDTO struct {
	// No omitempty: "" means the entry is bucket-scoped rather than owned by a
	// named ledger, same distinction as indexDTO.Ledger.
	Ledger string    `json:"ledger"`
	Index  *indexDTO `json:"index,omitempty"`
	// Unquoted number, unlike protojson's quoted fixed64.
	Cursor uint64 `json:"cursor"`
	// No omitempty on either version: 0 = never built is the documented poll
	// signal (misc/proto/bucket.proto), and 0 = idle for pending.
	CurrentVersion uint32 `json:"currentVersion"`
	PendingVersion uint32 `json:"pendingVersion"`
}

type indexStatusDTO struct {
	// No omitempty on either sequence: 0 means nothing has been indexed or
	// logged yet, a real state on a fresh bucket rather than a missing value.
	LastIndexedSequence uint64 `json:"lastIndexedSequence"`
	LastLogSequence     uint64 `json:"lastLogSequence"`
	// No omitempty: lag 0 means fully caught up, the healthy case.
	Lag uint64 `json:"lag"`
	// No omitempty: 0 is a real size for an index that holds no rows yet.
	IndexFileSize uint64 `json:"indexFileSize"`
	// Allocated by the converter so it marshals as [] rather than null.
	Indexes []indexEntryDTO `json:"indexes"`
}

// formatTimestamp renders a commonpb.Timestamp as RFC3339Nano.
//
// The DTO holds a string rather than the *commonpb.Timestamp itself so this
// package owns the rendering. commonpb.Timestamp.MarshalJSON is also what the
// CLI and, transitively, misc/operator consume, so reaching for it here would
// tie the HTTP wire to a cross-module contract; a plain RFC3339Nano string also
// maps directly onto OpenAPI's `type: string, format: date-time`.
// Deliberately mirrors commonpb.Timestamp.MarshalJSON rather than calling it —
// the two are allowed to diverge.
func formatTimestamp(ts *commonpb.Timestamp) *string {
	if ts == nil {
		return nil
	}

	s := ts.AsTime().Format(time.RFC3339Nano)

	return &s
}

// newIndexIDDTO hand-dispatches the oneof. There is no tag-driven path: the
// generated wrapper structs carry only protobuf: tags, so a reflective marshal
// would emit {"Kind":{"TxBuiltin":1}}.
func newIndexIDDTO(id *commonpb.IndexID) *indexIDDTO {
	if id == nil {
		return nil
	}

	dto := &indexIDDTO{}

	switch k := id.GetKind().(type) {
	case *commonpb.IndexID_TxBuiltin:
		v := k.TxBuiltin.String()
		dto.TxBuiltin = &v
	case *commonpb.IndexID_LogBuiltin:
		v := k.LogBuiltin.String()
		dto.LogBuiltin = &v
	case *commonpb.IndexID_AccountBuiltin:
		v := k.AccountBuiltin.String()
		dto.AccountBuiltin = &v
	case *commonpb.IndexID_Metadata:
		dto.Metadata = &metadataIndexIDDTO{
			Target: k.Metadata.GetTarget().String(),
			Key:    k.Metadata.GetKey(),
		}
	}

	return dto
}

func newIndexDTO(idx *commonpb.Index) *indexDTO {
	if idx == nil {
		return nil
	}

	return &indexDTO{
		ID:                     newIndexIDDTO(idx.GetId()),
		CanonicalID:            indexes.Canonical(idx.GetId()),
		BuildStatus:            idx.GetBuildStatus().String(),
		Ledger:                 idx.GetLedger(),
		ForwardEncodingVersion: idx.GetForwardEncodingVersion(),
		CreatedAt:              formatTimestamp(idx.GetCreatedAt()),
	}
}

// newIndexDTOList converts a slice, allocating so an empty input marshals as
// [] rather than null.
func newIndexDTOList(src []*commonpb.Index) []indexDTO {
	out := make([]indexDTO, 0, len(src))

	for _, idx := range src {
		dto := newIndexDTO(idx)
		// A nil element would be a backend bug. Skip rather than deref: this is a
		// read-only ops surface, so degrading the list beats a 500, and nothing can
		// desync from it.
		if dto == nil {
			continue
		}

		out = append(out, *dto)
	}

	return out
}

func newIndexEntryDTO(entry *servicepb.IndexEntry) *indexEntryDTO {
	if entry == nil {
		return nil
	}

	return &indexEntryDTO{
		Ledger:         entry.GetLedger(),
		Index:          newIndexDTO(entry.GetIndex()),
		Cursor:         entry.GetCursor(),
		CurrentVersion: entry.GetCurrentVersion(),
		PendingVersion: entry.GetPendingVersion(),
	}
}

func newIndexStatusDTO(resp *servicepb.GetIndexStatusResponse) *indexStatusDTO {
	if resp == nil {
		return nil
	}

	entries := make([]indexEntryDTO, 0, len(resp.GetIndexes()))

	for _, e := range resp.GetIndexes() {
		dto := newIndexEntryDTO(e)
		// A nil element would be a backend bug. Skip rather than deref: this is a
		// read-only ops surface, so degrading the list beats a 500, and nothing can
		// desync from it.
		if dto == nil {
			continue
		}

		entries = append(entries, *dto)
	}

	return &indexStatusDTO{
		LastIndexedSequence: resp.GetLastIndexedSequence(),
		LastLogSequence:     resp.GetLastLogSequence(),
		Lag:                 resp.GetLag(),
		IndexFileSize:       resp.GetIndexFileSize(),
		Indexes:             entries,
	}
}
