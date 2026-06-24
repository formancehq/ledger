package state

import (
	"encoding/binary"
	"errors"
	"slices"

	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/signaturepb"
)

// ErrAuditEntryMissingOutcome is returned by BuildHashedHeaderPayload when the
// caller-supplied AuditEntry has neither a Success nor a Failure outcome set.
// The apply path always populates one of the two; reaching this state at
// apply time is an FSM bug, and at verify time it means a persisted entry
// was tampered to wipe its outcome.
var ErrAuditEntryMissingOutcome = errors.New("audit entry has no outcome (neither success nor failure)")

// Audit hash envelope — canonical binary encoding of AuditEntry and AuditItem
// fields that feed the audit hash chain.
//
// The chain hashes opaque bytes, never a proto. We assemble those bytes here
// in a stable, language-agnostic format:
//
//   - Every integer is big-endian (u8, u32, u64).
//   - Every variable-length blob (string, sub-payload, repeated entry) is
//     prefixed by its length as u32 BE. Empty blobs encode as the bare prefix
//     `0x00000000`, so "absent string" and "empty string" are bytes-equal —
//     the proto already conflates these (zero value), and the hash inherits
//     that property.
//   - Every map / repeated field is sorted by its key (or by element value
//     for plain string slices) before encoding. This makes the envelope
//     independent of Go's randomized map iteration and from any
//     implementation that would persist the entries in insertion order.
//
// The full spec lives in docs/ops/correctness.md and is mirrored by the
// hand-rolled golden test in internal/domain/processing/hash_golden_test.go —
// any drift between code and spec trips the golden test.
//
// CallerIdentity.source oneof tags:.
const (
	callerSourceNone   byte = 0
	callerSourceIssuer byte = 1
	callerSourceKeyID  byte = 2
)

// outcome_tag values in HashedHeaderPayload.
const (
	outcomeTagSuccess byte = 0
	outcomeTagFailure byte = 1
)

// appendLenBytes appends `len(value)` as u32 BE then the bytes themselves.
// Used for every variable-length field in the envelope.
func appendLenBytes(buf []byte, value []byte) []byte {
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(value)))
	buf = append(buf, lenBuf[:]...)
	buf = append(buf, value...)

	return buf
}

// appendLenString is a string-flavoured wrapper around appendLenBytes.
func appendLenString(buf []byte, s string) []byte {
	return appendLenBytes(buf, []byte(s))
}

// appendU32 / appendU64 / appendU8 keep the call sites readable.
func appendU8(buf []byte, v byte) []byte { return append(buf, v) }

func appendU32(buf []byte, v uint32) []byte {
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], v)

	return append(buf, lenBuf[:]...)
}

func appendU64(buf []byte, v uint64) []byte {
	var lenBuf [8]byte
	binary.BigEndian.PutUint64(lenBuf[:], v)

	return append(buf, lenBuf[:]...)
}

// BuildHashedHeaderPayload returns the canonical bytes that bind every
// AuditEntry field except `hash` (the chain output itself). The caller
// is responsible for filling the entry completely — Sequence, Timestamp,
// ProposalId, Outcome, OrderCount, Ledgers, HashVersion, CallerSnapshot —
// BEFORE invoking this function. The result feeds the hash pre-image at
// the apply side and is rebuilt on the verifier side, so any tampering
// with a typed field is structurally detectable. `entry.Items` is
// intentionally NOT bound here: items are stored under their own Pebble
// keys and bound separately via BuildPerItemPayload.
//
// Returns ErrAuditEntryMissingOutcome if the entry has neither a success
// nor a failure outcome. Apply path callers treat this as a fatal FSM
// bug; verifier callers treat it as a tampering signal.
func BuildHashedHeaderPayload(entry *auditpb.AuditEntry) ([]byte, error) {
	buf := make([]byte, 0, 128)

	buf = appendU64(buf, entry.GetSequence())
	buf = appendU64(buf, entry.GetTimestamp().GetData())
	buf = appendU64(buf, entry.GetProposalId())
	buf = appendU32(buf, entry.GetOrderCount())
	buf = appendU32(buf, entry.GetHashVersion())

	ledgers := slices.Clone(entry.GetLedgers())
	slices.Sort(ledgers)
	buf = appendU32(buf, uint32(len(ledgers)))

	for _, ledger := range ledgers {
		buf = appendLenString(buf, ledger)
	}

	// Outcome: tag + length-prefixed payload. No silent fallback — the
	// apply path always sets exactly one outcome, so a missing outcome
	// here is either an FSM regression (apply side) or persistence
	// tampering (verifier side). Either way, surface it loudly.
	switch out := entry.GetOutcome().(type) {
	case *auditpb.AuditEntry_Success:
		buf = appendU8(buf, outcomeTagSuccess)
		buf = appendLenBytes(buf, buildAuditSuccessPayload(out.Success))
	case *auditpb.AuditEntry_Failure:
		buf = appendU8(buf, outcomeTagFailure)
		buf = appendLenBytes(buf, buildAuditFailurePayload(out.Failure))
	default:
		return nil, ErrAuditEntryMissingOutcome
	}

	// CallerSnapshot: length-prefixed sub-payload (0-length when absent).
	if snap := entry.GetCallerSnapshot(); snap != nil {
		buf = appendLenBytes(buf, buildCallerSnapshotPayload(snap))
	} else {
		buf = appendLenBytes(buf, nil)
	}

	// Batch identity: the idempotency key, then the signature sub-payload
	// (0-length when unsigned). Binding both makes the AppliedProposal
	// idempotency projection and the batch's non-repudiation proof
	// tamper-evident via the chain.
	buf = appendLenString(buf, entry.GetIdempotency().GetKey())
	buf = appendLenBytes(buf, buildSignaturePayload(entry.GetSignature()))

	return buf, nil
}

// buildSignaturePayload encodes a SignedApplyBatch as key_id || signature ||
// payload, each length-prefixed. Returns nil for an unsigned (nil) batch, which
// the caller length-prefixes to a bare 0x00000000 — bytes-equal to an empty
// signature, matching the envelope's absent/empty conflation.
func buildSignaturePayload(sb *signaturepb.SignedApplyBatch) []byte {
	if sb == nil {
		return nil
	}

	buf := make([]byte, 0, 64)
	buf = appendLenString(buf, sb.GetKeyId())
	buf = appendLenBytes(buf, sb.GetSignature())
	buf = appendLenBytes(buf, sb.GetPayload())

	return buf
}

func buildAuditSuccessPayload(s *auditpb.AuditSuccess) []byte {
	buf := make([]byte, 0, 16)

	buf = appendU64(buf, s.GetMinLogSequence())
	buf = appendU64(buf, s.GetMaxLogSequence())

	return buf
}

func buildAuditFailurePayload(f *auditpb.AuditFailure) []byte {
	buf := make([]byte, 0, 64)

	buf = appendU32(buf, uint32(f.GetReason()))
	buf = appendLenString(buf, f.GetMessage())

	ctx := f.GetContext()
	keys := make([]string, 0, len(ctx))

	for k := range ctx {
		keys = append(keys, k)
	}

	slices.Sort(keys)
	buf = appendU32(buf, uint32(len(keys)))

	for _, k := range keys {
		buf = appendLenString(buf, k)
		buf = appendLenString(buf, ctx[k])
	}

	return buf
}

func buildCallerSnapshotPayload(snap *commonpb.CallerSnapshot) []byte {
	buf := make([]byte, 0, 64)

	id := snap.GetIdentity()
	buf = appendLenString(buf, id.GetSubject())

	// Source oneof: tag byte + length-prefixed value. Switch on the
	// oneof wrapper type, NOT on the inner string value — otherwise a
	// caller with Source set but value empty (e.g.
	// CallerIdentity_Issuer{Issuer: ""}) is indistinguishable from
	// Source absent, and an attacker could swap one for the other
	// without breaking the envelope.
	switch src := id.GetSource().(type) {
	case *commonpb.CallerIdentity_Issuer:
		buf = appendU8(buf, callerSourceIssuer)
		buf = appendLenString(buf, src.Issuer)
	case *commonpb.CallerIdentity_KeyId:
		buf = appendU8(buf, callerSourceKeyID)
		buf = appendLenString(buf, src.KeyId)
	default:
		buf = appendU8(buf, callerSourceNone)
		buf = appendLenBytes(buf, nil)
	}

	if snap.GetGod() {
		buf = appendU8(buf, 1)
	} else {
		buf = appendU8(buf, 0)
	}

	scopes := slices.Clone(snap.GetScopes())
	slices.Sort(scopes)
	buf = appendU32(buf, uint32(len(scopes)))

	for _, scope := range scopes {
		buf = appendLenString(buf, scope)
	}

	return buf
}

// BuildPerItemPayload returns the canonical bytes that bind every AuditItem
// field in the audit hash chain. The chain pre-image is the concatenation of
// these payloads (one per item, in order_index order). Both the apply path
// and the checker call this function with the AuditItem they have on hand;
// any tampering with order_index, log_sequence, or serialized_order changes
// the bytes and trips the hash check.
func BuildPerItemPayload(item *auditpb.AuditItem) []byte {
	buf := make([]byte, 0, 32+len(item.GetSerializedOrder()))

	buf = appendU32(buf, item.GetOrderIndex())
	buf = appendU64(buf, item.GetLogSequence())
	buf = appendLenBytes(buf, item.GetSerializedOrder())

	return buf
}
