package processing

import (
	"encoding/binary"
	"io"
	"sort"
	"unsafe"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/zeebo/blake3"
)

// logHasher writes proto message fields directly into an io.Writer for hashing.
// This avoids proto.Marshal allocations in the hot path by feeding fields
// one-by-one into the blake3 hasher.
type logHasher struct {
	w   io.Writer
	buf [8]byte // scratch buffer for fixed-size integers
}

// --- Primitive writers ---

func (h *logHasher) writeUint64(v uint64) {
	binary.LittleEndian.PutUint64(h.buf[:8], v)
	_, _ = h.w.Write(h.buf[:8])
}

func (h *logHasher) writeUint32(v uint32) {
	binary.LittleEndian.PutUint32(h.buf[:4], v)
	_, _ = h.w.Write(h.buf[:4])
}

func (h *logHasher) writeString(s string) {
	binary.LittleEndian.PutUint32(h.buf[:4], uint32(len(s)))
	_, _ = h.w.Write(h.buf[:4])
	if len(s) > 0 {
		_, _ = h.w.Write(unsafe.Slice(unsafe.StringData(s), len(s)))
	}
}

func (h *logHasher) writeBytes(b []byte) {
	binary.LittleEndian.PutUint32(h.buf[:4], uint32(len(b)))
	_, _ = h.w.Write(h.buf[:4])
	if len(b) > 0 {
		_, _ = h.w.Write(b)
	}
}

func (h *logHasher) writeBool(v bool) {
	if v {
		h.buf[0] = 1
	} else {
		h.buf[0] = 0
	}
	_, _ = h.w.Write(h.buf[:1])
}

func (h *logHasher) writePresence(present bool) {
	h.writeBool(present)
}

func (h *logHasher) writeDiscriminator(d byte) {
	h.buf[0] = d
	_, _ = h.w.Write(h.buf[:1])
}

// --- Message hashers ---

func (h *logHasher) hashLog(log *commonpb.Log) {
	// Hash fields: sequence, payload, idempotency (excludes hash field 4)
	h.writeUint64(log.Sequence)
	h.hashLogPayload(log.Payload)
	h.hashIdempotency(log.Idempotency)
}

func (h *logHasher) hashLogPayload(p *commonpb.LogPayload) {
	if p == nil {
		h.writePresence(false)
		return
	}
	h.writePresence(true)
	switch v := p.Type.(type) {
	case *commonpb.LogPayload_CreateLedger:
		h.writeDiscriminator(1)
		h.hashCreateLedgerLog(v.CreateLedger)
	case *commonpb.LogPayload_DeleteLedger:
		h.writeDiscriminator(2)
		h.hashDeleteLedgerLog(v.DeleteLedger)
	case *commonpb.LogPayload_Apply:
		h.writeDiscriminator(3)
		h.hashApplyLedgerLog(v.Apply)
	default:
		h.writeDiscriminator(0)
	}
}

func (h *logHasher) hashCreateLedgerLog(cl *commonpb.CreateLedgerLog) {
	if cl == nil {
		h.writePresence(false)
		return
	}
	h.writePresence(true)
	h.hashLedgerInfo(cl.Info)
}

func (h *logHasher) hashDeleteLedgerLog(dl *commonpb.DeleteLedgerLog) {
	if dl == nil {
		h.writePresence(false)
		return
	}
	h.writePresence(true)
	h.hashLedgerInfo(dl.Info)
}

func (h *logHasher) hashLedgerInfo(info *commonpb.LedgerInfo) {
	if info == nil {
		h.writePresence(false)
		return
	}
	h.writePresence(true)
	h.writeString(info.Name)
	h.hashTimestamp(info.CreatedAt)
	h.writeUint32(info.Id)
	h.hashTimestamp(info.DeletedAt)
}

func (h *logHasher) hashApplyLedgerLog(a *commonpb.ApplyLedgerLog) {
	if a == nil {
		h.writePresence(false)
		return
	}
	h.writePresence(true)
	h.writeString(a.LedgerName)
	h.hashLedgerLog(a.Log)
}

func (h *logHasher) hashLedgerLog(ll *commonpb.LedgerLog) {
	if ll == nil {
		h.writePresence(false)
		return
	}
	h.writePresence(true)
	h.hashLedgerLogPayload(ll.Data)
	h.hashTimestamp(ll.Date)
	h.writeUint64(ll.Id)
}

func (h *logHasher) hashLedgerLogPayload(p *commonpb.LedgerLogPayload) {
	if p == nil {
		h.writePresence(false)
		return
	}
	h.writePresence(true)
	switch v := p.Payload.(type) {
	case *commonpb.LedgerLogPayload_CreatedTransaction:
		h.writeDiscriminator(1)
		h.hashCreatedTransaction(v.CreatedTransaction)
	case *commonpb.LedgerLogPayload_RevertedTransaction:
		h.writeDiscriminator(2)
		h.hashRevertedTransaction(v.RevertedTransaction)
	case *commonpb.LedgerLogPayload_SavedMetadata:
		h.writeDiscriminator(3)
		h.hashSavedMetadata(v.SavedMetadata)
	case *commonpb.LedgerLogPayload_DeletedMetadata:
		h.writeDiscriminator(4)
		h.hashDeletedMetadata(v.DeletedMetadata)
	default:
		h.writeDiscriminator(0)
	}
}

func (h *logHasher) hashCreatedTransaction(ct *commonpb.CreatedTransaction) {
	if ct == nil {
		h.writePresence(false)
		return
	}
	h.writePresence(true)
	h.hashTransaction(ct.Transaction)
	// map<string, MetadataSet> — sorted iteration for determinism
	h.writeUint32(uint32(len(ct.AccountMetadata)))
	if len(ct.AccountMetadata) > 0 {
		keys := make([]string, 0, len(ct.AccountMetadata))
		for k := range ct.AccountMetadata {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			h.writeString(k)
			h.hashMetadataSet(ct.AccountMetadata[k])
		}
	}
}

func (h *logHasher) hashTransaction(tx *commonpb.Transaction) {
	if tx == nil {
		h.writePresence(false)
		return
	}
	h.writePresence(true)
	// repeated Posting
	h.writeUint32(uint32(len(tx.Postings)))
	for _, p := range tx.Postings {
		h.hashPosting(p)
	}
	h.hashMetadataSet(tx.Metadata)
	h.hashTimestamp(tx.Timestamp)
	h.writeString(tx.Reference)
	h.writeUint64(tx.Id)
	h.writeBool(tx.Reverted)
	h.hashTimestamp(tx.InsertedAt)
	h.hashTimestamp(tx.UpdatedAt)
	h.hashTimestamp(tx.RevertedAt)
}

func (h *logHasher) hashPosting(p *commonpb.Posting) {
	if p == nil {
		h.writePresence(false)
		return
	}
	h.writePresence(true)
	h.writeString(p.Source)
	h.writeString(p.Destination)
	h.hashBigInt(p.Amount)
	h.writeString(p.Asset)
}

func (h *logHasher) hashBigInt(bi *commonpb.BigInt) {
	if bi == nil {
		h.writePresence(false)
		return
	}
	h.writePresence(true)
	h.writeBytes(bi.Data)
}

func (h *logHasher) hashRevertedTransaction(rt *commonpb.RevertedTransaction) {
	if rt == nil {
		h.writePresence(false)
		return
	}
	h.writePresence(true)
	h.writeUint64(rt.RevertedTransactionId)
	h.hashTransaction(rt.RevertTransaction)
}

func (h *logHasher) hashSavedMetadata(sm *commonpb.SavedMetadata) {
	if sm == nil {
		h.writePresence(false)
		return
	}
	h.writePresence(true)
	h.hashTarget(sm.Target)
	h.hashMetadataSet(sm.Metadata)
}

func (h *logHasher) hashDeletedMetadata(dm *commonpb.DeletedMetadata) {
	if dm == nil {
		h.writePresence(false)
		return
	}
	h.writePresence(true)
	h.hashTarget(dm.Target)
	h.writeString(dm.Key)
}

func (h *logHasher) hashTarget(t *commonpb.Target) {
	if t == nil {
		h.writePresence(false)
		return
	}
	h.writePresence(true)
	switch v := t.Target.(type) {
	case *commonpb.Target_Account:
		h.writeDiscriminator(1)
		if v.Account != nil {
			h.writePresence(true)
			h.writeString(v.Account.Addr)
		} else {
			h.writePresence(false)
		}
	case *commonpb.Target_Transaction:
		h.writeDiscriminator(2)
		if v.Transaction != nil {
			h.writePresence(true)
			h.writeUint64(v.Transaction.Id)
		} else {
			h.writePresence(false)
		}
	default:
		h.writeDiscriminator(0)
	}
}

func (h *logHasher) hashIdempotency(i *commonpb.Idempotency) {
	if i == nil {
		h.writePresence(false)
		return
	}
	h.writePresence(true)
	h.writeString(i.Key)
}

func (h *logHasher) hashMetadataSet(ms *commonpb.MetadataSet) {
	if ms == nil {
		h.writePresence(false)
		return
	}
	h.writePresence(true)
	h.writeUint32(uint32(len(ms.Metadata)))
	for _, m := range ms.Metadata {
		h.hashMetadata(m)
	}
}

func (h *logHasher) hashMetadata(m *commonpb.Metadata) {
	if m == nil {
		h.writePresence(false)
		return
	}
	h.writePresence(true)
	h.writeString(m.Key)
	h.hashMetadataValue(m.Value)
}

func (h *logHasher) hashMetadataValue(mv *commonpb.MetadataValue) {
	if mv == nil {
		h.writePresence(false)
		return
	}
	h.writePresence(true)
	h.writeString(mv.Value)
}

func (h *logHasher) hashTimestamp(ts *commonpb.Timestamp) {
	if ts == nil {
		h.writePresence(false)
		return
	}
	h.writePresence(true)
	h.writeUint64(ts.Data)
}

// ComputeLogHash computes a blake3 hash for log chaining: blake3(lastHash || hashFields(log)).
// The log's Hash field is excluded by design (hashLog only hashes sequence, payload, idempotency).
// The hasher is reset and reused to avoid allocation overhead.
func ComputeLogHash(hasher *blake3.Hasher, lastHash []byte, log *commonpb.Log) []byte {
	hasher.Reset()
	if len(lastHash) > 0 {
		_, _ = hasher.Write(lastHash)
	}
	lh := logHasher{w: hasher}
	lh.hashLog(log)
	return hasher.Sum(nil)
}
