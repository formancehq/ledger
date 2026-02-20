package processing

import (
	"encoding/binary"
	"io"
	"sort"
	"unsafe"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/zeebo/blake3"
)

// HashVersion is written as the first byte of every hash computation.
// Bumping this value invalidates all existing hashes, making format
// changes immediately detectable.
const HashVersion byte = 1

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

func (h *logHasher) writeInt32(v int32) {
	binary.LittleEndian.PutUint32(h.buf[:4], uint32(v))
	_, _ = h.w.Write(h.buf[:4])
}

func (h *logHasher) writeInt64(v int64) {
	binary.LittleEndian.PutUint64(h.buf[:8], uint64(v))
	_, _ = h.w.Write(h.buf[:8])
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
	case *commonpb.LogPayload_RegisterSigningKey:
		h.writeDiscriminator(4)
		h.hashRegisterSigningKeyLog(v.RegisterSigningKey)
	case *commonpb.LogPayload_RevokeSigningKey:
		h.writeDiscriminator(5)
		h.hashRevokeSigningKeyLog(v.RevokeSigningKey)
	case *commonpb.LogPayload_SetSigningConfig:
		h.writeDiscriminator(6)
		h.hashSetSigningConfigLog(v.SetSigningConfig)
	case *commonpb.LogPayload_AddedEventsSink:
		h.writeDiscriminator(7)
		h.hashAddedEventsSinkLog(v.AddedEventsSink)
	case *commonpb.LogPayload_RemovedEventsSink:
		h.writeDiscriminator(8)
		h.hashRemovedEventsSinkLog(v.RemovedEventsSink)
	case *commonpb.LogPayload_ClosePeriod:
		h.writeDiscriminator(9)
		h.hashClosePeriodLog(v.ClosePeriod)
	case *commonpb.LogPayload_SealPeriod:
		h.writeDiscriminator(10)
		h.hashSealPeriodLog(v.SealPeriod)
	case *commonpb.LogPayload_ArchivePeriod:
		h.writeDiscriminator(11)
		h.hashArchivePeriodLog(v.ArchivePeriod)
	case *commonpb.LogPayload_ConfirmArchivePeriod:
		h.writeDiscriminator(12)
		h.hashConfirmArchivePeriodLog(v.ConfirmArchivePeriod)
	case *commonpb.LogPayload_SetMaintenanceMode:
		h.writeDiscriminator(13)
		h.hashSetMaintenanceModeLog(v.SetMaintenanceMode)
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
	h.hashUint256(p.Amount)
	h.writeString(p.Asset)
}

func (h *logHasher) hashUint256(u *commonpb.Uint256) {
	if u == nil {
		h.writePresence(false)
		return
	}
	h.writePresence(true)
	h.writeUint64(u.V0)
	h.writeUint64(u.V1)
	h.writeUint64(u.V2)
	h.writeUint64(u.V3)
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

// --- Signing key log hashers ---

func (h *logHasher) hashRegisterSigningKeyLog(r *commonpb.RegisterSigningKeyLog) {
	if r == nil {
		h.writePresence(false)
		return
	}
	h.writePresence(true)
	h.writeString(r.KeyId)
	h.writeBytes(r.PublicKey)
}

func (h *logHasher) hashRevokeSigningKeyLog(r *commonpb.RevokeSigningKeyLog) {
	if r == nil {
		h.writePresence(false)
		return
	}
	h.writePresence(true)
	h.writeString(r.KeyId)
}

func (h *logHasher) hashSetSigningConfigLog(s *commonpb.SetSigningConfigLog) {
	if s == nil {
		h.writePresence(false)
		return
	}
	h.writePresence(true)
	h.writeBool(s.RequireSignatures)
}

// --- Events sink log hashers ---

func (h *logHasher) hashAddedEventsSinkLog(a *commonpb.AddedEventsSinkLog) {
	if a == nil {
		h.writePresence(false)
		return
	}
	h.writePresence(true)
	h.hashSinkConfig(a.Config)
}

func (h *logHasher) hashRemovedEventsSinkLog(r *commonpb.RemovedEventsSinkLog) {
	if r == nil {
		h.writePresence(false)
		return
	}
	h.writePresence(true)
	h.writeString(r.Name)
}

func (h *logHasher) hashSinkConfig(c *commonpb.SinkConfig) {
	if c == nil {
		h.writePresence(false)
		return
	}
	h.writePresence(true)
	h.writeString(c.Name)
	switch v := c.Type.(type) {
	case *commonpb.SinkConfig_Nats:
		h.writeDiscriminator(1)
		h.hashNatsSinkConfig(v.Nats)
	case *commonpb.SinkConfig_Clickhouse:
		h.writeDiscriminator(2)
		h.hashClickHouseSinkConfig(v.Clickhouse)
	case *commonpb.SinkConfig_Kafka:
		h.writeDiscriminator(3)
		h.hashKafkaSinkConfig(v.Kafka)
	case *commonpb.SinkConfig_Http:
		h.writeDiscriminator(4)
		h.hashHttpSinkConfig(v.Http)
	default:
		h.writeDiscriminator(0)
	}
	h.writeString(c.Format)
	h.writeInt32(c.BatchSize)
	h.writeInt64(c.BatchDelayMs)
}

func (h *logHasher) hashNatsSinkConfig(n *commonpb.NatsSinkConfig) {
	if n == nil {
		h.writePresence(false)
		return
	}
	h.writePresence(true)
	h.writeString(n.Url)
	h.writeString(n.Topic)
}

func (h *logHasher) hashClickHouseSinkConfig(c *commonpb.ClickHouseSinkConfig) {
	if c == nil {
		h.writePresence(false)
		return
	}
	h.writePresence(true)
	h.writeString(c.Dsn)
	h.writeString(c.Table)
}

func (h *logHasher) hashKafkaSinkConfig(k *commonpb.KafkaSinkConfig) {
	if k == nil {
		h.writePresence(false)
		return
	}
	h.writePresence(true)
	h.writeUint32(uint32(len(k.Brokers)))
	for _, b := range k.Brokers {
		h.writeString(b)
	}
	h.writeString(k.Topic)
	h.writeBool(k.Tls)
	h.writeString(k.SaslMechanism)
	h.writeString(k.SaslUsername)
	h.writeString(k.SaslPassword)
}

func (h *logHasher) hashHttpSinkConfig(c *commonpb.HttpSinkConfig) {
	if c == nil {
		h.writePresence(false)
		return
	}
	h.writePresence(true)
	h.writeString(c.Endpoint)
	h.writeString(c.Secret)
}

// --- Period log hashers ---

func (h *logHasher) hashClosePeriodLog(c *commonpb.ClosePeriodLog) {
	if c == nil {
		h.writePresence(false)
		return
	}
	h.writePresence(true)
	h.hashPeriod(c.ClosedPeriod)
	h.hashPeriod(c.NewPeriod)
}

func (h *logHasher) hashSealPeriodLog(s *commonpb.SealPeriodLog) {
	if s == nil {
		h.writePresence(false)
		return
	}
	h.writePresence(true)
	h.hashPeriod(s.Period)
}

func (h *logHasher) hashArchivePeriodLog(a *commonpb.ArchivePeriodLog) {
	if a == nil {
		h.writePresence(false)
		return
	}
	h.writePresence(true)
	h.hashPeriod(a.Period)
}

func (h *logHasher) hashConfirmArchivePeriodLog(c *commonpb.ConfirmArchivePeriodLog) {
	if c == nil {
		h.writePresence(false)
		return
	}
	h.writePresence(true)
	h.hashPeriod(c.Period)
}

func (h *logHasher) hashPeriod(p *commonpb.Period) {
	if p == nil {
		h.writePresence(false)
		return
	}
	h.writePresence(true)
	h.writeUint64(p.Id)
	h.hashTimestamp(p.Start)
	h.hashTimestamp(p.End)
	h.writeInt32(int32(p.Status))
	h.writeUint64(p.CloseSequence)
	h.writeBytes(p.SealingHash)
	h.writeBytes(p.LastLogHash)
	h.writeUint64(p.StartSequence)
}

// --- Maintenance mode log hasher ---

func (h *logHasher) hashSetMaintenanceModeLog(m *commonpb.SetMaintenanceModeLog) {
	if m == nil {
		h.writePresence(false)
		return
	}
	h.writePresence(true)
	h.writeBool(m.Enabled)
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

// ComputeLogHash computes a blake3 hash for log chaining:
//
//	blake3(HashVersion || lastHash || hashFields(log))
//
// The log's Hash field is excluded by design (hashLog only hashes sequence, payload, idempotency).
// The hasher is reset and reused to avoid allocation overhead.
func ComputeLogHash(hasher *blake3.Hasher, lastHash []byte, log *commonpb.Log) []byte {
	hasher.Reset()
	_, _ = hasher.Write([]byte{HashVersion})
	if len(lastHash) > 0 {
		_, _ = hasher.Write(lastHash)
	}
	lh := logHasher{w: hasher}
	lh.hashLog(log)
	return hasher.Sum(nil)
}
