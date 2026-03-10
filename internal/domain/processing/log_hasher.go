package processing

import (
	"encoding/binary"
	"io"
	"sort"
	"unsafe"

	"github.com/zeebo/blake3"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
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
	h.writeUint64(log.GetSequence())
	h.hashLogPayload(log.GetPayload())
	h.hashIdempotency(log.GetIdempotency())
}

func (h *logHasher) hashLogPayload(p *commonpb.LogPayload) {
	if p == nil {
		h.writePresence(false)

		return
	}

	h.writePresence(true)

	switch v := p.GetType().(type) {
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
	case *commonpb.LogPayload_SetPeriodSchedule:
		h.writeDiscriminator(14)
		h.hashSetPeriodScheduleLog(v.SetPeriodSchedule)
	case *commonpb.LogPayload_DeletePeriodSchedule:
		h.writeDiscriminator(15)
		h.hashDeletePeriodScheduleLog(v.DeletePeriodSchedule)
	case *commonpb.LogPayload_SetAuditConfig:
		h.writeDiscriminator(16)
		h.hashSetAuditConfigLog(v.SetAuditConfig)
	case *commonpb.LogPayload_PromoteLedger:
		h.writeDiscriminator(17)
		h.hashPromoteLedgerLog(v.PromoteLedger)
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
	h.hashLedgerInfo(cl.GetInfo())
}

func (h *logHasher) hashDeleteLedgerLog(dl *commonpb.DeleteLedgerLog) {
	if dl == nil {
		h.writePresence(false)

		return
	}

	h.writePresence(true)
	h.hashLedgerInfo(dl.GetInfo())
}

func (h *logHasher) hashLedgerInfo(info *commonpb.LedgerInfo) {
	if info == nil {
		h.writePresence(false)

		return
	}

	h.writePresence(true)
	h.writeString(info.GetName())
	h.hashTimestamp(info.GetCreatedAt())
	h.hashTimestamp(info.GetDeletedAt())
	h.writeInt32(int32(info.GetMode()))
	h.hashMirrorSourceConfig(info.GetMirrorSource())
}

func (h *logHasher) hashMirrorSourceConfig(cfg *commonpb.MirrorSourceConfig) {
	if cfg == nil {
		h.writePresence(false)

		return
	}

	h.writePresence(true)
	h.writeString(cfg.GetLedgerName())

	switch s := cfg.GetType().(type) {
	case *commonpb.MirrorSourceConfig_Http:
		h.writeDiscriminator(1)
		h.writeString(s.Http.GetBaseUrl())

		if cc := s.Http.GetOauth2ClientCredentials(); cc != nil {
			h.writeString(cc.GetClientId())
			h.writeString(cc.GetClientSecret())
			h.writeString(cc.GetTokenEndpoint())

			for _, scope := range cc.GetScopes() {
				h.writeString(scope)
			}
		}
	case *commonpb.MirrorSourceConfig_Postgres:
		h.writeDiscriminator(2)
		h.writeString(s.Postgres.GetDsn())
	default:
		h.writeDiscriminator(0)
	}
}

func (h *logHasher) hashApplyLedgerLog(a *commonpb.ApplyLedgerLog) {
	if a == nil {
		h.writePresence(false)

		return
	}

	h.writePresence(true)
	h.writeString(a.GetLedgerName())
	h.hashLedgerLog(a.GetLog())
}

func (h *logHasher) hashLedgerLog(ll *commonpb.LedgerLog) {
	if ll == nil {
		h.writePresence(false)

		return
	}

	h.writePresence(true)
	h.hashLedgerLogPayload(ll.GetData())
	h.hashTimestamp(ll.GetDate())
	h.writeUint64(ll.GetId())
}

func (h *logHasher) hashLedgerLogPayload(p *commonpb.LedgerLogPayload) {
	if p == nil {
		h.writePresence(false)

		return
	}

	h.writePresence(true)

	switch v := p.GetPayload().(type) {
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
	case *commonpb.LedgerLogPayload_SetMetadataFieldType:
		h.writeDiscriminator(5)
		h.hashSetMetadataFieldType(v.SetMetadataFieldType)
	case *commonpb.LedgerLogPayload_RemovedMetadataFieldType:
		h.writeDiscriminator(6)
		h.hashRemovedMetadataFieldType(v.RemovedMetadataFieldType)
	case *commonpb.LedgerLogPayload_ConvertMetadataBatch:
		h.writeDiscriminator(7)
		h.hashConvertMetadataBatchLog(v.ConvertMetadataBatch)
	case *commonpb.LedgerLogPayload_MetadataConversionComplete:
		h.writeDiscriminator(8)
		h.hashMetadataConversionCompleteLog(v.MetadataConversionComplete)
	case *commonpb.LedgerLogPayload_FillGap:
		h.writeDiscriminator(9)
		h.hashFillGapLog(v.FillGap)
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
	h.hashTransaction(ct.GetTransaction())
	// map<string, MetadataSet> — sorted iteration for determinism
	h.hashMetadataSetMap(ct.GetAccountMetadata())
	h.hashMetadataSetMap(ct.GetPreviousAccountMetadata())
}

func (h *logHasher) hashTransaction(tx *commonpb.Transaction) {
	if tx == nil {
		h.writePresence(false)

		return
	}

	h.writePresence(true)
	// repeated Posting
	h.writeUint32(uint32(len(tx.GetPostings())))

	for _, p := range tx.GetPostings() {
		h.hashPosting(p)
	}

	h.hashMetadataSet(tx.GetMetadata())
	h.hashTimestamp(tx.GetTimestamp())
	h.writeString(tx.GetReference())
	h.writeUint64(tx.GetId())
	h.writeBool(tx.GetReverted())
	h.hashTimestamp(tx.GetInsertedAt())
	h.hashTimestamp(tx.GetUpdatedAt())
	h.hashTimestamp(tx.GetRevertedAt())
}

func (h *logHasher) hashPosting(p *commonpb.Posting) {
	if p == nil {
		h.writePresence(false)

		return
	}

	h.writePresence(true)
	h.writeString(p.GetSource())
	h.writeString(p.GetDestination())
	h.hashUint256(p.GetAmount())
	h.writeString(p.GetAsset())
}

func (h *logHasher) hashUint256(u *commonpb.Uint256) {
	if u == nil {
		h.writePresence(false)

		return
	}

	h.writePresence(true)
	h.writeUint64(u.GetV0())
	h.writeUint64(u.GetV1())
	h.writeUint64(u.GetV2())
	h.writeUint64(u.GetV3())
}

func (h *logHasher) hashRevertedTransaction(rt *commonpb.RevertedTransaction) {
	if rt == nil {
		h.writePresence(false)

		return
	}

	h.writePresence(true)
	h.writeUint64(rt.GetRevertedTransactionId())
	h.hashTransaction(rt.GetRevertTransaction())
}

func (h *logHasher) hashSavedMetadata(sm *commonpb.SavedMetadata) {
	if sm == nil {
		h.writePresence(false)

		return
	}

	h.writePresence(true)
	h.hashTarget(sm.GetTarget())
	h.hashMetadataSet(sm.GetMetadata())
	h.hashMetadataValueMap(sm.GetPreviousValues())
}

func (h *logHasher) hashDeletedMetadata(dm *commonpb.DeletedMetadata) {
	if dm == nil {
		h.writePresence(false)

		return
	}

	h.writePresence(true)
	h.hashTarget(dm.GetTarget())
	h.writeString(dm.GetKey())
	h.hashMetadataValue(dm.GetPreviousValue())
}

func (h *logHasher) hashSetMetadataFieldType(l *commonpb.SetMetadataFieldTypeLog) {
	if l == nil {
		h.writePresence(false)

		return
	}

	h.writePresence(true)
	h.writeUint32(uint32(l.GetTargetType()))
	h.writeString(l.GetKey())
	h.writeUint32(uint32(l.GetType()))
}

func (h *logHasher) hashRemovedMetadataFieldType(l *commonpb.RemovedMetadataFieldTypeLog) {
	if l == nil {
		h.writePresence(false)

		return
	}

	h.writePresence(true)
	h.writeUint32(uint32(l.GetTargetType()))
	h.writeString(l.GetKey())
}

func (h *logHasher) hashConvertMetadataBatchLog(l *commonpb.ConvertMetadataBatchLog) {
	if l == nil {
		h.writePresence(false)

		return
	}

	h.writePresence(true)
	h.writeUint32(uint32(l.GetTargetType()))
	h.writeString(l.GetKey())
	h.writeUint32(l.GetCount())
}

func (h *logHasher) hashMetadataConversionCompleteLog(l *commonpb.MetadataConversionCompleteLog) {
	if l == nil {
		h.writePresence(false)

		return
	}

	h.writePresence(true)
	h.writeUint32(uint32(l.GetTargetType()))
	h.writeString(l.GetKey())
}

func (h *logHasher) hashTarget(t *commonpb.Target) {
	if t == nil {
		h.writePresence(false)

		return
	}

	h.writePresence(true)

	switch v := t.GetTarget().(type) {
	case *commonpb.Target_Account:
		h.writeDiscriminator(1)

		if v.Account != nil {
			h.writePresence(true)
			h.writeString(v.Account.GetAddr())
		} else {
			h.writePresence(false)
		}
	case *commonpb.Target_Transaction:
		h.writeDiscriminator(2)

		if v.Transaction != nil {
			h.writePresence(true)
			h.writeUint64(v.Transaction.GetId())
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
	h.writeString(i.GetKey())
}

// --- Signing key log hashers ---

func (h *logHasher) hashRegisterSigningKeyLog(r *commonpb.RegisterSigningKeyLog) {
	if r == nil {
		h.writePresence(false)

		return
	}

	h.writePresence(true)
	h.writeString(r.GetKeyId())
	h.writeBytes(r.GetPublicKey())
}

func (h *logHasher) hashRevokeSigningKeyLog(r *commonpb.RevokeSigningKeyLog) {
	if r == nil {
		h.writePresence(false)

		return
	}

	h.writePresence(true)
	h.writeString(r.GetKeyId())
}

func (h *logHasher) hashSetSigningConfigLog(s *commonpb.SetSigningConfigLog) {
	if s == nil {
		h.writePresence(false)

		return
	}

	h.writePresence(true)
	h.writeBool(s.GetRequireSignatures())
}

// --- Events sink log hashers ---

func (h *logHasher) hashAddedEventsSinkLog(a *commonpb.AddedEventsSinkLog) {
	if a == nil {
		h.writePresence(false)

		return
	}

	h.writePresence(true)
	h.hashSinkConfig(a.GetConfig())
}

func (h *logHasher) hashRemovedEventsSinkLog(r *commonpb.RemovedEventsSinkLog) {
	if r == nil {
		h.writePresence(false)

		return
	}

	h.writePresence(true)
	h.writeString(r.GetName())
}

func (h *logHasher) hashSinkConfig(c *commonpb.SinkConfig) {
	if c == nil {
		h.writePresence(false)

		return
	}

	h.writePresence(true)
	h.writeString(c.GetName())

	switch v := c.GetType().(type) {
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

	h.writeString(c.GetFormat())
	h.writeInt32(c.GetBatchSize())
	h.writeInt64(c.GetBatchDelayMs())
}

func (h *logHasher) hashNatsSinkConfig(n *commonpb.NatsSinkConfig) {
	if n == nil {
		h.writePresence(false)

		return
	}

	h.writePresence(true)
	h.writeString(n.GetUrl())
	h.writeString(n.GetTopic())
}

func (h *logHasher) hashClickHouseSinkConfig(c *commonpb.ClickHouseSinkConfig) {
	if c == nil {
		h.writePresence(false)

		return
	}

	h.writePresence(true)
	h.writeString(c.GetDsn())
	h.writeString(c.GetTable())
}

func (h *logHasher) hashKafkaSinkConfig(k *commonpb.KafkaSinkConfig) {
	if k == nil {
		h.writePresence(false)

		return
	}

	h.writePresence(true)
	h.writeUint32(uint32(len(k.GetBrokers())))

	for _, b := range k.GetBrokers() {
		h.writeString(b)
	}

	h.writeString(k.GetTopic())
	h.writeBool(k.GetTls())
	h.writeString(k.GetSaslMechanism())
	h.writeString(k.GetSaslUsername())
	h.writeString(k.GetSaslPassword())
}

func (h *logHasher) hashHttpSinkConfig(c *commonpb.HttpSinkConfig) {
	if c == nil {
		h.writePresence(false)

		return
	}

	h.writePresence(true)
	h.writeString(c.GetEndpoint())
	h.writeString(c.GetSecret())
}

// --- Period log hashers ---

func (h *logHasher) hashClosePeriodLog(c *commonpb.ClosePeriodLog) {
	if c == nil {
		h.writePresence(false)

		return
	}

	h.writePresence(true)
	h.hashPeriod(c.GetClosedPeriod())
	h.hashPeriod(c.GetNewPeriod())
}

func (h *logHasher) hashSealPeriodLog(s *commonpb.SealPeriodLog) {
	if s == nil {
		h.writePresence(false)

		return
	}

	h.writePresence(true)
	h.hashPeriod(s.GetPeriod())
}

func (h *logHasher) hashArchivePeriodLog(a *commonpb.ArchivePeriodLog) {
	if a == nil {
		h.writePresence(false)

		return
	}

	h.writePresence(true)
	h.hashPeriod(a.GetPeriod())
}

func (h *logHasher) hashConfirmArchivePeriodLog(c *commonpb.ConfirmArchivePeriodLog) {
	if c == nil {
		h.writePresence(false)

		return
	}

	h.writePresence(true)
	h.hashPeriod(c.GetPeriod())
}

func (h *logHasher) hashPeriod(p *commonpb.Period) {
	if p == nil {
		h.writePresence(false)

		return
	}

	h.writePresence(true)
	h.writeUint64(p.GetId())
	h.hashTimestamp(p.GetStart())
	h.hashTimestamp(p.GetEnd())
	h.writeInt32(int32(p.GetStatus()))
	h.writeUint64(p.GetCloseSequence())
	h.writeBytes(p.GetSealingHash())
	h.writeBytes(p.GetLastLogHash())
	h.writeUint64(p.GetStartSequence())
}

// --- Maintenance mode log hasher ---

func (h *logHasher) hashSetMaintenanceModeLog(m *commonpb.SetMaintenanceModeLog) {
	if m == nil {
		h.writePresence(false)

		return
	}

	h.writePresence(true)
	h.writeBool(m.GetEnabled())
}

// --- Period schedule log hasher ---

func (h *logHasher) hashSetPeriodScheduleLog(m *commonpb.SetPeriodScheduleLog) {
	if m == nil {
		h.writePresence(false)

		return
	}

	h.writePresence(true)
	h.writeString(m.GetCron())
}

// --- Delete period schedule log hasher ---

func (h *logHasher) hashDeletePeriodScheduleLog(m *commonpb.DeletePeriodScheduleLog) {
	if m == nil {
		h.writePresence(false)

		return
	}

	h.writePresence(true)
}

func (h *logHasher) hashMetadataSet(ms *commonpb.MetadataSet) {
	if ms == nil {
		h.writePresence(false)

		return
	}

	h.writePresence(true)
	h.writeUint32(uint32(len(ms.GetMetadata())))

	for _, m := range ms.GetMetadata() {
		h.hashMetadata(m)
	}
}

func (h *logHasher) hashMetadataSetMap(m map[string]*commonpb.MetadataSet) {
	h.writeUint32(uint32(len(m)))

	if len(m) > 0 {
		keys := make([]string, 0, len(m))
		for k := range m {
			keys = append(keys, k)
		}

		sort.Strings(keys)

		for _, k := range keys {
			h.writeString(k)
			h.hashMetadataSet(m[k])
		}
	}
}

func (h *logHasher) hashMetadataValueMap(m map[string]*commonpb.MetadataValue) {
	h.writeUint32(uint32(len(m)))

	if len(m) > 0 {
		keys := make([]string, 0, len(m))
		for k := range m {
			keys = append(keys, k)
		}

		sort.Strings(keys)

		for _, k := range keys {
			h.writeString(k)
			h.hashMetadataValue(m[k])
		}
	}
}

func (h *logHasher) hashMetadata(m *commonpb.Metadata) {
	if m == nil {
		h.writePresence(false)

		return
	}

	h.writePresence(true)
	h.writeString(m.GetKey())
	h.hashMetadataValue(m.GetValue())
}

func (h *logHasher) hashMetadataValue(mv *commonpb.MetadataValue) {
	if mv == nil {
		h.writePresence(false)

		return
	}

	h.writePresence(true)
	h.writeString(commonpb.MetadataValueToString(mv))
}

func (h *logHasher) hashTimestamp(ts *commonpb.Timestamp) {
	if ts == nil {
		h.writePresence(false)

		return
	}

	h.writePresence(true)
	h.writeUint64(ts.GetData())
}

// --- Audit config log hasher ---

func (h *logHasher) hashSetAuditConfigLog(a *commonpb.SetAuditConfigLog) {
	if a == nil {
		h.writePresence(false)

		return
	}

	h.writePresence(true)
	h.writeBool(a.GetEnabled())
}

// --- FillGap log hasher ---

func (h *logHasher) hashFillGapLog(f *commonpb.FillGapLog) {
	if f == nil {
		h.writePresence(false)

		return
	}

	h.writePresence(true)
	h.writeUint64(f.GetOriginalId())
}

// --- Promote ledger log hasher ---

func (h *logHasher) hashPromoteLedgerLog(p *commonpb.PromoteLedgerLog) {
	if p == nil {
		h.writePresence(false)

		return
	}

	h.writePresence(true)
	h.hashLedgerInfo(p.GetInfo())
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
