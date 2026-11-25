package backup

import (
	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

func coldLogKey(seq uint64) []byte {
	return dal.NewKeyBuilder().PutZonePrefix(dal.ZoneHistory, dal.SubHistoryLog).PutUint64(seq).Build()
}

func coldAuditKey(seq uint64) []byte {
	return dal.NewKeyBuilder().PutZonePrefix(dal.ZoneHistory, dal.SubHistoryAudit).PutUint64(seq).Build()
}

func createLedgerLog(seq uint64, name string, id uint32) *commonpb.Log {
	return &commonpb.Log{
		Sequence: seq,
		Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_CreateLedger{
			CreateLedger: &commonpb.CreatedLedgerLog{Name: name, Id: id},
		}},
	}
}

func auditSuccess(seq, minLogSeq, maxLogSeq uint64) *auditpb.AuditEntry {
	return &auditpb.AuditEntry{
		Sequence: seq,
		Outcome: &auditpb.AuditEntry_Success{Success: &auditpb.AuditSuccess{
			MinLogSequence: minLogSeq,
			MaxLogSequence: maxLogSeq,
		}},
	}
}
