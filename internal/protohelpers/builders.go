package protohelpers

import commonpb "github.com/formancehq/ledger/pkg/client/v3/grpc"

// NewTransaction constructs the server's transaction model with empty metadata.
func NewTransaction() *commonpb.Transaction {
	return &commonpb.Transaction{Metadata: map[string]*commonpb.MetadataValue{}}
}

func WithTransactionPostings(tx *commonpb.Transaction, postings ...*commonpb.Posting) *commonpb.Transaction {
	if tx == nil {
		tx = NewTransaction()
	}
	tx.Postings = append(tx.Postings, postings...)

	return tx
}

func WithTransactionID(tx *commonpb.Transaction, id uint64) *commonpb.Transaction {
	if tx == nil {
		tx = NewTransaction()
	}
	tx.Id = id

	return tx
}

func WithTransactionTimestamp(tx *commonpb.Transaction, ts interface{ UnixMicro() int64 }) *commonpb.Transaction {
	if tx == nil {
		tx = NewTransaction()
	}
	tx.Timestamp = commonpb.NewTimestamp(ts)

	return tx
}

func NewLedgerLog(payload *commonpb.LedgerLogPayload) *commonpb.LedgerLog {
	return &commonpb.LedgerLog{Data: payload}
}

func WithLedgerLogID(log *commonpb.LedgerLog, id uint64) *commonpb.LedgerLog {
	if log == nil {
		log = &commonpb.LedgerLog{}
	}
	log.Id = id

	return log
}

func WithLedgerLogDate(log *commonpb.LedgerLog, date interface{ UnixMicro() int64 }) *commonpb.LedgerLog {
	if log == nil {
		log = &commonpb.LedgerLog{}
	}
	log.Date = commonpb.NewTimestamp(date)

	return log
}

// ToLedgerInfo projects the server's create-log payload into an HTTP response.
func ToLedgerInfo(created *commonpb.CreatedLedgerLog) *commonpb.LedgerInfo {
	if created == nil {
		return nil
	}

	return &commonpb.LedgerInfo{
		Name:                   created.GetName(),
		Id:                     created.GetId(),
		CreatedAt:              created.GetCreatedAt(),
		MetadataSchema:         created.GetMetadataSchema(),
		Mode:                   created.GetMode(),
		MirrorSource:           created.GetMirrorSource(),
		AccountTypes:           created.GetAccountTypes(),
		DefaultEnforcementMode: created.GetDefaultEnforcementMode(),
	}
}
