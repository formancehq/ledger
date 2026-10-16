package grpc

// NewTransaction constructs a transaction with initialized metadata.
func NewTransaction() *Transaction {
	return &Transaction{Metadata: map[string]*MetadataValue{}}
}

func WithTransactionPostings(tx *Transaction, postings ...*Posting) *Transaction {
	if tx == nil {
		tx = NewTransaction()
	}
	tx.Postings = append(tx.Postings, postings...)
	return tx
}

func WithTransactionID(tx *Transaction, id uint64) *Transaction {
	if tx == nil {
		tx = NewTransaction()
	}
	tx.Id = id
	return tx
}

func WithTransactionTimestamp(tx *Transaction, ts interface{ UnixMicro() int64 }) *Transaction {
	if tx == nil {
		tx = NewTransaction()
	}
	tx.Timestamp = NewTimestamp(ts)
	return tx
}

func NewLedgerLog(payload *LedgerLogPayload) *LedgerLog {
	return &LedgerLog{Data: payload}
}

func WithLedgerLogID(log *LedgerLog, id uint64) *LedgerLog {
	if log == nil {
		log = &LedgerLog{}
	}
	log.Id = id
	return log
}

func WithLedgerLogDate(log *LedgerLog, date interface{ UnixMicro() int64 }) *LedgerLog {
	if log == nil {
		log = &LedgerLog{}
	}
	log.Date = NewTimestamp(date)
	return log
}

// ToLedgerInfo projects a created log payload into its public ledger info.
func ToLedgerInfo(created *CreatedLedgerLog) *LedgerInfo {
	if created == nil {
		return nil
	}
	return &LedgerInfo{
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
