package store

import (
	"github.com/formancehq/ledger-v3-poc/internal/commandsfb"
	"github.com/formancehq/ledger-v3-poc/internal/ledgerpb"
	flatbuffers "github.com/google/flatbuffers/go"
)

// FlatBufferLog wraps raw FlatBuffer bytes and provides typed access to log data.
// This enables zero-copy access to log fields without deserialization.
type FlatBufferLog struct {
	data []byte
	fb   *commandsfb.Log
}

// NewFlatBufferLog creates a new FlatBufferLog from raw bytes
func NewFlatBufferLog(data []byte) *FlatBufferLog {
	return &FlatBufferLog{
		data: data,
		fb:   commandsfb.GetRootAsLog(data, 0),
	}
}

// Bytes returns the raw FlatBuffer bytes
func (l *FlatBufferLog) Bytes() []byte {
	return l.data
}

// Id returns the log ID
func (l *FlatBufferLog) Id() uint64 {
	return l.fb.Id()
}

// LedgerId returns the ledger ID
func (l *FlatBufferLog) LedgerId() uint32 {
	return l.fb.LedgerId()
}

// Date returns the log date as a Timestamp
func (l *FlatBufferLog) Date() *ledgerpb.Timestamp {
	if ts := l.fb.Date(nil); ts != nil {
		return &ledgerpb.Timestamp{Data: ts.Data()}
	}
	return nil
}

// Idempotency returns the idempotency info if present
func (l *FlatBufferLog) Idempotency() *ledgerpb.Idempotency {
	if idem := l.fb.Idempotency(nil); idem != nil {
		return &ledgerpb.Idempotency{
			Key:  string(idem.Key()),
			Hash: idem.HashBytes(),
		}
	}
	return nil
}

// IdempotencyKey returns the idempotency key if present
func (l *FlatBufferLog) IdempotencyKey() string {
	if idem := l.fb.Idempotency(nil); idem != nil {
		return string(idem.Key())
	}
	return ""
}

// IdempotencyHash returns the idempotency hash if present
func (l *FlatBufferLog) IdempotencyHash() []byte {
	if idem := l.fb.Idempotency(nil); idem != nil {
		return idem.HashBytes()
	}
	return nil
}

// Data returns the log payload accessor
func (l *FlatBufferLog) Data() *commandsfb.LogPayload {
	return l.fb.Data(nil)
}

// HasCreatedTransaction returns true if this log contains a created transaction
func (l *FlatBufferLog) HasCreatedTransaction() bool {
	if data := l.Data(); data != nil {
		return data.CreatedTransaction(nil) != nil
	}
	return false
}

// HasRevertedTransaction returns true if this log contains a reverted transaction
func (l *FlatBufferLog) HasRevertedTransaction() bool {
	if data := l.Data(); data != nil {
		return data.RevertedTransaction(nil) != nil
	}
	return false
}

// HasSavedMetadata returns true if this log contains saved metadata
func (l *FlatBufferLog) HasSavedMetadata() bool {
	if data := l.Data(); data != nil {
		return data.SavedMetadata(nil) != nil
	}
	return false
}

// HasDeletedMetadata returns true if this log contains deleted metadata
func (l *FlatBufferLog) HasDeletedMetadata() bool {
	if data := l.Data(); data != nil {
		return data.DeletedMetadata(nil) != nil
	}
	return false
}

// FlatBufferLogToProtobuf converts the FlatBuffer log to a protobuf Log
func FlatBufferLogToProtobuf(l *FlatBufferLog) *ledgerpb.Log {
	log := &ledgerpb.Log{
		Id:       l.fb.Id(),
		LedgerId: l.fb.LedgerId(),
	}

	// Convert date
	if ts := l.fb.Date(nil); ts != nil {
		log.Date = &ledgerpb.Timestamp{Data: ts.Data()}
	}

	// Convert idempotency
	if idem := l.fb.Idempotency(nil); idem != nil {
		log.Idempotency = &ledgerpb.Idempotency{
			Key:  string(idem.Key()),
			Hash: idem.HashBytes(),
		}
	}

	// Convert payload
	if data := l.fb.Data(nil); data != nil {
		log.Data = convertLogPayload(data)
	}

	return log
}

func convertLogPayload(fb *commandsfb.LogPayload) *ledgerpb.LogPayload {
	if ct := fb.CreatedTransaction(nil); ct != nil {
		return &ledgerpb.LogPayload{
			Payload: &ledgerpb.LogPayload_CreatedTransaction{
				CreatedTransaction: convertCreatedTransaction(ct),
			},
		}
	}

	if rt := fb.RevertedTransaction(nil); rt != nil {
		return &ledgerpb.LogPayload{
			Payload: &ledgerpb.LogPayload_RevertedTransaction{
				RevertedTransaction: convertRevertedTransaction(rt),
			},
		}
	}

	if sm := fb.SavedMetadata(nil); sm != nil {
		return &ledgerpb.LogPayload{
			Payload: &ledgerpb.LogPayload_SavedMetadata{
				SavedMetadata: convertSavedMetadata(sm),
			},
		}
	}

	if dm := fb.DeletedMetadata(nil); dm != nil {
		return &ledgerpb.LogPayload{
			Payload: &ledgerpb.LogPayload_DeletedMetadata{
				DeletedMetadata: convertDeletedMetadata(dm),
			},
		}
	}

	return nil
}

func convertCreatedTransaction(fb *commandsfb.CreatedTransaction) *ledgerpb.CreatedTransaction {
	ct := &ledgerpb.CreatedTransaction{}

	if tx := fb.Transaction(nil); tx != nil {
		ct.Transaction = convertTransaction(tx)
	}

	if fb.AccountMetadataLength() > 0 {
		ct.AccountMetadata = make(map[string]*ledgerpb.Metadata, fb.AccountMetadataLength())
		var entry commandsfb.AccountMetadataEntry
		for i := 0; i < fb.AccountMetadataLength(); i++ {
			if fb.AccountMetadata(&entry, i) {
				ct.AccountMetadata[string(entry.Key())] = convertMetadataToProtobuf(entry.Value(nil))
			}
		}
	}

	return ct
}

func convertRevertedTransaction(fb *commandsfb.RevertedTransaction) *ledgerpb.RevertedTransaction {
	rt := &ledgerpb.RevertedTransaction{
		RevertedTransactionId: fb.RevertedTransactionId(),
	}

	if tx := fb.RevertTransaction(nil); tx != nil {
		rt.RevertTransaction = convertTransaction(tx)
	}

	return rt
}

func convertSavedMetadata(fb *commandsfb.SavedMetadata) *ledgerpb.SavedMetadata {
	sm := &ledgerpb.SavedMetadata{}

	if target := fb.Target(nil); target != nil {
		sm.Target = convertTarget(target)
	}

	if metadata := fb.Metadata(nil); metadata != nil {
		sm.Metadata = convertMetadataToProtobuf(metadata)
	}

	return sm
}

func convertDeletedMetadata(fb *commandsfb.DeletedMetadata) *ledgerpb.DeletedMetadata {
	dm := &ledgerpb.DeletedMetadata{
		Key: string(fb.Key()),
	}

	if target := fb.Target(nil); target != nil {
		dm.Target = convertTarget(target)
	}

	return dm
}

func convertTransaction(fb *commandsfb.Transaction) *ledgerpb.Transaction {
	tx := &ledgerpb.Transaction{
		Id:        fb.Id(),
		Reference: string(fb.Reference()),
		Reverted:  fb.Reverted(),
	}

	// Convert timestamps
	if ts := fb.Timestamp(nil); ts != nil {
		tx.Timestamp = &ledgerpb.Timestamp{Data: ts.Data()}
	}
	if ts := fb.InsertedAt(nil); ts != nil {
		tx.InsertedAt = &ledgerpb.Timestamp{Data: ts.Data()}
	}
	if ts := fb.UpdatedAt(nil); ts != nil {
		tx.UpdatedAt = &ledgerpb.Timestamp{Data: ts.Data()}
	}
	if ts := fb.RevertedAt(nil); ts != nil {
		tx.RevertedAt = &ledgerpb.Timestamp{Data: ts.Data()}
	}

	// Convert postings
	if fb.PostingsLength() > 0 {
		tx.Postings = make([]*ledgerpb.Posting, fb.PostingsLength())
		var posting commandsfb.Posting
		for i := 0; i < fb.PostingsLength(); i++ {
			if fb.Postings(&posting, i) {
				tx.Postings[i] = &ledgerpb.Posting{
					Source:      string(posting.Source()),
					Destination: string(posting.Destination()),
					Asset:       string(posting.Asset()),
				}
				if amount := posting.Amount(nil); amount != nil {
					tx.Postings[i].Amount = &ledgerpb.BigInt{Data: amount.DataBytes()}
				}
			}
		}
	}

	// Convert metadata
	if fb.MetadataLength() > 0 {
		tx.Metadata = make(map[string]string, fb.MetadataLength())
		var entry commandsfb.MetadataEntry
		for i := 0; i < fb.MetadataLength(); i++ {
			if fb.Metadata(&entry, i) {
				tx.Metadata[string(entry.Key())] = string(entry.Value())
			}
		}
	}

	return tx
}

// ConvertMetadata extracts metadata from FlatBuffer Metadata and returns FlatBufferMetadata
func ConvertMetadata(fb *commandsfb.Metadata) *FlatBufferMetadata {
	if fb == nil {
		return nil
	}

	// Build FlatBuffer metadata from the parsed data
	builder := NewFlatBufferMetadataBuilder()
	metadata := make(map[string]string)
	if fb.EntriesLength() > 0 {
		var entry commandsfb.EntriesEntry
		for i := 0; i < fb.EntriesLength(); i++ {
			if fb.Entries(&entry, i) {
				metadata[string(entry.Key())] = string(entry.Value())
			}
		}
	}
	return NewFlatBufferMetadata(builder.Build(metadata))
}

// convertMetadataToProtobuf converts FlatBuffer Metadata to protobuf Metadata (for API compatibility)
func convertMetadataToProtobuf(fb *commandsfb.Metadata) *ledgerpb.Metadata {
	if fb == nil {
		return nil
	}

	m := &ledgerpb.Metadata{}
	if fb.EntriesLength() > 0 {
		m.Entries = make(map[string]string, fb.EntriesLength())
		var entry commandsfb.EntriesEntry
		for i := 0; i < fb.EntriesLength(); i++ {
			if fb.Entries(&entry, i) {
				m.Entries[string(entry.Key())] = string(entry.Value())
			}
		}
	}
	return m
}

func convertTarget(fb *commandsfb.Target) *ledgerpb.Target {
	if fb == nil {
		return nil
	}

	target := &ledgerpb.Target{}
	if inner := fb.Target(nil); inner != nil {
		if account := inner.Account(nil); account != nil {
			target.Target = &ledgerpb.Target_Account{
				Account: &ledgerpb.TargetAccount{
					Addr: string(account.Addr()),
				},
			}
		} else if transaction := inner.Transaction(nil); transaction != nil {
			target.Target = &ledgerpb.Target_Transaction{
				Transaction: &ledgerpb.TargetTransaction{
					Id: transaction.Id(),
				},
			}
		}
	}
	return target
}

// FlatBufferLogBuilder helps build FlatBuffer logs
type FlatBufferLogBuilder struct {
	builder *flatbuffers.Builder
}

// NewFlatBufferLogBuilder creates a new builder
func NewFlatBufferLogBuilder() *FlatBufferLogBuilder {
	return &FlatBufferLogBuilder{
		builder: flatbuffers.NewBuilder(1024),
	}
}

// Reset resets the builder for reuse
func (b *FlatBufferLogBuilder) Reset() {
	b.builder.Reset()
}

// BuildFromProtobuf builds a FlatBuffer log from a protobuf log
func (b *FlatBufferLogBuilder) BuildFromProtobuf(log *ledgerpb.Log) []byte {
	b.Reset()

	// Build payload first
	payloadOffset := b.buildLogPayload(log.Data)

	// Build date
	var dateOffset flatbuffers.UOffsetT
	if log.Date != nil {
		commandsfb.TimestampStart(b.builder)
		commandsfb.TimestampAddData(b.builder, log.Date.Data)
		dateOffset = commandsfb.TimestampEnd(b.builder)
	}

	// Build idempotency
	var idempotencyOffset flatbuffers.UOffsetT
	if log.Idempotency != nil {
		keyOffset := b.builder.CreateString(log.Idempotency.Key)
		hashOffset := b.builder.CreateByteVector(log.Idempotency.Hash)
		commandsfb.IdempotencyStart(b.builder)
		commandsfb.IdempotencyAddKey(b.builder, keyOffset)
		commandsfb.IdempotencyAddHash(b.builder, hashOffset)
		idempotencyOffset = commandsfb.IdempotencyEnd(b.builder)
	}

	// Build log
	commandsfb.LogStart(b.builder)
	if payloadOffset != 0 {
		commandsfb.LogAddData(b.builder, payloadOffset)
	}
	if dateOffset != 0 {
		commandsfb.LogAddDate(b.builder, dateOffset)
	}
	if idempotencyOffset != 0 {
		commandsfb.LogAddIdempotency(b.builder, idempotencyOffset)
	}
	commandsfb.LogAddId(b.builder, log.Id)
	commandsfb.LogAddLedgerId(b.builder, log.LedgerId)
	logOffset := commandsfb.LogEnd(b.builder)

	b.builder.Finish(logOffset)
	// Make a copy of the bytes since the builder's internal buffer will be reused
	result := make([]byte, len(b.builder.FinishedBytes()))
	copy(result, b.builder.FinishedBytes())
	return result
}

func (b *FlatBufferLogBuilder) buildLogPayload(payload *ledgerpb.LogPayload) flatbuffers.UOffsetT {
	if payload == nil {
		return 0
	}

	var (
		createdTxOffset   flatbuffers.UOffsetT
		revertedTxOffset  flatbuffers.UOffsetT
		savedMetaOffset   flatbuffers.UOffsetT
		deletedMetaOffset flatbuffers.UOffsetT
	)

	switch p := payload.Payload.(type) {
	case *ledgerpb.LogPayload_CreatedTransaction:
		createdTxOffset = b.buildCreatedTransaction(p.CreatedTransaction)
	case *ledgerpb.LogPayload_RevertedTransaction:
		revertedTxOffset = b.buildRevertedTransaction(p.RevertedTransaction)
	case *ledgerpb.LogPayload_SavedMetadata:
		savedMetaOffset = b.buildSavedMetadata(p.SavedMetadata)
	case *ledgerpb.LogPayload_DeletedMetadata:
		deletedMetaOffset = b.buildDeletedMetadata(p.DeletedMetadata)
	}

	commandsfb.LogPayloadStart(b.builder)
	if createdTxOffset != 0 {
		commandsfb.LogPayloadAddCreatedTransaction(b.builder, createdTxOffset)
	}
	if revertedTxOffset != 0 {
		commandsfb.LogPayloadAddRevertedTransaction(b.builder, revertedTxOffset)
	}
	if savedMetaOffset != 0 {
		commandsfb.LogPayloadAddSavedMetadata(b.builder, savedMetaOffset)
	}
	if deletedMetaOffset != 0 {
		commandsfb.LogPayloadAddDeletedMetadata(b.builder, deletedMetaOffset)
	}
	return commandsfb.LogPayloadEnd(b.builder)
}

func (b *FlatBufferLogBuilder) buildCreatedTransaction(ct *ledgerpb.CreatedTransaction) flatbuffers.UOffsetT {
	txOffset := b.buildTransaction(ct.Transaction)

	// Build account metadata
	var accountMetadataOffset flatbuffers.UOffsetT
	if len(ct.AccountMetadata) > 0 {
		offsets := make([]flatbuffers.UOffsetT, 0, len(ct.AccountMetadata))
		for key, meta := range ct.AccountMetadata {
			keyOffset := b.builder.CreateString(key)
			metaOffset := b.buildMetadata(meta)
			commandsfb.AccountMetadataEntryStart(b.builder)
			commandsfb.AccountMetadataEntryAddKey(b.builder, keyOffset)
			commandsfb.AccountMetadataEntryAddValue(b.builder, metaOffset)
			offsets = append(offsets, commandsfb.AccountMetadataEntryEnd(b.builder))
		}
		commandsfb.CreatedTransactionStartAccountMetadataVector(b.builder, len(offsets))
		for i := len(offsets) - 1; i >= 0; i-- {
			b.builder.PrependUOffsetT(offsets[i])
		}
		accountMetadataOffset = b.builder.EndVector(len(offsets))
	}

	commandsfb.CreatedTransactionStart(b.builder)
	if txOffset != 0 {
		commandsfb.CreatedTransactionAddTransaction(b.builder, txOffset)
	}
	if accountMetadataOffset != 0 {
		commandsfb.CreatedTransactionAddAccountMetadata(b.builder, accountMetadataOffset)
	}
	return commandsfb.CreatedTransactionEnd(b.builder)
}

func (b *FlatBufferLogBuilder) buildRevertedTransaction(rt *ledgerpb.RevertedTransaction) flatbuffers.UOffsetT {
	txOffset := b.buildTransaction(rt.RevertTransaction)

	commandsfb.RevertedTransactionStart(b.builder)
	commandsfb.RevertedTransactionAddRevertedTransactionId(b.builder, rt.RevertedTransactionId)
	if txOffset != 0 {
		commandsfb.RevertedTransactionAddRevertTransaction(b.builder, txOffset)
	}
	return commandsfb.RevertedTransactionEnd(b.builder)
}

func (b *FlatBufferLogBuilder) buildSavedMetadata(sm *ledgerpb.SavedMetadata) flatbuffers.UOffsetT {
	targetOffset := b.buildTarget(sm.Target)
	metaOffset := b.buildMetadata(sm.Metadata)

	commandsfb.SavedMetadataStart(b.builder)
	if targetOffset != 0 {
		commandsfb.SavedMetadataAddTarget(b.builder, targetOffset)
	}
	if metaOffset != 0 {
		commandsfb.SavedMetadataAddMetadata(b.builder, metaOffset)
	}
	return commandsfb.SavedMetadataEnd(b.builder)
}

func (b *FlatBufferLogBuilder) buildDeletedMetadata(dm *ledgerpb.DeletedMetadata) flatbuffers.UOffsetT {
	targetOffset := b.buildTarget(dm.Target)
	keyOffset := b.builder.CreateString(dm.Key)

	commandsfb.DeletedMetadataStart(b.builder)
	if targetOffset != 0 {
		commandsfb.DeletedMetadataAddTarget(b.builder, targetOffset)
	}
	commandsfb.DeletedMetadataAddKey(b.builder, keyOffset)
	return commandsfb.DeletedMetadataEnd(b.builder)
}

func (b *FlatBufferLogBuilder) buildTransaction(tx *ledgerpb.Transaction) flatbuffers.UOffsetT {
	if tx == nil {
		return 0
	}

	// Build postings
	var postingsOffset flatbuffers.UOffsetT
	if len(tx.Postings) > 0 {
		offsets := make([]flatbuffers.UOffsetT, len(tx.Postings))
		for i, posting := range tx.Postings {
			offsets[i] = b.buildPosting(posting)
		}
		commandsfb.TransactionStartPostingsVector(b.builder, len(offsets))
		for i := len(offsets) - 1; i >= 0; i-- {
			b.builder.PrependUOffsetT(offsets[i])
		}
		postingsOffset = b.builder.EndVector(len(offsets))
	}

	// Build metadata
	var metadataOffset flatbuffers.UOffsetT
	if len(tx.Metadata) > 0 {
		offsets := make([]flatbuffers.UOffsetT, 0, len(tx.Metadata))
		for key, value := range tx.Metadata {
			keyOffset := b.builder.CreateString(key)
			valueOffset := b.builder.CreateString(value)
			commandsfb.MetadataEntryStart(b.builder)
			commandsfb.MetadataEntryAddKey(b.builder, keyOffset)
			commandsfb.MetadataEntryAddValue(b.builder, valueOffset)
			offsets = append(offsets, commandsfb.MetadataEntryEnd(b.builder))
		}
		commandsfb.TransactionStartMetadataVector(b.builder, len(offsets))
		for i := len(offsets) - 1; i >= 0; i-- {
			b.builder.PrependUOffsetT(offsets[i])
		}
		metadataOffset = b.builder.EndVector(len(offsets))
	}

	// Build timestamps
	var timestampOffset, insertedAtOffset, updatedAtOffset, revertedAtOffset flatbuffers.UOffsetT
	if tx.Timestamp != nil {
		commandsfb.TimestampStart(b.builder)
		commandsfb.TimestampAddData(b.builder, tx.Timestamp.Data)
		timestampOffset = commandsfb.TimestampEnd(b.builder)
	}
	if tx.InsertedAt != nil {
		commandsfb.TimestampStart(b.builder)
		commandsfb.TimestampAddData(b.builder, tx.InsertedAt.Data)
		insertedAtOffset = commandsfb.TimestampEnd(b.builder)
	}
	if tx.UpdatedAt != nil {
		commandsfb.TimestampStart(b.builder)
		commandsfb.TimestampAddData(b.builder, tx.UpdatedAt.Data)
		updatedAtOffset = commandsfb.TimestampEnd(b.builder)
	}
	if tx.RevertedAt != nil {
		commandsfb.TimestampStart(b.builder)
		commandsfb.TimestampAddData(b.builder, tx.RevertedAt.Data)
		revertedAtOffset = commandsfb.TimestampEnd(b.builder)
	}

	// Build reference
	var referenceOffset flatbuffers.UOffsetT
	if tx.Reference != "" {
		referenceOffset = b.builder.CreateString(tx.Reference)
	}

	commandsfb.TransactionStart(b.builder)
	if postingsOffset != 0 {
		commandsfb.TransactionAddPostings(b.builder, postingsOffset)
	}
	if metadataOffset != 0 {
		commandsfb.TransactionAddMetadata(b.builder, metadataOffset)
	}
	if timestampOffset != 0 {
		commandsfb.TransactionAddTimestamp(b.builder, timestampOffset)
	}
	if referenceOffset != 0 {
		commandsfb.TransactionAddReference(b.builder, referenceOffset)
	}
	commandsfb.TransactionAddId(b.builder, tx.Id)
	commandsfb.TransactionAddReverted(b.builder, tx.Reverted)
	if insertedAtOffset != 0 {
		commandsfb.TransactionAddInsertedAt(b.builder, insertedAtOffset)
	}
	if updatedAtOffset != 0 {
		commandsfb.TransactionAddUpdatedAt(b.builder, updatedAtOffset)
	}
	if revertedAtOffset != 0 {
		commandsfb.TransactionAddRevertedAt(b.builder, revertedAtOffset)
	}
	return commandsfb.TransactionEnd(b.builder)
}

func (b *FlatBufferLogBuilder) buildPosting(posting *ledgerpb.Posting) flatbuffers.UOffsetT {
	sourceOffset := b.builder.CreateString(posting.Source)
	destOffset := b.builder.CreateString(posting.Destination)
	assetOffset := b.builder.CreateString(posting.Asset)

	var amountOffset flatbuffers.UOffsetT
	if posting.Amount != nil {
		dataOffset := b.builder.CreateByteVector(posting.Amount.Data)
		commandsfb.BigIntStart(b.builder)
		commandsfb.BigIntAddData(b.builder, dataOffset)
		amountOffset = commandsfb.BigIntEnd(b.builder)
	}

	commandsfb.PostingStart(b.builder)
	commandsfb.PostingAddSource(b.builder, sourceOffset)
	commandsfb.PostingAddDestination(b.builder, destOffset)
	if amountOffset != 0 {
		commandsfb.PostingAddAmount(b.builder, amountOffset)
	}
	commandsfb.PostingAddAsset(b.builder, assetOffset)
	return commandsfb.PostingEnd(b.builder)
}

func (b *FlatBufferLogBuilder) buildMetadata(meta *ledgerpb.Metadata) flatbuffers.UOffsetT {
	if meta == nil || len(meta.Entries) == 0 {
		commandsfb.MetadataStart(b.builder)
		return commandsfb.MetadataEnd(b.builder)
	}

	offsets := make([]flatbuffers.UOffsetT, 0, len(meta.Entries))
	for key, value := range meta.Entries {
		keyOffset := b.builder.CreateString(key)
		valueOffset := b.builder.CreateString(value)
		commandsfb.EntriesEntryStart(b.builder)
		commandsfb.EntriesEntryAddKey(b.builder, keyOffset)
		commandsfb.EntriesEntryAddValue(b.builder, valueOffset)
		offsets = append(offsets, commandsfb.EntriesEntryEnd(b.builder))
	}

	commandsfb.MetadataStartEntriesVector(b.builder, len(offsets))
	for i := len(offsets) - 1; i >= 0; i-- {
		b.builder.PrependUOffsetT(offsets[i])
	}
	entriesOffset := b.builder.EndVector(len(offsets))

	commandsfb.MetadataStart(b.builder)
	commandsfb.MetadataAddEntries(b.builder, entriesOffset)
	return commandsfb.MetadataEnd(b.builder)
}

func (b *FlatBufferLogBuilder) buildTarget(target *ledgerpb.Target) flatbuffers.UOffsetT {
	if target == nil {
		return 0
	}

	var anonymous1Offset flatbuffers.UOffsetT

	switch t := target.Target.(type) {
	case *ledgerpb.Target_Account:
		addrOffset := b.builder.CreateString(t.Account.Addr)
		commandsfb.TargetAccountStart(b.builder)
		commandsfb.TargetAccountAddAddr(b.builder, addrOffset)
		accountOffset := commandsfb.TargetAccountEnd(b.builder)

		commandsfb.Anonymous1Start(b.builder)
		commandsfb.Anonymous1AddAccount(b.builder, accountOffset)
		anonymous1Offset = commandsfb.Anonymous1End(b.builder)

	case *ledgerpb.Target_Transaction:
		commandsfb.TargetTransactionStart(b.builder)
		commandsfb.TargetTransactionAddId(b.builder, t.Transaction.Id)
		txOffset := commandsfb.TargetTransactionEnd(b.builder)

		commandsfb.Anonymous1Start(b.builder)
		commandsfb.Anonymous1AddTransaction(b.builder, txOffset)
		anonymous1Offset = commandsfb.Anonymous1End(b.builder)
	}

	commandsfb.TargetStart(b.builder)
	commandsfb.TargetAddTarget(b.builder, anonymous1Offset)
	return commandsfb.TargetEnd(b.builder)
}
