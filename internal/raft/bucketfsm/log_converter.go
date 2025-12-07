package bucketfsm

import (
	"fmt"
	"math/big"

	"github.com/formancehq/go-libs/v3/metadata"
	"github.com/formancehq/go-libs/v3/time"
	ledger "github.com/formancehq/ledger-v3-poc/internal"
	"github.com/formancehq/ledger-v3-poc/internal/service"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// logToProto converts a ledger.Log to protobuf Log
func logToProto(l ledger.Log) (*Log, error) {
	logProto := &Log{
		Type:            int32(l.Type),
		Ledger:          l.Ledger,
		IdempotencyKey:  l.IdempotencyKey,
		IdempotencyHash: l.IdempotencyHash,
	}

	if l.ID != nil {
		logProto.Id = *l.ID
	}

	if !l.Date.IsZero() {
		logProto.Date = timestamppb.New(l.Date.Time)
	}

	// Convert LogPayload to protobuf
	logPayloadProto, err := logPayloadToProto(l.Data)
	if err != nil {
		return nil, fmt.Errorf("converting log payload to proto: %w", err)
	}
	logProto.Data = logPayloadProto

	return logProto, nil
}

// logFromProto converts a protobuf Log to ledger.Log
func logFromProto(l *Log) (ledger.Log, error) {
	log := ledger.Log{
		Type:            ledger.LogType(l.Type),
		Ledger:          l.Ledger,
		IdempotencyKey:  l.IdempotencyKey,
		IdempotencyHash: l.IdempotencyHash,
	}

	if l.Id != 0 {
		id := l.Id
		log.ID = &id
	}

	if l.Date != nil {
		log.Date = time.New(l.Date.AsTime())
	}

	// Convert protobuf LogPayload to ledger.LogPayload
	logPayload, err := logPayloadFromProto(l.Data)
	if err != nil {
		return log, fmt.Errorf("converting log payload from proto: %w", err)
	}
	log.Data = logPayload

	return log, nil
}

// logPayloadToProto converts a ledger.LogPayload to protobuf LogPayload
func logPayloadToProto(payload ledger.LogPayload) (*LogPayload, error) {
	switch p := payload.(type) {
	case *ledger.CreatedTransaction:
		return &LogPayload{
			Payload: &LogPayload_CreatedTransaction{
				CreatedTransaction: createdTransactionToProto(p),
			},
		}, nil
	case *ledger.RevertedTransaction:
		return &LogPayload{
			Payload: &LogPayload_RevertedTransaction{
				RevertedTransaction: revertedTransactionToProto(p),
			},
		}, nil
	case *ledger.SavedMetadata:
		return &LogPayload{
			Payload: &LogPayload_SavedMetadata{
				SavedMetadata: savedMetadataToProto(p),
			},
		}, nil
	case *ledger.DeletedMetadata:
		return &LogPayload{
			Payload: &LogPayload_DeletedMetadata{
				DeletedMetadata: deletedMetadataToProto(p),
			},
		}, nil
	default:
		return nil, fmt.Errorf("unknown log payload type: %T", payload)
	}
}

// logPayloadFromProto converts a protobuf LogPayload to ledger.LogPayload
func logPayloadFromProto(payload *LogPayload) (ledger.LogPayload, error) {
	if payload == nil {
		return nil, fmt.Errorf("log payload is nil")
	}

	switch p := payload.Payload.(type) {
	case *LogPayload_CreatedTransaction:
		return createdTransactionFromProto(p.CreatedTransaction)
	case *LogPayload_RevertedTransaction:
		return revertedTransactionFromProto(p.RevertedTransaction)
	case *LogPayload_SavedMetadata:
		return savedMetadataFromProto(p.SavedMetadata)
	case *LogPayload_DeletedMetadata:
		return deletedMetadataFromProto(p.DeletedMetadata)
	default:
		return nil, fmt.Errorf("unknown log payload type: %T", payload.Payload)
	}
}

// Helper functions for conversion

func createdTransactionToProto(ct *ledger.CreatedTransaction) *CreatedTransaction {
	txProto := transactionToProto(ct.Transaction)
	accountMetadata := make(map[string]*structpb.Struct)
	for addr, md := range ct.AccountMetadata {
		if s, err := metadataToStruct(md); err == nil {
			accountMetadata[addr] = s
		}
	}
	return &CreatedTransaction{
		Transaction:     txProto,
		AccountMetadata: accountMetadata,
	}
}

func createdTransactionFromProto(ct *CreatedTransaction) (*ledger.CreatedTransaction, error) {
	tx, err := transactionFromProto(ct.Transaction)
	if err != nil {
		return nil, err
	}
	accountMetadata := make(ledger.AccountMetadata)
	for addr, md := range ct.AccountMetadata {
		if md != nil {
			accountMetadata[addr] = structToMetadata(md)
		}
	}
	return &ledger.CreatedTransaction{
		Transaction:     tx,
		AccountMetadata: accountMetadata,
	}, nil
}

func revertedTransactionToProto(rt *ledger.RevertedTransaction) *RevertedTransaction {
	return &RevertedTransaction{
		RevertedTransaction: transactionToProto(rt.RevertedTransaction),
		RevertTransaction:   transactionToProto(rt.RevertTransaction),
	}
}

func revertedTransactionFromProto(rt *RevertedTransaction) (*ledger.RevertedTransaction, error) {
	revertedTx, err := transactionFromProto(rt.RevertedTransaction)
	if err != nil {
		return nil, err
	}
	revertTx, err := transactionFromProto(rt.RevertTransaction)
	if err != nil {
		return nil, err
	}
	return &ledger.RevertedTransaction{
		RevertedTransaction: revertedTx,
		RevertTransaction:   revertTx,
	}, nil
}

func savedMetadataToProto(sm *ledger.SavedMetadata) *SavedMetadata {
	mdStruct, _ := metadataToStruct(sm.Metadata)
	proto := &SavedMetadata{
		TargetType: sm.TargetType,
		Metadata:   mdStruct,
	}
	switch id := sm.TargetID.(type) {
	case string:
		proto.TargetId = &SavedMetadata_AccountId{AccountId: id}
	case uint64:
		proto.TargetId = &SavedMetadata_TransactionId{TransactionId: id}
	}
	return proto
}

func savedMetadataFromProto(sm *SavedMetadata) (*ledger.SavedMetadata, error) {
	var targetID interface{}
	switch id := sm.TargetId.(type) {
	case *SavedMetadata_AccountId:
		targetID = id.AccountId
	case *SavedMetadata_TransactionId:
		targetID = id.TransactionId
	default:
		return nil, fmt.Errorf("unknown target ID type")
	}
	return &ledger.SavedMetadata{
		TargetType: sm.TargetType,
		TargetID:   targetID,
		Metadata:   structToMetadata(sm.Metadata),
	}, nil
}

func deletedMetadataToProto(dm *ledger.DeletedMetadata) *DeletedMetadata {
	proto := &DeletedMetadata{
		TargetType: dm.TargetType,
		Key:        dm.Key,
	}
	switch id := dm.TargetID.(type) {
	case string:
		proto.TargetId = &DeletedMetadata_AccountId{AccountId: id}
	case uint64:
		proto.TargetId = &DeletedMetadata_TransactionId{TransactionId: id}
	}
	return proto
}

func deletedMetadataFromProto(dm *DeletedMetadata) (*ledger.DeletedMetadata, error) {
	var targetID interface{}
	switch id := dm.TargetId.(type) {
	case *DeletedMetadata_AccountId:
		targetID = id.AccountId
	case *DeletedMetadata_TransactionId:
		targetID = id.TransactionId
	default:
		return nil, fmt.Errorf("unknown target ID type")
	}
	return &ledger.DeletedMetadata{
		TargetType: dm.TargetType,
		TargetID:   targetID,
		Key:        dm.Key,
	}, nil
}

func transactionToProto(tx ledger.Transaction) *service.Transaction {
	postings := make([]*service.Posting, 0, len(tx.Postings))
	for _, p := range tx.Postings {
		postings = append(postings, &service.Posting{
			Source:      p.Source,
			Destination: p.Destination,
			Amount:      p.Amount.String(),
			Asset:       p.Asset,
		})
	}

	var metadata *structpb.Struct
	if len(tx.Metadata) > 0 {
		if md, err := metadataToStruct(tx.Metadata); err == nil {
			metadata = md
		}
	}

	var timestamp *timestamppb.Timestamp
	if !tx.Timestamp.IsZero() {
		timestamp = timestamppb.New(tx.Timestamp.Time)
	}

	var id uint64
	if tx.ID != nil {
		id = *tx.ID
	}

	return &service.Transaction{
		Postings:  postings,
		Metadata:  metadata,
		Timestamp: timestamp,
		Reference: tx.Reference,
		Id:        id,
		Reverted:  tx.RevertedAt != nil,
	}
}

func transactionFromProto(tx *service.Transaction) (ledger.Transaction, error) {
	postings := make(ledger.Postings, 0, len(tx.Postings))
	for _, p := range tx.Postings {
		amount, ok := new(big.Int).SetString(p.Amount, 10)
		if !ok {
			return ledger.Transaction{}, fmt.Errorf("invalid amount: %s", p.Amount)
		}
		postings = append(postings, ledger.NewPosting(p.Source, p.Destination, p.Asset, amount))
	}

	txResult := ledger.NewTransaction()
	txResult = txResult.WithPostings(postings...)

	if tx.Metadata != nil {
		txResult = txResult.WithMetadata(structToMetadata(tx.Metadata))
	}

	if tx.Timestamp != nil {
		txResult = txResult.WithTimestamp(time.New(tx.Timestamp.AsTime()))
	}

	if tx.Reference != "" {
		txResult = txResult.WithReference(tx.Reference)
	}

	if tx.Id != 0 {
		txResult = txResult.WithID(tx.Id)
	}

	return txResult, nil
}

func metadataToStruct(md metadata.Metadata) (*structpb.Struct, error) {
	if len(md) == 0 {
		return nil, nil
	}
	fields := make(map[string]*structpb.Value)
	for k, v := range md {
		val, err := structpb.NewValue(v)
		if err != nil {
			return nil, err
		}
		fields[k] = val
	}
	return &structpb.Struct{Fields: fields}, nil
}

func structToMetadata(s *structpb.Struct) metadata.Metadata {
	if s == nil {
		return metadata.Metadata{}
	}
	md := make(metadata.Metadata)
	for k, v := range s.Fields {
		md[k] = v.GetStringValue()
	}
	return md
}
