package v2

import (
	stdjson "encoding/json"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/bytedance/sonic"
	"github.com/holiman/uint256"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// Pools for V2 data structs. sonic.Unmarshal reuses existing slice capacity,
// so pre-allocated Postings slices avoid growslice on the hot path.
var (
	v2NewTxPool = sync.Pool{New: func() any {
		return &V2NewTransactionData{
			Transaction: V2Transaction{
				Postings: make([]V2Posting, 0, 8),
			},
		}
	}}
	v2RevertPool = sync.Pool{New: func() any {
		return &V2RevertedTransactionData{
			RevertTransaction: V2Transaction{
				Postings: make([]V2Posting, 0, 8),
			},
		}
	}}
)

// TranslateBatch translates a batch of v2 logs into v3 Raft orders.
// It generates FillGap orders for any gaps in log IDs or transaction IDs.
// expectedNextLogID and expectedNextTxID are used to detect gaps.
func TranslateBatch(ledger string, v2Logs []V2Log, expectedNextLogID, expectedNextTxID uint64) ([]*raftcmdpb.Order, uint64, uint64, error) {
	orders := make([]*raftcmdpb.Order, 0, len(v2Logs))

	for _, v2Log := range v2Logs {
		// Detect log ID gaps and fill them
		for expectedNextLogID < v2Log.ID {
			orders = append(orders, makeMirrorOrder(ledger, &raftcmdpb.MirrorLogEntry{
				V2LogId: expectedNextLogID,
				Data: &raftcmdpb.MirrorLogEntry_FillGap{
					FillGap: &raftcmdpb.MirrorFillGap{},
				},
			}))
			expectedNextLogID++
		}

		entry, newNextTxID, err := translateV2Log(v2Log, expectedNextTxID)
		if err != nil {
			return nil, 0, 0, fmt.Errorf("translating v2 log %d (type=%s): %w", v2Log.ID, v2Log.Type, err)
		}

		expectedNextTxID = newNextTxID

		if entry != nil {
			orders = append(orders, makeMirrorOrder(ledger, entry))
		}

		expectedNextLogID = v2Log.ID + 1
	}

	return orders, expectedNextLogID, expectedNextTxID, nil
}

func makeMirrorOrder(ledger string, entry *raftcmdpb.MirrorLogEntry) *raftcmdpb.Order {
	return &raftcmdpb.Order{
		Type: &raftcmdpb.Order_MirrorIngest{
			MirrorIngest: &raftcmdpb.MirrorIngestOrder{
				Ledger: ledger,
				Entry:  entry,
			},
		},
	}
}

func translateV2Log(v2Log V2Log, expectedNextTxID uint64) (*raftcmdpb.MirrorLogEntry, uint64, error) {
	switch v2Log.Type {
	case "NEW_TRANSACTION":
		return translateNewTransaction(v2Log, expectedNextTxID)
	case "SET_METADATA":
		entry, err := translateSetMetadata(v2Log)

		return entry, expectedNextTxID, err
	case "REVERTED_TRANSACTION":
		return translateRevertedTransaction(v2Log, expectedNextTxID)
	case "DELETE_METADATA":
		entry, err := translateDeleteMetadata(v2Log)

		return entry, expectedNextTxID, err
	default:
		// Unknown log types (e.g., INSERTED_SCHEMA) → fill gap
		return &raftcmdpb.MirrorLogEntry{
			V2LogId: v2Log.ID,
			Data: &raftcmdpb.MirrorLogEntry_FillGap{
				FillGap: &raftcmdpb.MirrorFillGap{},
			},
		}, expectedNextTxID, nil
	}
}

func translateNewTransaction(v2Log V2Log, _ uint64) (*raftcmdpb.MirrorLogEntry, uint64, error) {
	data, ok := v2NewTxPool.Get().(*V2NewTransactionData)
	if !ok {
		panic("unexpected type from v2NewTxPool")
	}

	if err := sonic.Unmarshal(v2Log.Data, data); err != nil {
		resetV2NewTxData(data)
		v2NewTxPool.Put(data)

		return nil, 0, fmt.Errorf("unmarshaling NEW_TRANSACTION data: %w", err)
	}

	txID := data.Transaction.ID

	postings, err := translatePostings(data.Transaction.Postings)
	if err != nil {
		resetV2NewTxData(data)
		v2NewTxPool.Put(data)

		return nil, 0, err
	}

	metadata := translateMetadataMap(data.Transaction.Metadata)

	var timestamp *commonpb.Timestamp

	if data.Transaction.Timestamp != "" {
		ts, err := time.Parse(time.RFC3339Nano, data.Transaction.Timestamp)
		if err == nil {
			timestamp = &commonpb.Timestamp{Data: uint64(ts.UnixMicro())}
		}
	}

	var accountMetadata map[string]*commonpb.MetadataMap
	if len(data.AccountMetadata) > 0 {
		accountMetadata = make(map[string]*commonpb.MetadataMap, len(data.AccountMetadata))
		for account, meta := range data.AccountMetadata {
			accountMetadata[account] = &commonpb.MetadataMap{Values: translateMetadataMap(meta)}
		}
	}

	entry := &raftcmdpb.MirrorLogEntry{
		V2LogId: v2Log.ID,
		Data: &raftcmdpb.MirrorLogEntry_CreatedTransaction{
			CreatedTransaction: &raftcmdpb.MirrorCreatedTransaction{
				TransactionId:   txID,
				Postings:        postings,
				Metadata:        metadata,
				Timestamp:       timestamp,
				Reference:       data.Transaction.Reference,
				AccountMetadata: accountMetadata,
			},
		},
	}

	resetV2NewTxData(data)
	v2NewTxPool.Put(data)

	return entry, txID + 1, nil
}

// resetV2NewTxData zeroes the struct while preserving the Postings backing array.
func resetV2NewTxData(d *V2NewTransactionData) {
	postings := d.Transaction.Postings[:0]
	*d = V2NewTransactionData{}
	d.Transaction.Postings = postings
}

func translateSetMetadata(v2Log V2Log) (*raftcmdpb.MirrorLogEntry, error) {
	var data V2SetMetadataData
	if err := sonic.Unmarshal(v2Log.Data, &data); err != nil {
		return nil, fmt.Errorf("unmarshaling SET_METADATA data: %w", err)
	}

	target, err := translateTarget(data.TargetType, data.TargetID)
	if err != nil {
		return nil, err
	}

	return &raftcmdpb.MirrorLogEntry{
		V2LogId: v2Log.ID,
		Data: &raftcmdpb.MirrorLogEntry_SavedMetadata{
			SavedMetadata: &raftcmdpb.MirrorSavedMetadata{
				Target:   target,
				Metadata: translateMetadataMap(data.Metadata),
			},
		},
	}, nil
}

func translateRevertedTransaction(v2Log V2Log, expectedNextTxID uint64) (*raftcmdpb.MirrorLogEntry, uint64, error) {
	data := v2RevertPool.Get().(*V2RevertedTransactionData)

	if err := sonic.Unmarshal(v2Log.Data, data); err != nil {
		resetV2RevertData(data)
		v2RevertPool.Put(data)

		return nil, 0, fmt.Errorf("unmarshaling REVERTED_TRANSACTION data: %w", err)
	}

	revertTxID := data.RevertTransaction.ID

	postings, err := translatePostings(data.RevertTransaction.Postings)
	if err != nil {
		resetV2RevertData(data)
		v2RevertPool.Put(data)

		return nil, 0, err
	}

	var timestamp *commonpb.Timestamp

	if data.RevertTransaction.Timestamp != "" {
		ts, err := time.Parse(time.RFC3339Nano, data.RevertTransaction.Timestamp)
		if err == nil {
			timestamp = &commonpb.Timestamp{Data: uint64(ts.UnixMicro())}
		}
	}

	entry := &raftcmdpb.MirrorLogEntry{
		V2LogId: v2Log.ID,
		Data: &raftcmdpb.MirrorLogEntry_RevertedTransaction{
			RevertedTransaction: &raftcmdpb.MirrorRevertedTransaction{
				RevertedTransactionId: data.RevertedTransactionID,
				NewTransactionId:      revertTxID,
				ReversePostings:       postings,
				Metadata:              translateMetadataMap(data.RevertTransaction.Metadata),
				Timestamp:             timestamp,
			},
		},
	}

	newNextTxID := expectedNextTxID
	if revertTxID >= newNextTxID {
		newNextTxID = revertTxID + 1
	}

	resetV2RevertData(data)
	v2RevertPool.Put(data)

	return entry, newNextTxID, nil
}

// resetV2RevertData zeroes the struct while preserving the Postings backing array.
func resetV2RevertData(d *V2RevertedTransactionData) {
	postings := d.RevertTransaction.Postings[:0]
	*d = V2RevertedTransactionData{}
	d.RevertTransaction.Postings = postings
}

func translateDeleteMetadata(v2Log V2Log) (*raftcmdpb.MirrorLogEntry, error) {
	var data V2DeleteMetadataData
	if err := sonic.Unmarshal(v2Log.Data, &data); err != nil {
		return nil, fmt.Errorf("unmarshaling DELETE_METADATA data: %w", err)
	}

	target, err := translateTarget(data.TargetType, data.TargetID)
	if err != nil {
		return nil, err
	}

	return &raftcmdpb.MirrorLogEntry{
		V2LogId: v2Log.ID,
		Data: &raftcmdpb.MirrorLogEntry_DeletedMetadata{
			DeletedMetadata: &raftcmdpb.MirrorDeletedMetadata{
				Target: target,
				Key:    data.Key,
			},
		},
	}, nil
}

// translatePostings converts v2 postings to v3 proto postings.
// Uses batch-allocated backing arrays to reduce per-posting heap allocations
// from 3N+1 to 3 (one []Posting, one []Uint256, one []*Posting).
func translatePostings(v2Postings []V2Posting) ([]*commonpb.Posting, error) {
	n := len(v2Postings)
	postingBuf := make([]commonpb.Posting, n)
	uint256Buf := make([]commonpb.Uint256, n)
	ptrs := make([]*commonpb.Posting, n)

	for i, p := range v2Postings {
		err := parseUint256Into(p.Amount.String(), &uint256Buf[i])
		if err != nil {
			return nil, fmt.Errorf("parsing posting amount %q: %w", p.Amount.String(), err)
		}

		postingBuf[i] = commonpb.Posting{
			Source:      p.Source,
			Destination: p.Destination,
			Amount:      &uint256Buf[i],
			Asset:       p.Asset,
		}
		ptrs[i] = &postingBuf[i]
	}

	return ptrs, nil
}

// parseUint256Into parses a decimal string directly into a pre-allocated Uint256.
func parseUint256Into(s string, dst *commonpb.Uint256) error {
	if len(s) > 0 && s[0] == '-' {
		return fmt.Errorf("negative amount: %s", s)
	}

	var u uint256.Int

	err := u.SetFromDecimal(s)
	if err != nil {
		return fmt.Errorf("invalid uint256: %s", s)
	}

	dst.V0 = u[0]
	dst.V1 = u[1]
	dst.V2 = u[2]
	dst.V3 = u[3]

	return nil
}

// translateTarget converts v2 target type and ID to a v3 Target.
func translateTarget(targetType string, rawID stdjson.RawMessage) (*commonpb.Target, error) {
	switch targetType {
	case "TRANSACTION":
		var txID uint64

		err := sonic.Unmarshal(rawID, &txID)
		if err != nil {
			// Try string format
			var s string

			err2 := sonic.Unmarshal(rawID, &s)
			if err2 != nil {
				return nil, fmt.Errorf("parsing transaction target ID: %w", err)
			}

			parsed, err3 := strconv.ParseUint(s, 10, 64)
			if err3 != nil {
				return nil, fmt.Errorf("parsing transaction target ID string: %w", err3)
			}

			txID = parsed
		}

		return &commonpb.Target{
			Target: &commonpb.Target_Transaction{
				Transaction: &commonpb.TargetTransaction{
					Identifier: &commonpb.TargetTransaction_Id{Id: txID},
				},
			},
		}, nil
	case "ACCOUNT":
		var addr string

		err := sonic.Unmarshal(rawID, &addr)
		if err != nil {
			return nil, fmt.Errorf("parsing account target ID: %w", err)
		}

		return &commonpb.Target{
			Target: &commonpb.Target_Account{
				Account: &commonpb.TargetAccount{Addr: addr},
			},
		}, nil
	default:
		return nil, fmt.Errorf("unknown target type: %s", targetType)
	}
}

// translateMetadataMap converts v2 string metadata to proto metadata values.
func translateMetadataMap(meta map[string]string) map[string]*commonpb.MetadataValue {
	if len(meta) == 0 {
		return nil
	}

	result := make(map[string]*commonpb.MetadataValue, len(meta))
	for key, value := range meta {
		result[key] = &commonpb.MetadataValue{
			Type: &commonpb.MetadataValue_StringValue{StringValue: value},
		}
	}

	return result
}
