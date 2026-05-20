package indexbuilder

import (
	"errors"
	"fmt"

	"google.golang.org/protobuf/encoding/protowire"
)

// rawPosting holds the source and destination extracted from a Posting message.
type rawPosting struct {
	Source      string
	Destination string
}

// parsedLog holds the fields extracted by the protowire fast path.
type parsedLog struct {
	Sequence uint64
	Ledger   string
	LedgerID uint32
	TxID     uint64
	Postings []rawPosting // reused across iterations via truncate-to-zero
	LogType  int32        // LedgerLogPayload oneof tag: 1=created, 2=reverted, 0=skip
}

// parsePostingsFromLog extracts only the fields needed for posting indexation
// from the raw bytes of a serialized Log message. It skips ~70% of the payload
// (metadata, balances, volumes, timestamps, signatures, hash) by navigating
// directly to the nested fields via protowire.
//
// Wire path:
//
//	Log[1:sequence, 2:payload]
//	  → LogPayload[oneof: 3=apply]
//	    → ApplyLedgerLog[1:ledger_name, 2:log]
//	      → LedgerLog[1:data]
//	        → LedgerLogPayload[oneof: 1=created_tx, 2=reverted_tx]
//	          → CreatedTransaction[1:transaction] / RevertedTransaction[2:revert_transaction]
//	            → Transaction[1:postings(repeated), 5:id]
//	              → Posting[1:source, 2:destination]
func parsePostingsFromLog(data []byte, out *parsedLog) error {
	out.LogType = 0
	out.Postings = out.Postings[:0]
	out.Sequence = 0
	out.Ledger = ""
	out.TxID = 0

	// --- Log level: extract sequence (field 1) and payload (field 2) ---
	var payloadBytes []byte

	for len(data) > 0 {
		num, typ, n := protowire.ConsumeTag(data)
		if n < 0 {
			return errors.New("protowire: invalid tag in Log")
		}

		data = data[n:]

		switch {
		case num == 1 && typ == protowire.Fixed64Type:
			v, vn := protowire.ConsumeFixed64(data)
			if vn < 0 {
				return errors.New("protowire: invalid fixed64 for Log.sequence")
			}

			out.Sequence = v
			data = data[vn:]
		case num == 2 && typ == protowire.BytesType:
			b, bn := protowire.ConsumeBytes(data)
			if bn < 0 {
				return errors.New("protowire: invalid bytes for Log.payload")
			}

			payloadBytes = b
			data = data[bn:]
		default:
			n := protowire.ConsumeFieldValue(num, typ, data)
			if n < 0 {
				return errors.New("protowire: invalid field in Log")
			}

			data = data[n:]
		}
	}

	if payloadBytes == nil {
		return nil
	}

	// --- LogPayload level: extract oneof field 3 (apply) ---
	applyBytes, err := scanBytesField(payloadBytes, 3)
	if err != nil {
		return fmt.Errorf("LogPayload: %w", err)
	}

	if applyBytes == nil {
		return nil // not an Apply log
	}

	// --- ApplyLedgerLog level: extract ledger_name (field 1) and log (field 2) ---
	var ledgerLogBytes []byte

	for len(applyBytes) > 0 {
		num, typ, n := protowire.ConsumeTag(applyBytes)
		if n < 0 {
			return errors.New("protowire: invalid tag in ApplyLedgerLog")
		}

		applyBytes = applyBytes[n:]

		switch {
		case num == 1 && typ == protowire.BytesType:
			b, bn := protowire.ConsumeBytes(applyBytes)
			if bn < 0 {
				return errors.New("protowire: invalid bytes for ApplyLedgerLog.ledger_name")
			}

			out.Ledger = string(b)
			applyBytes = applyBytes[bn:]
		case num == 2 && typ == protowire.BytesType:
			b, bn := protowire.ConsumeBytes(applyBytes)
			if bn < 0 {
				return errors.New("protowire: invalid bytes for ApplyLedgerLog.log")
			}

			ledgerLogBytes = b
			applyBytes = applyBytes[bn:]
		default:
			n := protowire.ConsumeFieldValue(num, typ, applyBytes)
			if n < 0 {
				return errors.New("protowire: invalid field in ApplyLedgerLog")
			}

			applyBytes = applyBytes[n:]
		}
	}

	if ledgerLogBytes == nil {
		return nil
	}

	// --- LedgerLog level: extract data (field 1) ---
	dataBytes, err := scanBytesField(ledgerLogBytes, 1)
	if err != nil {
		return fmt.Errorf("LedgerLog: %w", err)
	}

	if dataBytes == nil {
		return nil
	}

	// --- LedgerLogPayload level: determine oneof type and extract transaction bytes ---
	var txBytes []byte

	for len(dataBytes) > 0 {
		num, typ, n := protowire.ConsumeTag(dataBytes)
		if n < 0 {
			return errors.New("protowire: invalid tag in LedgerLogPayload")
		}

		dataBytes = dataBytes[n:]

		switch {
		case num == 1 && typ == protowire.BytesType:
			// CreatedTransaction
			b, bn := protowire.ConsumeBytes(dataBytes)
			if bn < 0 {
				return errors.New("protowire: invalid bytes for CreatedTransaction")
			}

			out.LogType = 1
			// CreatedTransaction.transaction is field 1
			txBytes, err = scanBytesField(b, 1)
			if err != nil {
				return fmt.Errorf("CreatedTransaction: %w", err)
			}

			dataBytes = dataBytes[bn:]
		case num == 2 && typ == protowire.BytesType:
			// RevertedTransaction
			b, bn := protowire.ConsumeBytes(dataBytes)
			if bn < 0 {
				return errors.New("protowire: invalid bytes for RevertedTransaction")
			}

			out.LogType = 2
			// RevertedTransaction.revert_transaction is field 2
			txBytes, err = scanBytesField(b, 2)
			if err != nil {
				return fmt.Errorf("RevertedTransaction: %w", err)
			}

			dataBytes = dataBytes[bn:]
		default:
			n := protowire.ConsumeFieldValue(num, typ, dataBytes)
			if n < 0 {
				return errors.New("protowire: invalid field in LedgerLogPayload")
			}

			dataBytes = dataBytes[n:]
		}
	}

	if txBytes == nil {
		return nil
	}

	// --- Transaction level: extract id (field 5) and postings (field 1, repeated) ---
	out.TxID, out.Postings, err = parseTransaction(txBytes, out.Postings)
	if err != nil {
		return fmt.Errorf("transaction: %w", err)
	}

	return nil
}

// parseTransaction extracts id and postings from Transaction bytes.
// The postings slice is passed in for reuse (truncated to len=0 by caller).
func parseTransaction(data []byte, postings []rawPosting) (txID uint64, result []rawPosting, err error) {
	result = postings

	for len(data) > 0 {
		num, typ, n := protowire.ConsumeTag(data)
		if n < 0 {
			return 0, result, errors.New("protowire: invalid tag in Transaction")
		}

		data = data[n:]

		switch {
		case num == 1 && typ == protowire.BytesType:
			// Posting (repeated)
			b, bn := protowire.ConsumeBytes(data)
			if bn < 0 {
				return 0, result, errors.New("protowire: invalid bytes for Posting")
			}

			src, dst, perr := parsePosting(b)
			if perr != nil {
				return 0, result, perr
			}

			result = append(result, rawPosting{Source: src, Destination: dst})
			data = data[bn:]
		case num == 5 && typ == protowire.Fixed64Type:
			v, vn := protowire.ConsumeFixed64(data)
			if vn < 0 {
				return 0, result, errors.New("protowire: invalid fixed64 for Transaction.id")
			}

			txID = v
			data = data[vn:]
		default:
			n := protowire.ConsumeFieldValue(num, typ, data)
			if n < 0 {
				return 0, result, errors.New("protowire: invalid field in Transaction")
			}

			data = data[n:]
		}
	}

	return txID, result, nil
}

// parsePosting extracts source and destination from Posting bytes.
func parsePosting(data []byte) (source, destination string, err error) {
	for len(data) > 0 {
		num, typ, n := protowire.ConsumeTag(data)
		if n < 0 {
			return "", "", errors.New("protowire: invalid tag in Posting")
		}

		data = data[n:]

		switch {
		case num == 1 && typ == protowire.BytesType:
			b, bn := protowire.ConsumeBytes(data)
			if bn < 0 {
				return "", "", errors.New("protowire: invalid bytes for Posting.source")
			}

			source = string(b)
			data = data[bn:]
		case num == 2 && typ == protowire.BytesType:
			b, bn := protowire.ConsumeBytes(data)
			if bn < 0 {
				return "", "", errors.New("protowire: invalid bytes for Posting.destination")
			}

			destination = string(b)
			data = data[bn:]
		default:
			n := protowire.ConsumeFieldValue(num, typ, data)
			if n < 0 {
				return "", "", errors.New("protowire: invalid field in Posting")
			}

			data = data[n:]
		}
	}

	return source, destination, nil
}

// scanBytesField scans protobuf fields looking for a length-delimited field
// with the given field number. Returns the raw bytes or nil if not found.
func scanBytesField(data []byte, targetField protowire.Number) ([]byte, error) {
	for len(data) > 0 {
		num, typ, n := protowire.ConsumeTag(data)
		if n < 0 {
			return nil, fmt.Errorf("protowire: invalid tag scanning for field %d", targetField)
		}

		data = data[n:]

		if num == targetField && typ == protowire.BytesType {
			b, bn := protowire.ConsumeBytes(data)
			if bn < 0 {
				return nil, fmt.Errorf("protowire: invalid bytes for field %d", targetField)
			}

			return b, nil
		}

		n = protowire.ConsumeFieldValue(num, typ, data)
		if n < 0 {
			return nil, fmt.Errorf("protowire: invalid field value scanning for field %d", targetField)
		}

		data = data[n:]
	}

	return nil, nil
}
