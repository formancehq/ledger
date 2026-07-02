package indexbuilder

import (
	"errors"
	"fmt"

	"google.golang.org/protobuf/encoding/protowire"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// rawPosting holds the source, destination and asset extracted from a
// Posting message. The asset is kept so per-asset exclusion lookups against
// purged/transient volume sets stay precise inside multi-asset accounts.
type rawPosting struct {
	Source      string
	Destination string
	Asset       string
}

// parsedLog holds the fields extracted by the protowire fast path.
type parsedLog struct {
	Sequence         uint64
	Ledger           string
	TxID             uint64
	Postings         []rawPosting              // reused across iterations via truncate-to-zero
	LogType          int32                     // LedgerLogPayload oneof tag: 1=created, 2=reverted, 0=skip
	PurgedVolumes    []*commonpb.TouchedVolume // LedgerLog.purged_volumes    (field 4) — draining evictions, reused
	EphemeralVolumes []*commonpb.TouchedVolume // LedgerLog.ephemeral_volumes (field 6) — pure ephemeral evictions, reused
	// DeletedLedger is the name carried by a DeleteLedger log (LogPayload
	// field 2); empty for every other log. It lets the backfill replay wipe
	// the deleted ledger's readstore rows, mirroring the live processLogs path.
	DeletedLedger string
}

// GetPurgedVolumes / GetEphemeralVolumes satisfy ledgerLogWithPurgedVolumes
// so extractPurgedVolumes can consume the protowire fast path without going
// through commonpb.
func (p *parsedLog) GetPurgedVolumes() []*commonpb.TouchedVolume    { return p.PurgedVolumes }
func (p *parsedLog) GetEphemeralVolumes() []*commonpb.TouchedVolume { return p.EphemeralVolumes }

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
//	      → LedgerLog[1:data, 4:purged_volumes(repeated)]
//	        → LedgerLogPayload[oneof: 1=created_tx, 2=reverted_tx]
//	          → CreatedTransaction[1:transaction] / RevertedTransaction[2:revert_transaction]
//	            → Transaction[1:postings(repeated), 5:id]
//	              → Posting[1:source, 2:destination, 3:amount(skipped), 4:asset]
func parsePostingsFromLog(data []byte, out *parsedLog) error {
	out.LogType = 0
	out.Postings = out.Postings[:0]
	out.Sequence = 0
	out.Ledger = ""
	out.TxID = 0
	out.PurgedVolumes = out.PurgedVolumes[:0]
	out.EphemeralVolumes = out.EphemeralVolumes[:0]
	out.DeletedLedger = ""

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

	// --- LogPayload level: a DeleteLedger payload (field 2) carries no
	// postings. Record the deleted ledger name so the backfill can wipe its
	// readstore rows (mirroring the live processLogs path), then stop. The
	// oneof guarantees field 2 and field 3 (apply) are mutually exclusive. ---
	deleteBytes, err := scanBytesField(payloadBytes, 2)
	if err != nil {
		return fmt.Errorf("LogPayload delete_ledger: %w", err)
	}

	if deleteBytes != nil {
		// DeletedLedgerLog.name is field 1.
		name, nerr := scanBytesField(deleteBytes, 1)
		if nerr != nil {
			return fmt.Errorf("DeletedLedgerLog: %w", nerr)
		}

		out.DeletedLedger = string(name)

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

	// --- LedgerLog level: extract data (field 1) and purged_volumes (field 4, repeated TouchedVolume) ---
	var dataBytes []byte

	for len(ledgerLogBytes) > 0 {
		num, typ, n := protowire.ConsumeTag(ledgerLogBytes)
		if n < 0 {
			return errors.New("protowire: invalid tag in LedgerLog")
		}

		ledgerLogBytes = ledgerLogBytes[n:]

		switch {
		case num == 1 && typ == protowire.BytesType:
			b, bn := protowire.ConsumeBytes(ledgerLogBytes)
			if bn < 0 {
				return errors.New("protowire: invalid bytes for LedgerLog.data")
			}

			dataBytes = b
			ledgerLogBytes = ledgerLogBytes[bn:]
		case num == 4 && typ == protowire.BytesType:
			b, bn := protowire.ConsumeBytes(ledgerLogBytes)
			if bn < 0 {
				return errors.New("protowire: invalid bytes for LedgerLog.purged_volumes")
			}

			vol, perr := parseTouchedVolume(b)
			if perr != nil {
				return fmt.Errorf("TouchedVolume: %w", perr)
			}
			out.PurgedVolumes = append(out.PurgedVolumes, vol)
			ledgerLogBytes = ledgerLogBytes[bn:]
		case num == 6 && typ == protowire.BytesType:
			b, bn := protowire.ConsumeBytes(ledgerLogBytes)
			if bn < 0 {
				return errors.New("protowire: invalid bytes for LedgerLog.ephemeral_volumes")
			}

			vol, perr := parseTouchedVolume(b)
			if perr != nil {
				return fmt.Errorf("TouchedVolume: %w", perr)
			}
			out.EphemeralVolumes = append(out.EphemeralVolumes, vol)
			ledgerLogBytes = ledgerLogBytes[bn:]
		default:
			n := protowire.ConsumeFieldValue(num, typ, ledgerLogBytes)
			if n < 0 {
				return errors.New("protowire: invalid field in LedgerLog")
			}

			ledgerLogBytes = ledgerLogBytes[n:]
		}
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

			src, dst, asset, perr := parsePosting(b)
			if perr != nil {
				return 0, result, perr
			}

			result = append(result, rawPosting{Source: src, Destination: dst, Asset: asset})
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

// parseTouchedVolume extracts account (field 1) and asset (field 2) from a
// commonpb.TouchedVolume sub-message embedded in LedgerLog.purged_volumes.
func parseTouchedVolume(data []byte) (*commonpb.TouchedVolume, error) {
	out := &commonpb.TouchedVolume{}
	for len(data) > 0 {
		num, typ, n := protowire.ConsumeTag(data)
		if n < 0 {
			return nil, errors.New("protowire: invalid tag in TouchedVolume")
		}

		data = data[n:]

		switch {
		case num == 1 && typ == protowire.BytesType:
			b, bn := protowire.ConsumeBytes(data)
			if bn < 0 {
				return nil, errors.New("protowire: invalid bytes for TouchedVolume.account")
			}

			out.Account = string(b)
			data = data[bn:]
		case num == 2 && typ == protowire.BytesType:
			b, bn := protowire.ConsumeBytes(data)
			if bn < 0 {
				return nil, errors.New("protowire: invalid bytes for TouchedVolume.asset")
			}

			out.Asset = string(b)
			data = data[bn:]
		default:
			n := protowire.ConsumeFieldValue(num, typ, data)
			if n < 0 {
				return nil, errors.New("protowire: invalid field in TouchedVolume")
			}

			data = data[n:]
		}
	}

	return out, nil
}

// parsePosting extracts source, destination and asset from Posting bytes.
func parsePosting(data []byte) (source, destination, asset string, err error) {
	for len(data) > 0 {
		num, typ, n := protowire.ConsumeTag(data)
		if n < 0 {
			return "", "", "", errors.New("protowire: invalid tag in Posting")
		}

		data = data[n:]

		switch {
		case num == 1 && typ == protowire.BytesType:
			b, bn := protowire.ConsumeBytes(data)
			if bn < 0 {
				return "", "", "", errors.New("protowire: invalid bytes for Posting.source")
			}

			source = string(b)
			data = data[bn:]
		case num == 2 && typ == protowire.BytesType:
			b, bn := protowire.ConsumeBytes(data)
			if bn < 0 {
				return "", "", "", errors.New("protowire: invalid bytes for Posting.destination")
			}

			destination = string(b)
			data = data[bn:]
		case num == 4 && typ == protowire.BytesType:
			b, bn := protowire.ConsumeBytes(data)
			if bn < 0 {
				return "", "", "", errors.New("protowire: invalid bytes for Posting.asset")
			}

			asset = string(b)
			data = data[bn:]
		default:
			n := protowire.ConsumeFieldValue(num, typ, data)
			if n < 0 {
				return "", "", "", errors.New("protowire: invalid field in Posting")
			}

			data = data[n:]
		}
	}

	return source, destination, asset, nil
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
