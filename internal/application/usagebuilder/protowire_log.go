package usagebuilder

import (
	"errors"
	"fmt"

	"google.golang.org/protobuf/encoding/protowire"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// parseUsageLog extracts only the transaction kind, posting count, timestamp,
// and volume annotations consumed by the usage projection. In particular it
// skips the post-commit volume snapshot, posting contents, and metadata.
func parseUsageLog(data []byte, out *logVolumeAnnotations) error {
	out.postings = 0
	clear(out.purged)
	out.purged = out.purged[:0]
	clear(out.newKept)
	out.newKept = out.newKept[:0]
	clear(out.ephemeral)
	out.ephemeral = out.ephemeral[:0]
	out.txTimestamp = nil
	out.isCreatedTx = false
	out.isRevertedTx = false

	payload, err := usageScanBytesField(data, 2)
	if err != nil {
		return fmt.Errorf("Log: %w", err)
	}
	if payload == nil {
		return nil
	}

	apply, err := usageScanOneofBytesField(payload, 3, usageIsLogPayloadField, "LogPayload")
	if err != nil {
		return fmt.Errorf("LogPayload: %w", err)
	}
	if apply == nil {
		return nil
	}

	ledgerLog, err := usageScanBytesField(apply, 2)
	if err != nil {
		return fmt.Errorf("ApplyLedgerLog: %w", err)
	}
	if ledgerLog == nil {
		return nil
	}

	var (
		transactionPayload []byte
		seenData           bool
	)
	for len(ledgerLog) > 0 {
		num, typ, n := protowire.ConsumeTag(ledgerLog)
		if n < 0 {
			return errors.New("protowire: invalid tag in LedgerLog")
		}
		ledgerLog = ledgerLog[n:]

		switch num {
		case 1:
			if typ != protowire.BytesType {
				return fmt.Errorf("protowire: invalid wire type %d for LedgerLog.data", typ)
			}
			if seenData {
				return errors.New("protowire: non-canonical duplicate LedgerLog.data")
			}
			value, size := protowire.ConsumeBytes(ledgerLog)
			if size < 0 {
				return errors.New("protowire: invalid LedgerLog.data")
			}
			seenData = true
			transactionPayload = value
			ledgerLog = ledgerLog[size:]
		case 4, 5, 6:
			if typ != protowire.BytesType {
				return fmt.Errorf("protowire: invalid wire type %d for LedgerLog volume annotation %d", typ, num)
			}
			value, size := protowire.ConsumeBytes(ledgerLog)
			if size < 0 {
				return errors.New("protowire: invalid LedgerLog volume annotation")
			}
			volume, parseErr := parseUsageTouchedVolume(value)
			if parseErr != nil {
				return parseErr
			}
			switch num {
			case 4:
				out.purged = append(out.purged, volume)
			case 5:
				out.newKept = append(out.newKept, volume)
			case 6:
				out.ephemeral = append(out.ephemeral, volume)
			}
			ledgerLog = ledgerLog[size:]
		default:
			size := protowire.ConsumeFieldValue(num, typ, ledgerLog)
			if size < 0 {
				return errors.New("protowire: invalid field in LedgerLog")
			}
			ledgerLog = ledgerLog[size:]
		}
	}

	if transactionPayload == nil {
		return nil
	}

	var (
		transaction []byte
		seenPayload bool
	)
	for len(transactionPayload) > 0 {
		num, typ, n := protowire.ConsumeTag(transactionPayload)
		if n < 0 {
			return errors.New("protowire: invalid tag in LedgerLogPayload")
		}
		transactionPayload = transactionPayload[n:]

		if usageIsLedgerLogPayloadField(num) {
			if typ != protowire.BytesType {
				return fmt.Errorf("protowire: invalid wire type %d for LedgerLogPayload field %d", typ, num)
			}
			if seenPayload {
				return errors.New("protowire: non-canonical multiple LedgerLogPayload oneof members")
			}
			seenPayload = true

			value, size := protowire.ConsumeBytes(transactionPayload)
			if size < 0 {
				return fmt.Errorf("protowire: invalid LedgerLogPayload field %d", num)
			}

			switch num {
			case 1:
				out.isCreatedTx = true
				transaction, err = usageScanBytesField(value, 1)
				if err != nil {
					return fmt.Errorf("CreatedTransaction: %w", err)
				}
			case 2:
				out.isRevertedTx = true
				transaction, err = usageScanBytesField(value, 2)
				if err != nil {
					return fmt.Errorf("RevertedTransaction: %w", err)
				}
			}

			transactionPayload = transactionPayload[size:]
		} else {
			size := protowire.ConsumeFieldValue(num, typ, transactionPayload)
			if size < 0 {
				return errors.New("protowire: invalid field in LedgerLogPayload")
			}
			transactionPayload = transactionPayload[size:]
		}
	}

	if transaction == nil {
		return nil
	}

	for len(transaction) > 0 {
		num, typ, n := protowire.ConsumeTag(transaction)
		if n < 0 {
			return errors.New("protowire: invalid tag in Transaction")
		}
		transaction = transaction[n:]

		switch num {
		case 1:
			if typ != protowire.BytesType {
				return fmt.Errorf("protowire: invalid wire type %d for Transaction.postings", typ)
			}
			_, size := protowire.ConsumeBytes(transaction)
			if size < 0 {
				return errors.New("protowire: invalid Transaction.postings")
			}
			out.postings++
			transaction = transaction[size:]
		case 3:
			if typ != protowire.BytesType {
				return fmt.Errorf("protowire: invalid wire type %d for Transaction.timestamp", typ)
			}
			timestamp, size := protowire.ConsumeBytes(transaction)
			if size < 0 {
				return errors.New("protowire: invalid Transaction.timestamp")
			}
			data, timestampErr := parseUsageTimestamp(timestamp)
			if timestampErr != nil {
				return timestampErr
			}
			out.txTimestamp = &commonpb.Timestamp{Data: data}
			transaction = transaction[size:]
		default:
			size := protowire.ConsumeFieldValue(num, typ, transaction)
			if size < 0 {
				return errors.New("protowire: invalid field in Transaction")
			}
			transaction = transaction[size:]
		}
	}

	return nil
}

func parseUsageTouchedVolume(data []byte) (*commonpb.TouchedVolume, error) {
	out := &commonpb.TouchedVolume{}
	for len(data) > 0 {
		num, typ, n := protowire.ConsumeTag(data)
		if n < 0 {
			return nil, errors.New("protowire: invalid tag in TouchedVolume")
		}
		data = data[n:]

		switch {
		case num >= 1 && num <= 3:
			if typ != protowire.BytesType {
				return nil, fmt.Errorf("protowire: invalid wire type %d for TouchedVolume field %d", typ, num)
			}
			value, size := protowire.ConsumeBytes(data)
			if size < 0 {
				return nil, errors.New("protowire: invalid TouchedVolume string")
			}
			switch num {
			case 1:
				out.Account = string(value)
			case 2:
				out.Asset = string(value)
			case 3:
				out.Color = string(value)
			}
			data = data[size:]
		default:
			size := protowire.ConsumeFieldValue(num, typ, data)
			if size < 0 {
				return nil, errors.New("protowire: invalid field in TouchedVolume")
			}
			data = data[size:]
		}
	}

	return out, nil
}

func parseUsageTimestamp(data []byte) (uint64, error) {
	var value uint64
	for len(data) > 0 {
		num, typ, n := protowire.ConsumeTag(data)
		if n < 0 {
			return 0, errors.New("protowire: invalid tag in Timestamp")
		}
		data = data[n:]

		if num == 1 {
			if typ != protowire.Fixed64Type {
				return 0, fmt.Errorf("protowire: invalid wire type %d for Timestamp.data", typ)
			}
			parsed, size := protowire.ConsumeFixed64(data)
			if size < 0 {
				return 0, errors.New("protowire: invalid Timestamp.data")
			}
			value = parsed
			data = data[size:]

			continue
		}

		size := protowire.ConsumeFieldValue(num, typ, data)
		if size < 0 {
			return 0, errors.New("protowire: invalid field in Timestamp")
		}
		data = data[size:]
	}

	return value, nil
}

func usageScanBytesField(data []byte, target protowire.Number) ([]byte, error) {
	var (
		selected []byte
		seen     bool
	)

	for len(data) > 0 {
		num, typ, n := protowire.ConsumeTag(data)
		if n < 0 {
			return nil, fmt.Errorf("protowire: invalid tag scanning field %d", target)
		}
		data = data[n:]

		if num == target {
			if typ != protowire.BytesType {
				return nil, fmt.Errorf("protowire: invalid wire type %d for field %d", typ, target)
			}
			if seen {
				return nil, fmt.Errorf("protowire: non-canonical duplicate field %d", target)
			}
			value, size := protowire.ConsumeBytes(data)
			if size < 0 {
				return nil, fmt.Errorf("protowire: invalid bytes for field %d", target)
			}
			seen = true
			selected = value
			data = data[size:]

			continue
		}

		size := protowire.ConsumeFieldValue(num, typ, data)
		if size < 0 {
			return nil, fmt.Errorf("protowire: invalid value scanning field %d", target)
		}
		data = data[size:]
	}

	return selected, nil
}

func usageScanOneofBytesField(
	data []byte,
	target protowire.Number,
	isMember func(protowire.Number) bool,
	message string,
) ([]byte, error) {
	var selected []byte
	seen := false

	for len(data) > 0 {
		num, typ, n := protowire.ConsumeTag(data)
		if n < 0 {
			return nil, fmt.Errorf("protowire: invalid tag in %s", message)
		}
		data = data[n:]

		if isMember(num) {
			if typ != protowire.BytesType {
				return nil, fmt.Errorf("protowire: invalid wire type %d for %s field %d", typ, message, num)
			}
			if seen {
				return nil, fmt.Errorf("protowire: non-canonical multiple %s oneof members", message)
			}

			value, size := protowire.ConsumeBytes(data)
			if size < 0 {
				return nil, fmt.Errorf("protowire: invalid bytes for %s field %d", message, num)
			}
			seen = true
			if num == target {
				selected = value
			}
			data = data[size:]

			continue
		}

		size := protowire.ConsumeFieldValue(num, typ, data)
		if size < 0 {
			return nil, fmt.Errorf("protowire: invalid field in %s", message)
		}
		data = data[size:]
	}

	return selected, nil
}

func usageIsLogPayloadField(num protowire.Number) bool {
	switch num {
	case 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15,
		17, 18, 19, 20, 21, 23, 24, 25, 26, 27, 28:
		return true
	default:
		return false
	}
}

func usageIsLedgerLogPayloadField(num protowire.Number) bool {
	switch num {
	case 1, 2, 3, 4, 5, 6, 9, 10, 11, 13, 14, 15, 16:
		return true
	default:
		return false
	}
}
