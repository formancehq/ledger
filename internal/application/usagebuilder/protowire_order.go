package usagebuilder

import (
	"errors"
	"fmt"

	"google.golang.org/protobuf/encoding/protowire"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// usageCreateOrder is the small business-intent projection consumed by the
// usage builder for a native CreateTransaction order. Keeping this view local
// avoids materializing postings, metadata, script variables and technical
// fields that the usage projection never reads.
type usageCreateOrder struct {
	ledger       string
	hasReference bool
	isScripted   bool
	template     string
	usesTemplate bool
	timestamp    *commonpb.Timestamp
}

// parseUsageCreateOrder recognizes the common native CreateTransaction wire
// shape and extracts only the fields used by the usage projection. matched is
// false for every other Order variant, which keeps the generated Protobuf
// decoder as the authoritative fallback for rarer order kinds.
func parseUsageCreateOrder(data []byte) (usageCreateOrder, bool, error) {
	ledgerScoped, matched, err := usageOrderLedgerScoped(data)
	if err != nil || !matched {
		return usageCreateOrder{}, false, err
	}

	ledger, apply, matched, err := usageLedgerApply(ledgerScoped)
	if err != nil || !matched {
		return usageCreateOrder{}, false, err
	}

	createPayload, matched, err := usageLedgerCreateTransaction(apply)
	if err != nil || !matched {
		return usageCreateOrder{}, false, err
	}
	create, matched, err := usageCreateTransaction(createPayload)
	if err != nil || !matched {
		return usageCreateOrder{}, false, err
	}

	create.ledger = ledger

	return create, true, nil
}

// usageOrderLedgerScoped returns the final Order oneof payload. Protobuf oneof
// semantics are "last member wins", so a trailing system-scoped payload must
// disable the fast path even if a ledger-scoped payload appeared earlier.
func usageOrderLedgerScoped(data []byte) ([]byte, bool, error) {
	var (
		ledgerScoped []byte
		activeType   protowire.Number
	)

	for len(data) > 0 {
		num, typ, n := protowire.ConsumeTag(data)
		if n < 0 {
			return nil, false, errors.New("protowire: invalid tag in Order")
		}
		data = data[n:]

		if num == 3 {
			if typ != protowire.BytesType {
				return nil, false, fmt.Errorf("protowire: invalid wire type %d for Order.technical", typ)
			}
			size := protowire.ConsumeFieldValue(num, typ, data)
			if size < 0 {
				return nil, false, errors.New("protowire: invalid Order.technical")
			}
			data = data[size:]

			continue
		}

		if num == 1 || num == 2 {
			if typ != protowire.BytesType {
				return nil, false, fmt.Errorf("protowire: invalid wire type %d for Order field %d", typ, num)
			}
			value, size := protowire.ConsumeBytes(data)
			if size < 0 {
				return nil, false, fmt.Errorf("protowire: invalid bytes for Order field %d", num)
			}
			activeType = num
			if num == 1 {
				ledgerScoped = value
			}
			data = data[size:]

			continue
		}

		size := protowire.ConsumeFieldValue(num, typ, data)
		if size < 0 {
			return nil, false, errors.New("protowire: invalid field in Order")
		}
		data = data[size:]
	}

	return ledgerScoped, activeType == 1, nil
}

func usageLedgerApply(data []byte) (string, []byte, bool, error) {
	var (
		ledger        string
		apply         []byte
		activePayload protowire.Number
	)

	for len(data) > 0 {
		num, typ, n := protowire.ConsumeTag(data)
		if n < 0 {
			return "", nil, false, errors.New("protowire: invalid tag in LedgerScopedOrder")
		}
		data = data[n:]

		if num == 1 {
			if typ != protowire.BytesType {
				return "", nil, false, fmt.Errorf("protowire: invalid wire type %d for LedgerScopedOrder.ledger", typ)
			}
			value, size := protowire.ConsumeBytes(data)
			if size < 0 {
				return "", nil, false, errors.New("protowire: invalid LedgerScopedOrder.ledger")
			}
			ledger = string(value)
			data = data[size:]

			continue
		}

		if usageIsLedgerPayloadField(num) {
			if typ != protowire.BytesType {
				return "", nil, false, fmt.Errorf("protowire: invalid wire type %d for LedgerScopedOrder field %d", typ, num)
			}
			value, size := protowire.ConsumeBytes(data)
			if size < 0 {
				return "", nil, false, fmt.Errorf("protowire: invalid bytes for LedgerScopedOrder field %d", num)
			}
			activePayload = num
			if num == 2 {
				apply = value
			}
			data = data[size:]

			continue
		}

		size := protowire.ConsumeFieldValue(num, typ, data)
		if size < 0 {
			return "", nil, false, errors.New("protowire: invalid field in LedgerScopedOrder")
		}
		data = data[size:]
	}

	return ledger, apply, activePayload == 2, nil
}

func usageIsLedgerPayloadField(num protowire.Number) bool {
	switch num {
	case 2, 3, 4, 5, 6, 7, 8, 9, 11, 12, 13:
		return true
	default:
		return false
	}
}

func usageLedgerCreateTransaction(data []byte) ([]byte, bool, error) {
	var (
		create     []byte
		activeData protowire.Number
	)

	for len(data) > 0 {
		num, typ, n := protowire.ConsumeTag(data)
		if n < 0 {
			return nil, false, errors.New("protowire: invalid tag in LedgerApplyOrder")
		}
		data = data[n:]

		if num >= 1 && num <= 11 {
			if typ != protowire.BytesType {
				return nil, false, fmt.Errorf("protowire: invalid wire type %d for LedgerApplyOrder field %d", typ, num)
			}
			value, size := protowire.ConsumeBytes(data)
			if size < 0 {
				return nil, false, fmt.Errorf("protowire: invalid bytes for LedgerApplyOrder field %d", num)
			}
			activeData = num
			if num == 1 {
				create = value
			}
			data = data[size:]

			continue
		}

		size := protowire.ConsumeFieldValue(num, typ, data)
		if size < 0 {
			return nil, false, errors.New("protowire: invalid field in LedgerApplyOrder")
		}
		data = data[size:]
	}

	return create, activeData == 1, nil
}

func usageCreateTransaction(data []byte) (usageCreateOrder, bool, error) {
	var (
		out                usageCreateOrder
		seenScript         bool
		seenTimestamp      bool
		seenNumscript      bool
		requiresProtoMerge bool
	)

	for len(data) > 0 {
		num, typ, n := protowire.ConsumeTag(data)
		if n < 0 {
			return usageCreateOrder{}, false, errors.New("protowire: invalid tag in CreateTransactionOrder")
		}
		data = data[n:]

		switch num {
		case 2, 3, 4, 8:
			if typ != protowire.BytesType {
				return usageCreateOrder{}, false, fmt.Errorf("protowire: invalid wire type %d for CreateTransactionOrder field %d", typ, num)
			}
			value, size := protowire.ConsumeBytes(data)
			if size < 0 {
				return usageCreateOrder{}, false, fmt.Errorf("protowire: invalid bytes for CreateTransactionOrder field %d", num)
			}

			switch num {
			case 2:
				requiresProtoMerge = requiresProtoMerge || seenScript
				seenScript = true
				plain, err := usageScriptPlain(value)
				if err != nil {
					return usageCreateOrder{}, false, err
				}
				out.isScripted = out.isScripted || plain != ""
			case 3:
				requiresProtoMerge = requiresProtoMerge || seenTimestamp
				seenTimestamp = true
				value, err := parseUsageTimestamp(value)
				if err != nil {
					return usageCreateOrder{}, false, err
				}
				out.timestamp = &commonpb.Timestamp{Data: value}
			case 4:
				out.hasReference = len(value) > 0
			case 8:
				requiresProtoMerge = requiresProtoMerge || seenNumscript
				seenNumscript = true
				name, err := usageNumscriptName(value)
				if err != nil {
					return usageCreateOrder{}, false, err
				}
				out.isScripted = true
				out.usesTemplate = true
				out.template = name
			}

			data = data[size:]
		case 1, 5, 6:
			if typ != protowire.BytesType {
				return usageCreateOrder{}, false, fmt.Errorf("protowire: invalid wire type %d for CreateTransactionOrder field %d", typ, num)
			}
			size := protowire.ConsumeFieldValue(num, typ, data)
			if size < 0 {
				return usageCreateOrder{}, false, fmt.Errorf("protowire: invalid CreateTransactionOrder field %d", num)
			}
			data = data[size:]
		case 7:
			if typ != protowire.VarintType {
				return usageCreateOrder{}, false, fmt.Errorf("protowire: invalid wire type %d for CreateTransactionOrder.force", typ)
			}
			size := protowire.ConsumeFieldValue(num, typ, data)
			if size < 0 {
				return usageCreateOrder{}, false, errors.New("protowire: invalid CreateTransactionOrder.force")
			}
			data = data[size:]
		default:
			size := protowire.ConsumeFieldValue(num, typ, data)
			if size < 0 {
				return usageCreateOrder{}, false, errors.New("protowire: invalid field in CreateTransactionOrder")
			}
			data = data[size:]
		}
	}

	// Singular message fields merge when they appear more than once. The
	// generated decoder remains the source of truth for that non-canonical
	// shape rather than duplicating its merge semantics here.
	return out, !requiresProtoMerge, nil
}

func usageScriptPlain(data []byte) (string, error) {
	var plain string
	for len(data) > 0 {
		num, typ, n := protowire.ConsumeTag(data)
		if n < 0 {
			return "", errors.New("protowire: invalid tag in Script")
		}
		data = data[n:]

		if num == 1 {
			if typ != protowire.BytesType {
				return "", fmt.Errorf("protowire: invalid wire type %d for Script.plain", typ)
			}
			value, size := protowire.ConsumeBytes(data)
			if size < 0 {
				return "", errors.New("protowire: invalid Script.plain")
			}
			plain = string(value)
			data = data[size:]

			continue
		}
		if num == 2 || num == 3 {
			if typ != protowire.BytesType {
				return "", fmt.Errorf("protowire: invalid wire type %d for Script field %d", typ, num)
			}
			size := protowire.ConsumeFieldValue(num, typ, data)
			if size < 0 {
				return "", fmt.Errorf("protowire: invalid Script field %d", num)
			}
			data = data[size:]

			continue
		}

		size := protowire.ConsumeFieldValue(num, typ, data)
		if size < 0 {
			return "", errors.New("protowire: invalid field in Script")
		}
		data = data[size:]
	}

	return plain, nil
}

func usageNumscriptName(data []byte) (string, error) {
	var name string
	for len(data) > 0 {
		num, typ, n := protowire.ConsumeTag(data)
		if n < 0 {
			return "", errors.New("protowire: invalid tag in NumscriptReference")
		}
		data = data[n:]

		if num == 1 {
			if typ != protowire.BytesType {
				return "", fmt.Errorf("protowire: invalid wire type %d for NumscriptReference.name", typ)
			}
			value, size := protowire.ConsumeBytes(data)
			if size < 0 {
				return "", errors.New("protowire: invalid NumscriptReference.name")
			}
			name = string(value)
			data = data[size:]

			continue
		}
		if num == 2 || num == 3 {
			if typ != protowire.BytesType {
				return "", fmt.Errorf("protowire: invalid wire type %d for NumscriptReference field %d", typ, num)
			}
			size := protowire.ConsumeFieldValue(num, typ, data)
			if size < 0 {
				return "", fmt.Errorf("protowire: invalid NumscriptReference field %d", num)
			}
			data = data[size:]

			continue
		}

		size := protowire.ConsumeFieldValue(num, typ, data)
		if size < 0 {
			return "", errors.New("protowire: invalid field in NumscriptReference")
		}
		data = data[size:]
	}

	return name, nil
}
