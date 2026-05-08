package store

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/cockroachdb/pebble/v2"
	"github.com/spf13/cobra"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger-v3-poc/internal/proto/auditpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
)

// NewDumpCommand creates the store dump command.
func NewDumpCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "dump <data-dir>",
		Short: "Dump the entire contents of a Pebble store (offline)",
		Long: `Open a Pebble data directory in read-only mode and print every key-value pair.
Values are decoded based on key prefix where possible (logs, attributes, config, etc.).
This is an offline operation — the server must not be running.`,
		Args: cobra.ExactArgs(1),
		RunE: runDump,
	}

	cmd.Flags().String("prefix", "", "Only dump keys starting with this hex prefix (e.g. '01' for logs, 'f1' for attributes)")
	cmd.Flags().Int("limit", 0, "Maximum number of entries to print (0 = unlimited)")
	cmd.Flags().Bool("raw", false, "Print raw hex values instead of decoded output")

	return cmd
}

func runDump(cmd *cobra.Command, args []string) error {
	dataDir := args[0]
	prefixHex, _ := cmd.Flags().GetString("prefix")
	limit, _ := cmd.Flags().GetInt("limit")
	raw, _ := cmd.Flags().GetBool("raw")

	db, err := pebble.Open(dataDir, &pebble.Options{
		Logger:   dal.DiscardPebbleLogger(),
		ReadOnly: true,
	})
	if err != nil {
		return fmt.Errorf("opening pebble at %s: %w", dataDir, err)
	}

	defer func() { _ = db.Close() }()

	var iterOpts pebble.IterOptions
	if prefixHex != "" {
		prefixBytes, decErr := hex.DecodeString(prefixHex)
		if decErr != nil {
			return fmt.Errorf("invalid hex prefix %q: %w", prefixHex, decErr)
		}

		iterOpts.LowerBound = prefixBytes

		upper := make([]byte, len(prefixBytes))
		copy(upper, prefixBytes)
		upper[len(upper)-1]++
		iterOpts.UpperBound = upper
	}

	iter, err := db.NewIter(&iterOpts)
	if err != nil {
		return fmt.Errorf("creating iterator: %w", err)
	}

	defer func() { _ = iter.Close() }()

	count := 0

	for iter.First(); iter.Valid(); iter.Next() {
		if limit > 0 && count >= limit {
			break
		}

		key := iter.Key()
		val, valErr := iter.ValueAndErr()
		if valErr != nil {
			fmt.Printf("[%04d] key=%s  ERROR: %v\n", count, hex.EncodeToString(key), valErr)
			count++

			continue
		}

		if raw {
			fmt.Printf("[%04d] key=%s  val=%s\n", count, hex.EncodeToString(key), hex.EncodeToString(val))
		} else {
			prefix := describeKey(key)
			decoded := decodeValue(key, val)
			fmt.Printf("[%04d] %s\n       key=%s\n       %s\n\n", count, prefix, hex.EncodeToString(key), decoded)
		}

		count++
	}

	fmt.Printf("--- %d entries ---\n", count)

	return nil
}

// describeKey returns a human-readable label for a key based on its prefix byte.
func describeKey(key []byte) string {
	if len(key) == 0 {
		return "(empty key)"
	}

	switch key[0] {
	case dal.KeyPrefixIdempotency:
		return "IDEMPOTENCY key_hash=" + hex.EncodeToString(key[1:])
	case dal.KeyPrefixIdempotencyTimeIdx:
		if len(key) >= 9 {
			ts := binary.BigEndian.Uint64(key[1:9])

			return fmt.Sprintf("IDEMPOTENCY_TIME_IDX created_at=%d hash=%s", ts, hex.EncodeToString(key[9:]))
		}

		return "IDEMPOTENCY_TIME_IDX (short key)"
	case dal.KeyPrefixLog:
		if len(key) >= 9 {
			seq := binary.BigEndian.Uint64(key[1:9])

			return fmt.Sprintf("LOG seq=%d", seq)
		}

		return "LOG (short key)"
	case dal.KeyPrefixAudit:
		if len(key) >= 9 {
			seq := binary.BigEndian.Uint64(key[1:9])

			return fmt.Sprintf("AUDIT seq=%d", seq)
		}

		return "AUDIT (short key)"
	case dal.KeyPrefixQueryCheckpoint:
		if len(key) >= 9 {
			id := binary.BigEndian.Uint64(key[1:9])

			return fmt.Sprintf("QUERY_CHECKPOINT id=%d", id)
		}

		return "QUERY_CHECKPOINT (short key)"
	case dal.KeyPrefixPreparedQuery:
		return "PREPARED_QUERY rest=" + safeString(key[1:])
	case dal.KeyPrefixAttributes:
		return describeAttributeKey(key)
	case dal.KeyPrefixLastAppliedIndex:
		return "LAST_APPLIED_INDEX"
	case dal.KeyPrefixLastAppliedTimestamp:
		return "LAST_APPLIED_TIMESTAMP"
	case dal.KeyPrefixLedgerInfo:
		return "LEDGER_INFO name=" + safeString(key[1:])
	case dal.KeyPrefixSigningKey:
		return "SIGNING_KEY id=" + safeString(key[1:])
	case dal.KeyPrefixPeriods:
		if len(key) >= 9 {
			id := binary.BigEndian.Uint64(key[1:9])

			return fmt.Sprintf("PERIOD id=%d", id)
		}

		return "PERIOD (short key)"
	case dal.KeyPrefixNextPeriodID:
		return "NEXT_PERIOD_ID"
	case dal.KeyPrefixNextQueryCheckpointID:
		return "NEXT_QUERY_CHECKPOINT_ID"
	case dal.KeyPrefixSigningConfig:
		return "SIGNING_CONFIG"
	case dal.KeyPrefixSinkCursor:
		return "SINK_CURSOR name=" + safeString(key[1:])
	case dal.KeyPrefixEventsConfig:
		return "EVENTS_CONFIG name=" + safeString(key[1:])
	case dal.KeyPrefixSinkStatus:
		return "SINK_STATUS name=" + safeString(key[1:])
	case dal.KeyPrefixMaintenanceMode:
		return "MAINTENANCE_MODE"
	case dal.KeyPrefixPersistedConfig:
		return "PERSISTED_CONFIG"
	case dal.KeyPrefixMirrorCursor:
		return "MIRROR_CURSOR ledger=" + safeString(key[1:])
	case dal.KeyPrefixMirrorStatus:
		return "MIRROR_STATUS ledger=" + safeString(key[1:])
	case dal.KeyPrefixMirrorSourceHead:
		return "MIRROR_SOURCE_HEAD ledger=" + safeString(key[1:])
	case dal.KeyPrefixPeriodSchedule:
		return "PERIOD_SCHEDULE"
	case dal.KeyPrefixCacheSnapshot:
		return "CACHE_SNAPSHOT"
	default:
		return fmt.Sprintf("UNKNOWN(0x%02X)", key[0])
	}
}

// describeAttributeKey returns a human-readable label for an attribute key.
func describeAttributeKey(key []byte) string {
	// Layout: [0xF1][canonicalKey...][attrType(1)]
	if len(key) < 3 { // 1 prefix + at least 1 canonical + 1 type
		return "ATTR (short key)"
	}

	attrTypeByte := key[len(key)-1]
	canonicalHex := hex.EncodeToString(key[1 : len(key)-1])

	var attrType string
	switch attrTypeByte {
	case dal.AttributeCodeVolume:
		attrType = "Volume"
	case dal.AttributeCodeMetadata:
		attrType = "Metadata"
	case dal.AttributeCodeReference:
		attrType = "Reference"
	case dal.AttributeCodeLedger:
		attrType = "Ledger"
	case dal.AttributeCodeBoundary:
		attrType = "Boundary"
	case dal.AttributeCodeTransaction:
		attrType = "Transaction"
	default:
		attrType = fmt.Sprintf("0x%02X", attrTypeByte)
	}

	return fmt.Sprintf("ATTR type=%s canonical=%s", attrType, canonicalHex)
}

// decodeValue attempts to decode a value based on the key prefix.
func decodeValue(key, val []byte) string {
	if len(key) == 0 {
		return hexVal(val)
	}

	switch key[0] {
	case dal.KeyPrefixIdempotency:
		return tryProtoJSON(val, &commonpb.IdempotencyKeyValue{})
	case dal.KeyPrefixIdempotencyTimeIdx:
		return "(empty time index entry)"
	case dal.KeyPrefixLog:
		return tryProtoJSON(val, &commonpb.Log{})
	case dal.KeyPrefixAudit:
		return tryProtoJSON(val, &auditpb.AuditEntry{})
	case dal.KeyPrefixQueryCheckpoint:
		return tryProtoJSON(val, &raftcmdpb.QueryCheckpointState{})
	case dal.KeyPrefixPreparedQuery:
		return tryProtoJSON(val, &commonpb.PreparedQuery{})
	case dal.KeyPrefixLedgerInfo:
		return tryProtoJSON(val, &commonpb.LedgerInfo{})
	case dal.KeyPrefixPeriods:
		return tryProtoJSON(val, &commonpb.Period{})
	case dal.KeyPrefixEventsConfig:
		return tryProtoJSON(val, &commonpb.SinkConfig{})
	case dal.KeyPrefixSinkStatus:
		return tryProtoJSON(val, &commonpb.SinkStatus{})
	case dal.KeyPrefixLastAppliedIndex, dal.KeyPrefixLastAppliedTimestamp,
		dal.KeyPrefixNextPeriodID, dal.KeyPrefixNextQueryCheckpointID:
		if len(val) == 8 {
			return fmt.Sprintf("uint64=%d", binary.BigEndian.Uint64(val))
		}

		return hexVal(val)
	case dal.KeyPrefixSinkCursor:
		if len(val) == 8 {
			return fmt.Sprintf("cursor=%d", binary.BigEndian.Uint64(val))
		}

		return hexVal(val)
	case dal.KeyPrefixMaintenanceMode, dal.KeyPrefixSigningConfig:
		if len(val) == 1 {
			return fmt.Sprintf("enabled=%v", val[0] != 0)
		}

		return hexVal(val)
	case dal.KeyPrefixPeriodSchedule:
		return fmt.Sprintf("cron=%q", string(val))
	case dal.KeyPrefixPersistedConfig:
		return "json=" + string(val)
	case dal.KeyPrefixAttributes:
		return decodeAttributeValue(key, val)
	default:
		return hexVal(val)
	}
}

// decodeAttributeValue decodes an attribute value based on the type byte in the key.
func decodeAttributeValue(key, val []byte) string {
	if len(key) < 3 {
		return hexVal(val)
	}

	attrTypeByte := key[len(key)-1]

	switch attrTypeByte {
	case dal.AttributeCodeVolume:
		return tryProtoJSON(val, &raftcmdpb.VolumePair{})
	case dal.AttributeCodeMetadata:
		return tryProtoJSON(val, &commonpb.MetadataValue{})
	case dal.AttributeCodeReference:
		return tryProtoJSON(val, &commonpb.TransactionReferenceValue{})
	case dal.AttributeCodeLedger:
		return tryProtoJSON(val, &commonpb.LedgerInfo{})
	case dal.AttributeCodeBoundary:
		return tryProtoJSON(val, &raftcmdpb.LedgerBoundaries{})
	case dal.AttributeCodeTransaction:
		return tryProtoJSON(val, &commonpb.TransactionState{})
	default:
		return hexVal(val)
	}
}

func tryProtoJSON(val []byte, msg proto.Message) string {
	if err := proto.Unmarshal(val, msg); err != nil {
		return fmt.Sprintf("(proto decode error: %v) hex=%s", err, hex.EncodeToString(val))
	}

	opts := protojson.MarshalOptions{Multiline: false, EmitUnpopulated: false}

	jsonBytes, err := opts.Marshal(msg)
	if err != nil {
		return fmt.Sprintf("(json encode error: %v) hex=%s", err, hex.EncodeToString(val))
	}

	return string(jsonBytes)
}

func hexVal(val []byte) string {
	if len(val) > 128 {
		return fmt.Sprintf("hex(%d bytes)=%s...", len(val), hex.EncodeToString(val[:128]))
	}

	return fmt.Sprintf("hex(%d bytes)=%s", len(val), hex.EncodeToString(val))
}

func safeString(b []byte) string {
	s := string(b)
	s = strings.ReplaceAll(s, "\x00", "\\0")
	s = strings.Map(func(r rune) rune {
		if r < 32 || r > 126 {
			return '.'
		}

		return r
	}, s)

	return s
}
