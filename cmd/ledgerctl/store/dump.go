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

	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
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
	case dal.ZoneIdempotency:
		return describeIdempotencyKey(key)
	case dal.ZoneCold:
		return describeColdKey(key)
	case dal.ZonePerLedger:
		return describePerLedgerKey(key)
	case dal.ZoneGlobal:
		return describeGlobalKey(key)
	case dal.ZoneAttributes:
		return describeAttributeKey(key)
	case dal.ZoneCache:
		return describeCacheKey(key)
	default:
		return fmt.Sprintf("UNKNOWN(0x%02X)", key[0])
	}
}

// describeCacheKey returns a human-readable label for a cache zone key.
func describeCacheKey(key []byte) string {
	if len(key) < 2 {
		return "CACHE (short key)"
	}

	if key[1] == dal.SubCacheMeta {
		return "CACHE_META"
	}

	if len(key) >= 3 && key[2] == dal.SubCacheGenMeta {
		return fmt.Sprintf("CACHE_GEN_META gen=%d", key[1])
	}

	if len(key) >= 3 {
		return fmt.Sprintf("CACHE gen=%d type=0x%02X rest=%s", key[1], key[2], hex.EncodeToString(key[3:]))
	}

	return fmt.Sprintf("CACHE gen=%d", key[1])
}

// describeAttributeKey returns a human-readable label for an attribute key.
func describeAttributeKey(key []byte) string {
	// Layout: [ZoneAttributes][attrType(1)][canonicalKey...]
	if len(key) < 3 { // 1 prefix + 1 type + at least 1 canonical
		return "ATTR (short key)"
	}

	attrTypeByte := key[1]
	canonicalHex := hex.EncodeToString(key[2:])

	var attrType string
	switch attrTypeByte {
	case dal.SubAttrVolume:
		attrType = "Volume"
	case dal.SubAttrMetadata:
		attrType = "Metadata"
	case dal.SubAttrReference:
		attrType = "Reference"
	case dal.SubAttrLedger:
		attrType = "Ledger"
	case dal.SubAttrBoundary:
		attrType = "Boundary"
	case dal.SubAttrTransaction:
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
	case dal.ZoneIdempotency:
		if len(key) >= 2 && key[1] == dal.SubIdempTimeIdx {
			return "(empty time index entry)"
		}

		return tryProtoJSON(val, &commonpb.IdempotencyKeyValue{})
	case dal.ZoneCold:
		if len(key) >= 2 && key[1] == dal.SubColdAuditItem {
			return tryProtoJSON(val, &auditpb.AuditItem{})
		}

		if len(key) >= 2 && key[1] == dal.SubColdAudit {
			return tryProtoJSON(val, &auditpb.AuditEntry{})
		}

		return tryProtoJSON(val, &commonpb.Log{})
	case dal.ZonePerLedger:
		if len(key) >= 2 && key[1] == dal.SubPLPreparedQuery {
			return tryProtoJSON(val, &commonpb.PreparedQuery{})
		}

		return hexVal(val)
	case dal.ZoneGlobal:
		return decodeGlobalValue(key, val)
	case dal.ZoneAttributes:
		return decodeAttributeValue(key, val)
	default:
		return hexVal(val)
	}
}

// decodeGlobalValue decodes a value in the global zone based on the sub-prefix byte.
func decodeGlobalValue(key, val []byte) string {
	if len(key) < 2 {
		return hexVal(val)
	}

	switch key[1] {
	case dal.SubGlobLedgerInfo:
		return tryProtoJSON(val, &commonpb.LedgerInfo{})
	case dal.SubGlobPeriods:
		return tryProtoJSON(val, &commonpb.Period{})
	case dal.SubGlobEventsConfig:
		return tryProtoJSON(val, &commonpb.SinkConfig{})
	case dal.SubGlobSinkStatus:
		return tryProtoJSON(val, &commonpb.SinkStatus{})
	case dal.SubGlobQueryCheckpoint:
		return tryProtoJSON(val, &raftcmdpb.QueryCheckpointState{})
	case dal.SubGlobLastAppliedIndex, dal.SubGlobLastAppliedTimestamp,
		dal.SubGlobNextPeriodID, dal.SubGlobNextQueryCheckpointID:
		if len(val) == 8 {
			return fmt.Sprintf("uint64=%d", binary.BigEndian.Uint64(val))
		}

		return hexVal(val)
	case dal.SubGlobSinkCursor:
		if len(val) == 8 {
			return fmt.Sprintf("cursor=%d", binary.BigEndian.Uint64(val))
		}

		return hexVal(val)
	case dal.SubGlobMaintenanceMode, dal.SubGlobSigningConfig:
		if len(val) == 1 {
			return fmt.Sprintf("enabled=%v", val[0] != 0)
		}

		return hexVal(val)
	case dal.SubGlobPeriodSchedule, dal.SubGlobQueryCheckpointSchedule:
		return fmt.Sprintf("cron=%q", string(val))
	case dal.SubGlobPersistedConfig:
		return tryProtoJSON(val, &commonpb.PersistedConfig{})
	case dal.SubGlobClusterConfig:
		return tryProtoJSON(val, &commonpb.PersistedClusterState{})
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
	case dal.SubAttrVolume:
		return tryProtoJSON(val, &raftcmdpb.VolumePair{})
	case dal.SubAttrMetadata:
		return tryProtoJSON(val, &commonpb.MetadataValue{})
	case dal.SubAttrReference:
		return tryProtoJSON(val, &commonpb.TransactionReferenceValue{})
	case dal.SubAttrLedger:
		return tryProtoJSON(val, &commonpb.LedgerInfo{})
	case dal.SubAttrBoundary:
		return tryProtoJSON(val, &raftcmdpb.LedgerBoundaries{})
	case dal.SubAttrTransaction:
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

// stripNull removes a trailing null terminator if present.
func stripNull(b []byte) []byte {
	if len(b) > 0 && b[len(b)-1] == 0x00 {
		return b[:len(b)-1]
	}

	return b
}

func describeGlobalKey(key []byte) string {
	if len(key) < 2 {
		return "GLOBAL (short key)"
	}

	switch key[1] {
	case dal.SubGlobLastAppliedIndex:
		return "LAST_APPLIED_INDEX"
	case dal.SubGlobLastAppliedTimestamp:
		return "LAST_APPLIED_TIMESTAMP"
	case dal.SubGlobLedgerInfo:
		return "LEDGER_INFO name=" + safeString(stripNull(key[2:]))
	case dal.SubGlobSigningKey:
		return "SIGNING_KEY id=" + safeString(key[2:])
	case dal.SubGlobSigningConfig:
		return "SIGNING_CONFIG"
	case dal.SubGlobPeriods:
		if len(key) >= 10 {
			id := binary.BigEndian.Uint64(key[2:10])

			return fmt.Sprintf("PERIOD id=%d", id)
		}

		return "PERIOD (short key)"
	case dal.SubGlobNextPeriodID:
		return "NEXT_PERIOD_ID"
	case dal.SubGlobSinkCursor:
		return "SINK_CURSOR name=" + safeString(key[2:])
	case dal.SubGlobEventsConfig:
		return "EVENTS_CONFIG name=" + safeString(key[2:])
	case dal.SubGlobSinkStatus:
		return "SINK_STATUS name=" + safeString(key[2:])
	case dal.SubGlobMaintenanceMode:
		return "MAINTENANCE_MODE"
	case dal.SubGlobPersistedConfig:
		return "PERSISTED_CONFIG"
	case dal.SubGlobPeriodSchedule:
		return "PERIOD_SCHEDULE"
	case dal.SubGlobQueryCheckpoint:
		if len(key) >= 10 {
			id := binary.BigEndian.Uint64(key[2:10])

			return fmt.Sprintf("QUERY_CHECKPOINT id=%d", id)
		}

		return "QUERY_CHECKPOINT (short key)"
	case dal.SubGlobNextQueryCheckpointID:
		return "NEXT_QUERY_CHECKPOINT_ID"
	case dal.SubGlobQueryCheckpointSchedule:
		return "QUERY_CHECKPOINT_SCHEDULE"
	case dal.SubGlobClusterConfig:
		return "CLUSTER_CONFIG"
	case dal.SubGlobBloom:
		return "BLOOM rest=" + hex.EncodeToString(key[2:])
	default:
		return fmt.Sprintf("GLOBAL(sub=0x%02X)", key[1])
	}
}

func describePerLedgerKey(key []byte) string {
	if len(key) < 2 {
		return "PER_LEDGER (short key)"
	}

	switch key[1] {
	case dal.SubPLReversions:
		return "REVERSIONS rest=" + safeString(key[2:])
	case dal.SubPLPendingCleanup:
		return "PENDING_CLEANUP ledger=" + safeString(stripNull(key[2:]))
	case dal.SubPLPreparedQuery:
		return "PREPARED_QUERY rest=" + safeString(key[2:])
	case dal.SubPLMirrorSourceHead:
		return "MIRROR_SOURCE_HEAD ledger=" + safeString(stripNull(key[2:]))
	case dal.SubPLMirrorCursor:
		return "MIRROR_CURSOR ledger=" + safeString(stripNull(key[2:]))
	case dal.SubPLMirrorStatus:
		return "MIRROR_STATUS ledger=" + safeString(stripNull(key[2:]))
	default:
		return fmt.Sprintf("PER_LEDGER(sub=0x%02X)", key[1])
	}
}

func describeColdKey(key []byte) string {
	if len(key) < 2 {
		return "COLD (short key)"
	}

	switch key[1] {
	case dal.SubColdLog:
		if len(key) >= 10 {
			seq := binary.BigEndian.Uint64(key[2:10])

			return fmt.Sprintf("LOG seq=%d", seq)
		}

		return "LOG (short key)"
	case dal.SubColdAudit:
		if len(key) >= 10 {
			seq := binary.BigEndian.Uint64(key[2:10])

			return fmt.Sprintf("AUDIT seq=%d", seq)
		}

		return "AUDIT (short key)"
	case dal.SubColdAuditItem:
		if len(key) >= 14 {
			auditSeq := binary.BigEndian.Uint64(key[2:10])
			orderIdx := binary.BigEndian.Uint32(key[10:14])

			return fmt.Sprintf("AUDIT_ITEM audit_seq=%d order_idx=%d", auditSeq, orderIdx)
		}

		return "AUDIT_ITEM (short key)"
	default:
		return fmt.Sprintf("COLD(sub=0x%02X)", key[1])
	}
}

func describeIdempotencyKey(key []byte) string {
	if len(key) < 2 {
		return "IDEMPOTENCY (short key)"
	}

	switch key[1] {
	case dal.SubIdempKeys:
		return "IDEMPOTENCY key_hash=" + hex.EncodeToString(key[2:])
	case dal.SubIdempTimeIdx:
		if len(key) >= 10 {
			ts := binary.BigEndian.Uint64(key[2:10])

			return fmt.Sprintf("IDEMPOTENCY_TIME_IDX created_at=%d hash=%s", ts, hex.EncodeToString(key[10:]))
		}

		return "IDEMPOTENCY_TIME_IDX (short key)"
	default:
		return fmt.Sprintf("IDEMPOTENCY(sub=0x%02X)", key[1])
	}
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
