package query

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/cockroachdb/pebble/v2"

	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// GrpcLogSinkCursorKey retains the global position by name across removal.
// The creation incarnation separately fences stale acknowledgements.
func GrpcLogSinkCursorKey(name string) []byte {
	return dal.NewKeyBuilder().PutZonePrefix(dal.ZoneGlobal, dal.SubGlobGrpcLogSinkCursor).PutString(name).Build()
}

func ReadGrpcLogSinkCursor(reader dal.PebbleGetter, name string) (uint64, error) {
	if name == "" {
		return 0, errors.New("reading gRPC log sink cursor: empty name")
	}

	return dal.ReadUint64(reader, GrpcLogSinkCursorKey(name), 0)
}

// ReadAllGrpcLogSinkCursors restores the apply-time monotonic cursor map.
func ReadAllGrpcLogSinkCursors(reader dal.PebbleReader) (map[string]uint64, error) {
	lo := []byte{dal.ZoneGlobal, dal.SubGlobGrpcLogSinkCursor}
	hi := []byte{dal.ZoneGlobal, dal.SubGlobGrpcLogSinkCursor + 1}
	iter, err := reader.NewIter(&pebble.IterOptions{LowerBound: lo, UpperBound: hi})
	if err != nil {
		return nil, fmt.Errorf("scanning gRPC log sink cursors: %w", err)
	}
	defer func() { _ = iter.Close() }()
	cursors := make(map[string]uint64)
	for iter.First(); iter.Valid(); iter.Next() {
		key, value := iter.Key(), iter.Value()
		if len(key) <= 2 || len(value) != 8 {
			return nil, errors.New("invalid gRPC log sink cursor row")
		}
		cursors[string(key[2:])] = binary.BigEndian.Uint64(value)
	}
	if err := iter.Error(); err != nil {
		return nil, fmt.Errorf("scanning gRPC log sink cursors: %w", err)
	}

	return cursors, nil
}

// ReadSinkCursor returns the last successfully emitted log sequence for a named sink from the given reader.
// Returns 0 if no cursor has been persisted yet.
func ReadSinkCursor(reader dal.PebbleGetter, sinkName string) (uint64, error) {
	kb := dal.NewKeyBuilder()
	kb.PutZonePrefix(dal.ZoneGlobal, dal.SubGlobSinkCursor).
		PutString(sinkName)

	v, err := dal.ReadUint64(reader, kb.Build(), 0)
	if err != nil {
		return 0, fmt.Errorf("reading sink cursor: %w", err)
	}

	return v, nil
}

// ReadAllSinkStatuses returns all persisted sink statuses from the given reader.
func ReadAllSinkStatuses(reader dal.PebbleReader) ([]*commonpb.SinkStatus, error) {
	statuses, err := dal.CollectZone[*commonpb.SinkStatus](reader, dal.ZoneGlobal, dal.SubGlobSinkStatus)
	if err != nil {
		return nil, fmt.Errorf("reading sink statuses: %w", err)
	}

	return statuses, nil
}

// BuildSinkStatuses returns the per-sink status view exposed by the
// events-sinks read endpoints: one entry per configured sink in `sinks`,
// carrying its persisted error status (SubGlobSinkStatus), if any, and its
// last cursor (SubGlobSinkCursor for outbound sinks,
// SubGlobGrpcLogSinkCursor for gRPC log sinks). Removing a sink keeps its
// per-name status and cursor keys, so persisted statuses of sinks absent from
// `sinks` are omitted. Callers pass a single reader (ideally one snapshot) so
// statuses, cursors and the configs are read from one consistent point in
// time.
func BuildSinkStatuses(reader dal.PebbleReader, sinks []*commonpb.SinkConfig) ([]*commonpb.SinkStatus, error) {
	errorStatuses, err := ReadAllSinkStatuses(reader)
	if err != nil {
		return nil, err
	}

	errorBySink := make(map[string]*commonpb.SinkStatus, len(errorStatuses))
	for _, s := range errorStatuses {
		errorBySink[s.GetSinkName()] = s
	}

	statuses := make([]*commonpb.SinkStatus, 0, len(sinks))
	for _, sink := range sinks {
		readCursor := ReadSinkCursor
		if sink.GetGrpcLog() != nil {
			readCursor = ReadGrpcLogSinkCursor
		}
		cursor, err := readCursor(reader, sink.GetName())
		if err != nil {
			return nil, fmt.Errorf("loading sink cursor for %q: %w", sink.GetName(), err)
		}

		status, ok := errorBySink[sink.GetName()]
		if !ok {
			status = &commonpb.SinkStatus{SinkName: sink.GetName()}
		}
		status.Cursor = cursor
		statuses = append(statuses, status)
	}

	return statuses, nil
}

// ReadAllSinkConfigs loads all sink configurations from the attributes zone.
func ReadAllSinkConfigs(attr *attributes.Attribute[*commonpb.SinkConfig], reader dal.PebbleReader) ([]*commonpb.SinkConfig, error) {
	entries, err := attr.ComputeAllForPrefix(reader, nil)
	if err != nil {
		return nil, fmt.Errorf("scanning sink configs: %w", err)
	}

	configs := make([]*commonpb.SinkConfig, 0, len(entries))
	for _, entry := range entries {
		configs = append(configs, entry.Value)
	}

	return configs, nil
}
