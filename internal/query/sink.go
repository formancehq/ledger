package query

import (
	"fmt"

	"github.com/formancehq/ledger/pkg/proto/v3/ledgerpb"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

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
func ReadAllSinkStatuses(reader dal.PebbleReader) ([]*ledgerpb.SinkStatus, error) {
	statuses, err := dal.CollectZone[*ledgerpb.SinkStatus](reader, dal.ZoneGlobal, dal.SubGlobSinkStatus)
	if err != nil {
		return nil, fmt.Errorf("reading sink statuses: %w", err)
	}

	return statuses, nil
}

// BuildSinkStatuses returns the per-sink status view exposed by the
// events-sinks read endpoints: one entry per configured sink in `sinks`,
// carrying its persisted error status (SubGlobSinkStatus), if any, and its
// last-emitted cursor (SubGlobSinkCursor). Removing a sink keeps its
// per-name status and cursor keys, so persisted statuses of sinks absent from
// `sinks` are omitted. Callers pass a single reader (ideally one snapshot) so
// statuses, cursors and the configs are read from one consistent point in
// time.
func BuildSinkStatuses(reader dal.PebbleReader, sinks []*ledgerpb.SinkConfig) ([]*ledgerpb.SinkStatus, error) {
	errorStatuses, err := ReadAllSinkStatuses(reader)
	if err != nil {
		return nil, err
	}

	errorBySink := make(map[string]*ledgerpb.SinkStatus, len(errorStatuses))
	for _, s := range errorStatuses {
		errorBySink[s.GetSinkName()] = s
	}

	statuses := make([]*ledgerpb.SinkStatus, 0, len(sinks))
	for _, sink := range sinks {
		cursor, err := ReadSinkCursor(reader, sink.GetName())
		if err != nil {
			return nil, fmt.Errorf("loading sink cursor for %q: %w", sink.GetName(), err)
		}

		status, ok := errorBySink[sink.GetName()]
		if !ok {
			status = &ledgerpb.SinkStatus{SinkName: sink.GetName()}
		}
		status.Cursor = cursor
		statuses = append(statuses, status)
	}

	return statuses, nil
}

// ReadAllSinkConfigs loads all sink configurations from the attributes zone.
func ReadAllSinkConfigs(attr *attributes.Attribute[*ledgerpb.SinkConfig], reader dal.PebbleReader) ([]*ledgerpb.SinkConfig, error) {
	entries, err := attr.ComputeAllForPrefix(reader, nil)
	if err != nil {
		return nil, fmt.Errorf("scanning sink configs: %w", err)
	}

	configs := make([]*ledgerpb.SinkConfig, 0, len(entries))
	for _, entry := range entries {
		configs = append(configs, entry.Value)
	}

	return configs, nil
}
