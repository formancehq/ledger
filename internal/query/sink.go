package query

import (
	"fmt"

	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
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
func ReadAllSinkStatuses(reader dal.PebbleReader) ([]*commonpb.SinkStatus, error) {
	statuses, err := dal.CollectZone[*commonpb.SinkStatus](reader, dal.ZoneGlobal, dal.SubGlobSinkStatus)
	if err != nil {
		return nil, fmt.Errorf("reading sink statuses: %w", err)
	}

	return statuses, nil
}

// BuildSinkStatuses returns the per-sink status view exposed by the
// events-sinks read endpoints: the persisted error statuses
// (SubGlobSinkStatus) merged with the last-emitted cursor (SubGlobSinkCursor)
// for every configured sink. A sink with no persisted error status still gets
// an entry carrying just its cursor, so the result covers every sink in
// `sinks`. Callers pass a single reader (ideally one snapshot) so statuses,
// cursors and the configs are read from one consistent point in time.
func BuildSinkStatuses(reader dal.PebbleReader, sinks []*commonpb.SinkConfig) ([]*commonpb.SinkStatus, error) {
	errorStatuses, err := ReadAllSinkStatuses(reader)
	if err != nil {
		return nil, err
	}

	statusBySink := make(map[string]*commonpb.SinkStatus, len(errorStatuses))
	for _, s := range errorStatuses {
		statusBySink[s.GetSinkName()] = s
	}

	// Enrich with cursor values for every configured sink.
	for _, sink := range sinks {
		cursor, err := ReadSinkCursor(reader, sink.GetName())
		if err != nil {
			return nil, fmt.Errorf("loading sink cursor for %q: %w", sink.GetName(), err)
		}

		if existing, ok := statusBySink[sink.GetName()]; ok {
			existing.Cursor = cursor
		} else {
			statusBySink[sink.GetName()] = &commonpb.SinkStatus{
				SinkName: sink.GetName(),
				Cursor:   cursor,
			}
		}
	}

	statuses := make([]*commonpb.SinkStatus, 0, len(statusBySink))
	for _, s := range statusBySink {
		statuses = append(statuses, s)
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
