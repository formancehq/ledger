package query

import (
	"fmt"

	"github.com/formancehq/ledger-v3-poc/internal/infra/attributes"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
)

// ReadSinkCursor returns the last successfully emitted log sequence for a named sink from the given reader.
// Returns 0 if no cursor has been persisted yet.
func ReadSinkCursor(reader dal.PebbleReader, sinkName string) (uint64, error) {
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
