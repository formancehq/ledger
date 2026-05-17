package query

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/cockroachdb/pebble/v2"
	"google.golang.org/protobuf/proto"

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

	get, closer, err := reader.Get(kb.Build())
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return 0, nil
		}

		return 0, fmt.Errorf("reading sink cursor: %w", err)
	}

	defer func() {
		_ = closer.Close()
	}()

	if len(get) < 8 {
		return 0, nil
	}

	return binary.BigEndian.Uint64(get[:8]), nil
}

// ReadAllSinkStatuses returns all persisted sink statuses from the given reader.
func ReadAllSinkStatuses(reader dal.PebbleReader) ([]*commonpb.SinkStatus, error) {
	lowerBound := []byte{dal.ZoneGlobal, dal.SubGlobSinkStatus}
	upperBound := []byte{dal.ZoneGlobal, dal.SubGlobSinkStatus + 1}

	iter, err := dal.NewBoundedIter(reader, lowerBound, upperBound)
	if err != nil {
		return nil, fmt.Errorf("creating iterator for sink statuses: %w", err)
	}

	defer func() { _ = iter.Close() }()

	var statuses []*commonpb.SinkStatus

	for iter.First(); iter.Valid(); iter.Next() {
		value, err := iter.ValueAndErr()
		if err != nil {
			return nil, fmt.Errorf("reading sink status value: %w", err)
		}

		status := &commonpb.SinkStatus{}
		if err := proto.Unmarshal(value, status); err != nil {
			return nil, fmt.Errorf("unmarshaling sink status: %w", err)
		}

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
