package query

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/cockroachdb/pebble/v2"
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
)

// ReadSinkCursor returns the last successfully emitted log sequence for a named sink from the given reader.
// Returns 0 if no cursor has been persisted yet.
func ReadSinkCursor(reader dal.PebbleReader, sinkName string) (uint64, error) {
	kb := dal.NewKeyBuilder()
	kb.PutByte(dal.KeyPrefixSinkCursor).
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
	lowerBound := []byte{dal.KeyPrefixSinkStatus}
	upperBound := []byte{dal.KeyPrefixSinkStatus + 1}

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

// ReadSinkConfig loads a single sink configuration by name from the given reader.
// Returns nil (not error) if the sink does not exist.
func ReadSinkConfig(reader dal.PebbleReader, name string) (*commonpb.SinkConfig, error) {
	kb := dal.NewKeyBuilder()
	kb.PutByte(dal.KeyPrefixEventsConfig).
		PutString(name)

	value, closer, err := reader.Get(kb.Build())
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return nil, nil
		}

		return nil, fmt.Errorf("loading sink config %q: %w", name, err)
	}

	defer func() { _ = closer.Close() }()

	cfg := &commonpb.SinkConfig{}
	if err := proto.Unmarshal(value, cfg); err != nil {
		return nil, fmt.Errorf("unmarshaling sink config %q: %w", name, err)
	}

	return cfg, nil
}

// ReadAllSinkConfigs loads all sink configurations by scanning the events config prefix.
func ReadAllSinkConfigs(reader dal.PebbleReader) ([]*commonpb.SinkConfig, error) {
	lowerBound := []byte{dal.KeyPrefixEventsConfig}
	upperBound := []byte{dal.KeyPrefixEventsConfig + 1}

	iter, err := dal.NewBoundedIter(reader, lowerBound, upperBound)
	if err != nil {
		return nil, fmt.Errorf("creating iterator for sink configs: %w", err)
	}

	defer func() { _ = iter.Close() }()

	var configs []*commonpb.SinkConfig

	for iter.First(); iter.Valid(); iter.Next() {
		value, err := iter.ValueAndErr()
		if err != nil {
			return nil, fmt.Errorf("reading sink config value: %w", err)
		}

		cfg := &commonpb.SinkConfig{}
		if err := proto.Unmarshal(value, cfg); err != nil {
			return nil, fmt.Errorf("unmarshaling sink config: %w", err)
		}

		configs = append(configs, cfg)
	}

	return configs, nil
}
