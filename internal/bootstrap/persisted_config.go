package bootstrap

import (
	"errors"
	"fmt"

	"github.com/cockroachdb/pebble/v2"
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
)

// ConfigMismatchError is returned when a persisted configuration value differs
// from the current startup configuration.
type ConfigMismatchError struct {
	Field     string
	Persisted string
	Current   string
}

func (e *ConfigMismatchError) Error() string {
	return fmt.Sprintf(
		"configuration mismatch for %s: persisted=%s, current=%s (use --unsafe-skip-config-validation to override)",
		e.Field, e.Persisted, e.Current,
	)
}

// LoadPersistedConfig reads the persisted configuration from Pebble.
// Returns nil if no configuration has been persisted yet (first boot).
func LoadPersistedConfig(reader dal.PebbleReader) (*commonpb.PersistedConfig, error) {
	value, closer, err := reader.Get([]byte{dal.ZoneGlobal, dal.SubGlobPersistedConfig})
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return nil, nil
		}

		return nil, fmt.Errorf("loading persisted config: %w", err)
	}

	defer func() { _ = closer.Close() }()

	cfg := &commonpb.PersistedConfig{}
	if err := proto.Unmarshal(value, cfg); err != nil {
		return nil, fmt.Errorf("unmarshaling persisted config: %w", err)
	}

	return cfg, nil
}

// SavePersistedConfig writes the persisted configuration to the batch.
func SavePersistedConfig(b *dal.Batch, cfg *commonpb.PersistedConfig) error {
	value, err := proto.Marshal(cfg)
	if err != nil {
		return fmt.Errorf("marshaling persisted config: %w", err)
	}

	return b.SetBytes([]byte{dal.ZoneGlobal, dal.SubGlobPersistedConfig}, value)
}
