package usagestore

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/cockroachdb/pebble/v2"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// Snapshot is a point-in-time view of the usage store. Multiple Gets against
// the same Snapshot observe a coherent set of counter values — no
// intervening usagebuilder commit can shift one counter relative to another
// mid-read.
//
// Callers MUST call Close() to release the underlying Pebble snapshot.
type Snapshot struct {
	snap *pebble.Snapshot
}

// NewSnapshot returns a fresh point-in-time snapshot. The caller owns the
// snapshot and is responsible for closing it.
func (s *Store) NewSnapshot() *Snapshot {
	return &Snapshot{snap: s.db.NewSnapshot()}
}

// Close releases the underlying Pebble snapshot.
func (s *Snapshot) Close() error {
	return s.snap.Close()
}

// GetCounter reads the value of a per-ledger event counter from this
// snapshot. Missing keys return 0.
func (s *Snapshot) GetCounter(ledgerName string, counterID byte) (uint64, error) {
	kb := dal.NewKeyBuilder()
	key := CounterKey(kb, ledgerName, counterID)

	v, closer, err := s.snap.Get(key)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return 0, nil
		}

		return 0, fmt.Errorf("reading counter %#x for ledger %q: %w", counterID, ledgerName, err)
	}

	defer func() { _ = closer.Close() }()

	if len(v) != 8 {
		return 0, fmt.Errorf("corrupt counter value: expected 8 bytes, got %d", len(v))
	}

	return binary.BigEndian.Uint64(v), nil
}

// GetTemplateUsage reads a template usage record from this snapshot.
// Returns (nil, nil) when the entry does not exist.
func (s *Snapshot) GetTemplateUsage(ledgerName, templateName string) (*commonpb.TemplateUsage, error) {
	kb := dal.NewKeyBuilder()
	key := TemplateUsageKey(kb, ledgerName, templateName)

	v, closer, err := s.snap.Get(key)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return nil, nil
		}

		return nil, fmt.Errorf("reading template usage: %w", err)
	}

	defer func() { _ = closer.Close() }()

	usage := &commonpb.TemplateUsage{}
	if err := usage.UnmarshalVT(v); err != nil {
		return nil, fmt.Errorf("unmarshaling template usage: %w", err)
	}

	return usage, nil
}
