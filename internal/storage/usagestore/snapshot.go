package usagestore

import (
	"encoding/binary"
	"fmt"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/linxGnu/grocksdb"
)

// Snapshot pins one coherent RocksDB sequence until Close.
type Snapshot struct {
	db   *grocksdb.DB
	snap *grocksdb.Snapshot
}

func (s *Store) NewSnapshot() *Snapshot { return &Snapshot{db: s.db, snap: s.db.NewSnapshot()} }
func (s *Snapshot) Close() error        { s.db.ReleaseSnapshot(s.snap); return nil }

func (s *Snapshot) get(key []byte) ([]byte, error) {
	opts := grocksdb.NewDefaultReadOptions()
	defer opts.Destroy()
	opts.SetSnapshot(s.snap)
	slice, err := s.db.Get(opts, key)
	if err != nil {
		return nil, err
	}
	defer slice.Free()
	if !slice.Exists() {
		return nil, nil
	}
	return append([]byte{}, slice.Data()...), nil
}

func (s *Snapshot) GetCounter(ledgerName string, counterID byte) (uint64, error) {
	key := CounterKey(dal.NewKeyBuilder(), ledgerName, counterID)
	v, err := s.get(key)
	if err != nil {
		return 0, fmt.Errorf("reading counter %#x for ledger %q: %w", counterID, ledgerName, err)
	}
	if v == nil {
		return 0, nil
	}
	if len(v) != 8 {
		return 0, fmt.Errorf("corrupt counter value: expected 8 bytes, got %d", len(v))
	}
	return binary.BigEndian.Uint64(v), nil
}

func (s *Snapshot) GetTemplateUsage(ledgerName, templateName string) (*commonpb.TemplateUsage, error) {
	key := TemplateUsageKey(dal.NewKeyBuilder(), ledgerName, templateName)
	v, err := s.get(key)
	if err != nil {
		return nil, fmt.Errorf("reading template usage: %w", err)
	}
	if v == nil {
		return nil, nil
	}
	usage := &commonpb.TemplateUsage{}
	if err := usage.UnmarshalVT(v); err != nil {
		return nil, fmt.Errorf("unmarshaling template usage: %w", err)
	}
	return usage, nil
}
