package usagestore

import (
	"errors"
	"fmt"

	"github.com/linxGnu/grocksdb"
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// WriteSession owns one atomic RocksDB batch. A successful Commit or Cancel
// releases it; writes use the store's WAL-disabled options.
type WriteSession struct {
	db         *grocksdb.DB
	options    *grocksdb.WriteOptions
	batch      *grocksdb.WriteBatch
	KeyBuilder *dal.KeyBuilder
	committed  bool
}

func (b *WriteSession) active() error {
	if b.committed {
		return errors.New("write session already committed")
	}
	if b.batch == nil {
		return errors.New("write session already cancelled")
	}

	return nil
}

func (b *WriteSession) Cancel() error {
	if b.batch != nil {
		b.batch.Destroy()
		b.batch = nil
	}

	return nil
}

func (b *WriteSession) Commit() error {
	if err := b.active(); err != nil {
		return err
	}
	if err := b.db.Write(b.options, b.batch); err != nil {
		return fmt.Errorf("committing usage write session: %w", err)
	}
	b.committed = true

	return b.Cancel()
}

func (b *WriteSession) SetBytes(key, value []byte) error {
	if err := b.active(); err != nil {
		return err
	}
	b.batch.Put(key, value)

	return nil
}

func (b *WriteSession) SetProto(key []byte, msg proto.Message) error {
	if err := b.active(); err != nil {
		return err
	}
	var data []byte
	var err error
	if m, ok := msg.(interface{ MarshalVT() ([]byte, error) }); ok {
		data, err = m.MarshalVT()
	} else {
		data, err = proto.Marshal(msg)
	}
	if err != nil {
		return err
	}
	b.batch.Put(key, data)

	return nil
}

func (b *WriteSession) DeleteKey(key []byte) error {
	if err := b.active(); err != nil {
		return err
	}
	b.batch.Delete(key)

	return nil
}

func (b *WriteSession) DeleteRangeNoSync(start, end []byte) error {
	if err := b.active(); err != nil {
		return err
	}
	b.batch.DeleteRange(start, end)

	return nil
}
