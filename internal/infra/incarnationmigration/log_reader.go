package incarnationmigration

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/formancehq/ledger/v3/internal/pkg/cursor"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// auditLogReader follows log sequences in audit order. The normal path reads
// each SST block once via an iterator instead of probing Pebble for every log.
// A rare out-of-order reference still uses a point read without reordering
// the audit's business operations.
type auditLogReader struct {
	reader dal.PebbleReader
	cursor cursor.Cursor[*commonpb.Log]
	last   *commonpb.Log
}

func newAuditLogReader(ctx context.Context, reader dal.PebbleReader) (*auditLogReader, error) {
	logs, err := query.ReadLogsSince(ctx, reader, 0)
	if err != nil {
		return nil, err
	}
	return &auditLogReader{reader: reader, cursor: logs}, nil
}

func (r *auditLogReader) close() error { return r.cursor.Close() }

func (r *auditLogReader) get(ctx context.Context, sequence uint64) (*commonpb.Log, error) {
	if r.last != nil {
		switch {
		case sequence == r.last.GetSequence():
			return r.last, nil
		case sequence < r.last.GetSequence():
			log, err := query.ReadLogBySequence(ctx, r.reader, sequence)
			if err != nil {
				return nil, err
			}
			if log == nil {
				return nil, fmt.Errorf("missing audit-linked log %d", sequence)
			}
			return log, nil
		}
	}
	for {
		log, err := r.cursor.Next()
		if errors.Is(err, io.EOF) {
			return nil, fmt.Errorf("missing audit-linked log %d", sequence)
		}
		if err != nil {
			return nil, err
		}
		if log.GetSequence() < sequence {
			continue
		}
		r.last = log
		if log.GetSequence() != sequence {
			return nil, fmt.Errorf("missing audit-linked log %d", sequence)
		}
		return log, nil
	}
}
