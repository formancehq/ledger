package ctrl

import (
	"encoding/binary"
	"fmt"
	"io"
	"slices"

	"github.com/cockroachdb/pebble"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
	"google.golang.org/protobuf/proto"
)

// transactionCursor uses a single reverse iterator over transaction update keys
// to build full transactions without a second pass. It holds a ReadHandle for
// point-in-time consistency and closes it when the cursor is closed.
type transactionCursor struct {
	handle     *dal.ReadHandle
	iter       *pebble.Iterator
	started    bool
	pageSize   uint32
	count      uint32
	lastTxID   uint64
	txIDOffset int

	// pending holds an already-read update when we overshoot into the next txID's territory
	pendingTxID   uint64
	pendingUpdate *commonpb.TransactionUpdate
	hasPending    bool
}

func (c *transactionCursor) Next() (*commonpb.Transaction, error) {
	if c.pageSize > 0 && c.count >= c.pageSize {
		return nil, io.EOF
	}

	var (
		currentTxID uint64
		updates     []*commonpb.TransactionUpdate
	)

	// If we have a pending entry from the last call, start with it
	if c.hasPending {
		currentTxID = c.pendingTxID
		updates = append(updates, c.pendingUpdate)
		c.hasPending = false
	}

	for {
		var valid bool
		if !c.started {
			c.started = true
			valid = c.iter.Last()
		} else {
			valid = c.iter.Prev()
		}

		if !valid {
			if err := c.iter.Error(); err != nil {
				return nil, err
			}
			// End of iterator — return whatever we collected
			if len(updates) > 0 {
				c.count++
				return c.buildFromUpdates(currentTxID, updates)
			}
			return nil, io.EOF
		}

		key := c.iter.Key()
		if len(key) < c.txIDOffset+8 {
			continue
		}
		txID := binary.BigEndian.Uint64(key[c.txIDOffset : c.txIDOffset+8])

		valueBytes, err := c.iter.ValueAndErr()
		if err != nil {
			return nil, err
		}
		update := &commonpb.TransactionUpdate{}
		if err := proto.Unmarshal(valueBytes, update); err != nil {
			return nil, fmt.Errorf("unmarshaling transaction update: %w", err)
		}

		// First entry for this Next() call
		if len(updates) == 0 {
			currentTxID = txID
			updates = append(updates, update)
			continue
		}

		// Same txID — collect it
		if txID == currentTxID {
			updates = append(updates, update)
			continue
		}

		// Different txID — save as pending and return current collection
		c.pendingTxID = txID
		c.pendingUpdate = update
		c.hasPending = true
		c.count++
		return c.buildFromUpdates(currentTxID, updates)
	}
}

// buildFromUpdates reverses updates (collected in reverse order) and assembles the transaction.
func (c *transactionCursor) buildFromUpdates(txID uint64, updates []*commonpb.TransactionUpdate) (*commonpb.Transaction, error) {
	slices.Reverse(updates)
	return assembleTransaction(c.handle, txID, updates)
}

func (c *transactionCursor) Close() error {
	err := c.iter.Close()
	if closeErr := c.handle.Close(); err == nil {
		err = closeErr
	}
	return err
}
