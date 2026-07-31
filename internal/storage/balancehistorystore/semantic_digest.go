package balancehistorystore

import (
	"bytes"
	"container/heap"
	"context"
	"encoding/binary"
	"fmt"
	"hash"
	"math/big"

	"github.com/cockroachdb/pebble/v2"

	"github.com/formancehq/ledger/v3/internal/storage/balancehistoryarchive"
)

func advanceSemanticCursor(ctx context.Context, queue *semanticCursorHeap, cursor *semanticRunCursor) error {
	valid, err := cursor.Advance(ctx)
	if err != nil {
		return err
	}
	if valid {
		heap.Push(queue, cursor)
	}

	return nil
}

func writeSemanticField(digest hash.Hash, value []byte) {
	var length [8]byte
	binary.BigEndian.PutUint64(length[:], uint64(len(value)))
	_, _ = digest.Write(length[:])
	_, _ = digest.Write(value)
}

type semanticRecordCursor interface {
	Next() bool
	Record() ([]byte, []byte)
	Err() error
	Close() error
}

type semanticRunCursor struct {
	view             *View
	runID            uint64
	records          semanticRecordCursor
	previousIdentity recordIdentity
	previousValue    cumulativeValue
	previousKey      []byte
	hasPrevious      bool
	key              []byte
	input            *big.Int
	output           *big.Int
}

func (c *semanticRunCursor) Advance(ctx context.Context) (bool, error) {
	for c.records.Next() {
		if err := ctx.Err(); err != nil {
			return false, err
		}

		key, encodedValue := c.records.Record()
		runID, identity, _, err := decodeDataKey(key)
		if err != nil {
			return false, c.view.corrupt(fmt.Sprintf("decoding run %d semantic digest key: %v", c.runID, err))
		}
		if runID != c.runID {
			return false, c.view.corrupt(fmt.Sprintf(
				"semantic digest run key identifies run %d, expected %d",
				runID,
				c.runID,
			))
		}

		canonicalKey := append([]byte(nil), key[9:]...)
		if c.previousKey != nil && bytes.Compare(canonicalKey, c.previousKey) <= 0 {
			return false, c.view.corrupt(fmt.Sprintf("run %d semantic records are not strictly ordered", c.runID))
		}
		current, err := decodeCumulative(encodedValue)
		if err != nil {
			return false, c.view.corrupt(fmt.Sprintf("decoding run %d semantic digest value: %v", c.runID, err))
		}

		previous := newCumulativeValue()
		if c.hasPrevious && identity == c.previousIdentity {
			previous = c.previousValue
		}
		input := new(big.Int).Sub(current.input, previous.input)
		output := new(big.Int).Sub(current.output, previous.output)
		if input.Sign() < 0 || output.Sign() < 0 {
			return false, c.view.corrupt(fmt.Sprintf("run %d cumulative value decreased", c.runID))
		}

		c.previousIdentity = identity
		c.previousValue = current.clone()
		c.previousKey = canonicalKey
		c.hasPrevious = true
		if input.Sign() == 0 && output.Sign() == 0 {
			continue
		}

		c.key = canonicalKey
		c.input = input
		c.output = output

		return true, nil
	}
	if err := c.records.Err(); err != nil {
		if cold, ok := c.records.(*coldSemanticRecordCursor); ok {
			if cold.errMapped {
				return false, err
			}

			return false, c.view.store.failArchive(err)
		}

		return false, fmt.Errorf("iterating run %d for semantic digest: %w", c.runID, err)
	}

	return false, nil
}

func (c *semanticRunCursor) Close() error {
	return c.records.Close()
}

type semanticCursorHeap []*semanticRunCursor

func (h semanticCursorHeap) Len() int {
	return len(h)
}

func (h semanticCursorHeap) Less(i, j int) bool {
	return bytes.Compare(h[i].key, h[j].key) < 0
}

func (h semanticCursorHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *semanticCursorHeap) Push(value any) {
	*h = append(*h, value.(*semanticRunCursor))
}

func (h *semanticCursorHeap) Pop() any {
	old := *h
	last := len(old) - 1
	value := old[last]
	old[last] = nil
	*h = old[:last]

	return value
}

type hotSemanticRecordCursor struct {
	iter    *pebble.Iterator
	started bool
	valid   bool
}

func (c *hotSemanticRecordCursor) Next() bool {
	if !c.started {
		c.started = true
		c.valid = c.iter.First()
	} else if c.valid {
		c.valid = c.iter.Next()
	}

	return c.valid
}

func (c *hotSemanticRecordCursor) Record() ([]byte, []byte) {
	return c.iter.Key(), c.iter.Value()
}

func (c *hotSemanticRecordCursor) Err() error {
	return c.iter.Error()
}

func (c *hotSemanticRecordCursor) Close() error {
	return c.iter.Close()
}

type coldSemanticRecordCursor struct {
	ctx           context.Context
	view          *View
	cold          *coldRunView
	prefix        []byte
	upper         []byte
	partIndex     int
	reader        *balancehistoryarchive.IndexedReader
	readerStarted bool
	valid         bool
	err           error
	errMapped     bool
	closed        bool
}

func newColdSemanticRecordCursor(
	ctx context.Context,
	view *View,
	cold *coldRunView,
	prefix []byte,
) *coldSemanticRecordCursor {
	// IndexedReader carries mutable cursor state and explicitly is not safe for
	// concurrent calls. Hold the run mutex across the chained part cursors so no
	// other read can reposition one of their lazy readers between Next calls.
	cold.mu.Lock()

	return &coldSemanticRecordCursor{
		ctx:       ctx,
		view:      view,
		cold:      cold,
		prefix:    append([]byte(nil), prefix...),
		upper:     prefixEnd(prefix),
		partIndex: -1,
	}
}

func (c *coldSemanticRecordCursor) Next() bool {
	if c.closed || c.err != nil {
		return false
	}

	for {
		if c.reader == nil {
			if !c.openNextDataPart() {
				return false
			}
		}

		if !c.readerStarted {
			c.readerStarted = true
			c.valid = c.reader.SeekGE(c.prefix)
		} else if c.valid {
			c.valid = c.reader.Next()
		}
		if !c.valid {
			if err := c.reader.Err(); err != nil {
				c.err = err

				return false
			}
			c.reader = nil

			continue
		}
		if !bytes.HasPrefix(c.reader.Record().Key, c.prefix) {
			// Parts are immutable, non-overlapping key ranges. Once this part
			// leaves the data prefix, only a later intersecting part can match.
			c.reader = nil
			c.valid = false

			continue
		}

		return true
	}
}

func (c *coldSemanticRecordCursor) openNextDataPart() bool {
	for c.partIndex++; c.partIndex < len(c.cold.parts); c.partIndex++ {
		part := c.cold.parts[c.partIndex].meta
		if !partIntersects(part, c.prefix, c.upper) {
			continue
		}

		reader, err := c.view.coldPartReader(c.ctx, c.cold, c.partIndex)
		if err != nil {
			c.err = err
			c.errMapped = true

			return false
		}
		c.reader = reader
		c.readerStarted = false
		c.valid = false

		return true
	}

	return false
}

func (c *coldSemanticRecordCursor) Record() ([]byte, []byte) {
	record := c.reader.Record()

	return record.Key, record.Value
}

func (c *coldSemanticRecordCursor) Err() error {
	return c.err
}

func (c *coldSemanticRecordCursor) Close() error {
	if c.closed {
		return nil
	}
	c.closed = true
	c.cold.mu.Unlock()

	return nil
}
