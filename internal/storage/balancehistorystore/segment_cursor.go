package balancehistorystore

import (
	"bytes"
	"container/heap"
	"context"
	"fmt"
	"math/big"

	"github.com/cockroachdb/pebble/v2"
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

type semanticRecordCursor interface {
	Next() bool
	Record() ([]byte, []byte)
	Err() error
	Close() error
}

// semanticRunCursor decodes cumulative values into deltas for logical segment
// compaction. The historical name is private; it never denotes a Pebble run.
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
		segmentID, identity, _, err := decodeDataKey(key)
		if err != nil {
			return false, c.view.corrupt(fmt.Sprintf("decoding segment %d compaction key: %v", c.runID, err))
		}
		if segmentID != c.runID {
			return false, c.view.corrupt(fmt.Sprintf("segment key identifies %d, expected %d", segmentID, c.runID))
		}

		canonicalKey := bytes.Clone(key[9:])
		if c.previousKey != nil && bytes.Compare(canonicalKey, c.previousKey) <= 0 {
			return false, c.view.corrupt(fmt.Sprintf("segment %d records are not strictly ordered", c.runID))
		}
		current, err := decodeCumulative(encodedValue)
		if err != nil {
			return false, c.view.corrupt(fmt.Sprintf("decoding segment %d cumulative value: %v", c.runID, err))
		}
		previous := newCumulativeValue()
		if c.hasPrevious && identity == c.previousIdentity {
			previous = c.previousValue
		}
		input := new(big.Int).Sub(current.input, previous.input)
		output := new(big.Int).Sub(current.output, previous.output)
		if input.Sign() < 0 || output.Sign() < 0 {
			return false, c.view.corrupt(fmt.Sprintf("segment %d cumulative value decreased", c.runID))
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
		return false, fmt.Errorf("iterating segment %d for compaction: %w", c.runID, err)
	}

	return false, nil
}

func (c *semanticRunCursor) Close() error {
	return c.records.Close()
}

type semanticCursorHeap []*semanticRunCursor

func (h semanticCursorHeap) Len() int           { return len(h) }
func (h semanticCursorHeap) Less(i, j int) bool { return bytes.Compare(h[i].key, h[j].key) < 0 }
func (h semanticCursorHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *semanticCursorHeap) Push(value any)    { *h = append(*h, value.(*semanticRunCursor)) }
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

func (c *hotSemanticRecordCursor) Record() ([]byte, []byte) { return c.iter.Key(), c.iter.Value() }
func (c *hotSemanticRecordCursor) Err() error               { return c.iter.Error() }
func (c *hotSemanticRecordCursor) Close() error             { return c.iter.Close() }
