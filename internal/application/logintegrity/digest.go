package logintegrity

import (
	"context"
	"encoding/binary"
	"fmt"

	"github.com/cockroachdb/pebble/v2"
	"github.com/zeebo/blake3"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

const (
	// DigestVersion identifies both the canonical Log encoding and the range framing.
	DigestVersion = 1
	// MaxRangeEntries bounds the work and the lifetime of a pinned Pebble snapshot.
	MaxRangeEntries = 4096
	digestDomain    = "ledger-log-range:v1\x00"
)

// RangeDigest covers every log key in the inclusive range. A missing or malformed
// row is an error, not a digest of a shorter range.
type RangeDigest struct {
	First   uint64
	Last    uint64
	Version uint32
	Hash    [32]byte
}

// MissingLogError reports a hole at a sequence that the caller expects to be
// durably present. Callers must establish the Raft apply horizon first: a
// lagging follower can legitimately have a shorter log stream.
type MissingLogError struct{ Sequence uint64 }

func (e *MissingLogError) Error() string {
	return fmt.Sprintf("log %d is missing", e.Sequence)
}

// LastLogSequence reads the sequence from the last persisted log key. The key
// is authoritative here: a corrupt value must not make the node appear behind.
func LastLogSequence(reader dal.PebbleReader) (uint64, error) {
	iter, err := reader.NewIter(&pebble.IterOptions{
		LowerBound: []byte{dal.ZoneHistory, dal.SubHistoryLog},
		UpperBound: dal.ZonePrefixUpperBound(dal.ZoneHistory, dal.SubHistoryLog),
	})
	if err != nil {
		return 0, fmt.Errorf("opening last log iterator: %w", err)
	}
	defer func() {
		// Close releases the iterator; scan errors are checked via Error.
		_ = iter.Close()
	}()

	if !iter.Last() {
		return 0, iter.Error()
	}
	key := iter.Key()
	if len(key) != 10 {
		return 0, fmt.Errorf("malformed last log key of %d bytes", len(key))
	}

	return binary.BigEndian.Uint64(key[2:]), nil
}

// DigestRange hashes the canonical, persisted Log values for [first, last].
// It reads only the history-log zone through the caller's pinned reader. The
// result is comparable across nodes at the same durable Raft horizon.
func DigestRange(ctx context.Context, reader dal.PebbleReader, first, last uint64) (RangeDigest, error) {
	if first == 0 || last < first || last-first >= MaxRangeEntries {
		return RangeDigest{}, fmt.Errorf("invalid log digest range [%d,%d]: require 1 <= first <= last and at most %d entries", first, last, MaxRangeEntries)
	}

	start := dal.NewKeyBuilder().PutZonePrefix(dal.ZoneHistory, dal.SubHistoryLog).PutUint64(first).Build()
	iter, err := reader.NewIter(&pebble.IterOptions{
		LowerBound: start,
		UpperBound: dal.ZonePrefixUpperBound(dal.ZoneHistory, dal.SubHistoryLog),
	})
	if err != nil {
		return RangeDigest{}, fmt.Errorf("opening log digest iterator: %w", err)
	}
	defer func() {
		// Close releases the iterator; scan errors are checked via Error.
		_ = iter.Close()
	}()

	h := blake3.New()
	// hash.Hash.Write does not return an error.
	_, _ = h.WriteString(digestDomain)
	var word [8]byte
	binary.BigEndian.PutUint64(word[:], first)
	_, _ = h.Write(word[:])
	binary.BigEndian.PutUint64(word[:], last)
	_, _ = h.Write(word[:])

	valid := iter.First()
	for seq := first; ; seq++ {
		if err := ctx.Err(); err != nil {
			return RangeDigest{}, err
		}
		if !valid {
			if err := iter.Error(); err != nil {
				return RangeDigest{}, fmt.Errorf("reading log digest range: %w", err)
			}

			return RangeDigest{}, &MissingLogError{Sequence: seq}
		}

		key := iter.Key()
		if len(key) != 10 {
			return RangeDigest{}, fmt.Errorf("malformed log key of %d bytes at sequence %d", len(key), seq)
		}
		keySeq := binary.BigEndian.Uint64(key[2:])
		if keySeq != seq {
			return RangeDigest{}, &MissingLogError{Sequence: seq}
		}

		log := new(commonpb.Log)
		if err := log.UnmarshalVT(iter.Value()); err != nil {
			return RangeDigest{}, fmt.Errorf("decoding log %d: %w", seq, err)
		}
		if log.GetSequence() != seq {
			return RangeDigest{}, fmt.Errorf("log at key %d carries sequence %d", seq, log.GetSequence())
		}

		// This value came directly from Pebble. Include every persisted field;
		// response signatures are normally added only to returned copies.
		payload := log.MarshalDeterministicVT(nil)
		binary.BigEndian.PutUint64(word[:], seq)
		_, _ = h.Write(word[:])
		binary.BigEndian.PutUint64(word[:], uint64(len(payload)))
		_, _ = h.Write(word[:])
		_, _ = h.Write(payload)

		if seq == last {
			break
		}
		valid = iter.Next()
	}
	if err := iter.Error(); err != nil {
		return RangeDigest{}, fmt.Errorf("reading log digest range: %w", err)
	}

	result := RangeDigest{First: first, Last: last, Version: DigestVersion}
	copy(result.Hash[:], h.Sum(nil))

	return result, nil
}
