package state

import (
	"encoding/binary"
	"fmt"

	"github.com/cockroachdb/pebble/v2"
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// 0xFF cache value format (lean):
//
//	[8-byte tag LE][raw value proto bytes]
//
// The tag is the U128-collision fingerprint from the attribute system.
// The raw value bytes are the same proto bytes written to the 0xF1 attributes zone.
// attr.Set returns the marshaled bytes, avoiding a second marshal.

// fillCacheKey builds a 0xFF cache key on the stack and returns it.
// Format: [0xFF][genByte][cacheType][16-byte U128] = 19 bytes.
func fillCacheKey(genByte, cacheType byte, id attributes.U128) [3 + 16]byte {
	var buf [3 + 16]byte
	buf[0] = dal.ZoneCache
	buf[1] = genByte
	buf[2] = cacheType
	copy(buf[3:], id[:])

	return buf
}

// writeCacheRaw writes a [tag][valueBytes] entry to the 0xFF zone.
// Uses batch.CacheBuffer to avoid allocating per call — Pebble copies
// the value into its repr buffer, so reuse is safe.
func writeCacheRaw(batch *dal.WriteSession, genByte, cacheType byte, id attributes.U128, tag uint64, valueBytes []byte) error {
	needed := 8 + len(valueBytes)
	if cap(batch.CacheBuffer) >= needed {
		batch.CacheBuffer = batch.CacheBuffer[:needed]
	} else {
		batch.CacheBuffer = make([]byte, needed)
	}

	binary.LittleEndian.PutUint64(batch.CacheBuffer, tag)
	copy(batch.CacheBuffer[8:], valueBytes)

	key := fillCacheKey(genByte, cacheType, id)

	return batch.Set(key[:], batch.CacheBuffer, pebble.NoSync)
}

// writeCacheTombstone writes a tombstone (tag + empty value bytes) to both
// gen bytes in 0xFF, matching AttributeCache.Del's tombstone semantic.
// On restore, empty value bytes signal a tombstone entry.
func writeCacheTombstone(batch *dal.WriteSession, cacheType byte, id attributes.U128, tag uint64) error {
	for _, genByte := range []byte{0, 1} {
		if err := writeCacheRaw(batch, genByte, cacheType, id, tag, nil); err != nil {
			return fmt.Errorf("writing cache tombstone: %w", err)
		}
	}

	return nil
}

// mergeSimpleWithCache writes attribute updates to the 0xF1 zone via attr.Set,
// then immediately writes the lean [tag][valueBytes] to the 0xFF cache zone.
// This avoids marshaling the value proto twice.
func mergeSimpleWithCache[K attributes.Key, V proto.Message](
	attr *attributes.Attribute[V],
	batch *dal.WriteSession,
	genByte byte,
	cacheType byte,
	updates []attributes.Update[K, V],
) error {
	for _, u := range updates {
		valueBytes, err := attr.Set(batch, u.CanonicalKey, u.New)
		if err != nil {
			return err
		}

		if err := writeCacheRaw(batch, genByte, cacheType, u.ID, u.Tag, valueBytes); err != nil {
			return err
		}
	}

	return nil
}

// writeCacheRotation writes the 0xFF metadata and purges old gen1 data on a cache generation rotation.
// Must be called AFTER CheckRotationNeeded (which performs the in-memory rotation),
// so CurrentGeneration() and BaseIndex reflect the post-rotation state.
func writeCacheRotation(batch *dal.WriteSession, currentGeneration uint64, baseIndexGen0, baseIndexGen1 uint64) error {
	newGenByte := byte(currentGeneration % 2)
	gen1Byte := byte((currentGeneration + 1) % 2)

	// 1. Purge old gen1 data (same byte position as new gen0).
	clearStart := []byte{dal.ZoneCache, newGenByte}
	clearEnd := []byte{dal.ZoneCache, newGenByte + 1}

	if err := batch.DeleteRangeNoSync(clearStart, clearEnd); err != nil {
		return fmt.Errorf("clearing old gen1 cache data: %w", err)
	}

	// 2. Global cache snapshot meta.
	if err := batch.SetProto(
		[]byte{dal.ZoneCache, dal.SubCacheMeta},
		&raftcmdpb.CacheSnapshotMeta{CurrentGeneration: currentGeneration},
	); err != nil {
		return fmt.Errorf("updating cache snapshot meta: %w", err)
	}

	// 3. New gen0 metadata (empty generation).
	if err := batch.SetProto(
		[]byte{dal.ZoneCache, newGenByte, dal.SubCacheGenMeta},
		&raftcmdpb.CacheGenerationMeta{BaseIndex: baseIndexGen0},
	); err != nil {
		return fmt.Errorf("updating gen0 meta: %w", err)
	}

	// 4. Gen1 metadata (old gen0 entries, now at gen1Byte).
	if err := batch.SetProto(
		[]byte{dal.ZoneCache, gen1Byte, dal.SubCacheGenMeta},
		&raftcmdpb.CacheGenerationMeta{BaseIndex: baseIndexGen1},
	); err != nil {
		return fmt.Errorf("updating gen1 meta: %w", err)
	}

	return nil
}
