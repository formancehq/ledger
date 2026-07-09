package check

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/big"
	"os"

	"github.com/cockroachdb/pebble/v2"
	"github.com/holiman/uint256"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// Pebble key prefixes for the replay store.
const (
	replayPrefixVolume      = 'V'
	replayPrefixMetadata    = 'M'
	replayPrefixTransaction = 'T'
)

// Metadata value encoding in the replay store:
// key: [replayPrefixMetadata][canonicalKey]
// value: [flag][optional marshaled MetadataValue]
const (
	metaFlagSet     = 0x00
	metaFlagDeleted = 0x01
)

// Transaction merge op tags — each Merge operand starts with one of these.
const (
	txOpFinalized  = 0x00 // [txOpFinalized][marshaledTransactionState] — output of a previous Finish
	txOpCreate     = 0x01 // [txOpCreate][uint64 seq][uint64 revertsTransaction][uint8 hasTimestamp][uint64 timestamp][uint32 metaLen][meta][uint32 postingsLen][postings]
	txOpRevertedBy = 0x02 // [txOpRevertedBy][uint64 revertTxId][uint8 hasRevertedAt][uint64 revertedAt]
	txOpSetMeta    = 0x03 // [txOpSetMeta][key\x00][marshaledMetadataValue]
	txOpDeleteMeta = 0x04 // [txOpDeleteMeta][key string]
)

// replayStore is a temporary Pebble DB that stores replay state (volumes,
// metadata, transactions) on disk instead of in memory. This prevents OOM
// with large datasets and uses Pebble merge operators to avoid read-modify-write
// during replay — all writes are append-only.
type replayStore struct {
	db      *pebble.DB
	tempDir string
}

func newReplayStore() (*replayStore, error) {
	dir, err := os.MkdirTemp("", "checker-replay-*")
	if err != nil {
		return nil, fmt.Errorf("creating temp dir: %w", err)
	}

	db, err := pebble.Open(dir, &pebble.Options{
		Logger:       dal.DiscardPebbleLogger(),
		DisableWAL:   true,
		MemTableSize: 16 << 20, // 16 MB
		Merger:       replayMerger,
	})
	if err != nil {
		_ = os.RemoveAll(dir)

		return nil, fmt.Errorf("opening temp pebble: %w", err)
	}

	return &replayStore{db: db, tempDir: dir}, nil
}

func (s *replayStore) Close() error {
	err := s.db.Close()
	_ = os.RemoveAll(s.tempDir)

	return err
}

// replayKey builds a prefixed key: [prefix][canonicalKey].
func replayKey(prefix byte, canonicalKey []byte) []byte {
	key := make([]byte, 1+len(canonicalKey))
	key[0] = prefix
	copy(key[1:], canonicalKey)

	return key
}

// addVolumeDelta merges a volume delta without reading existing state.
func (s *replayStore) AddVolumeDelta(canonicalKey []byte, inputDelta, outputDelta *big.Int) error {
	key := replayKey(replayPrefixVolume, canonicalKey)

	var u256Input, u256Output uint256.Int

	u256Input.SetFromBig(inputDelta)
	u256Output.SetFromBig(outputDelta)

	pair := &raftcmdpb.VolumePair{
		Input:  commonpb.NewUint256(&u256Input),
		Output: commonpb.NewUint256(&u256Output),
	}

	data, err := pair.MarshalVT()
	if err != nil {
		return fmt.Errorf("marshaling volume delta: %w", err)
	}

	return s.db.Merge(key, data, pebble.NoSync)
}

// getVolume reads the current accumulated volume for a canonical key.
// Returns nil if the key does not exist.
func (s *replayStore) GetVolume(canonicalKey []byte) (*raftcmdpb.VolumePair, error) {
	key := replayKey(replayPrefixVolume, canonicalKey)

	val, closer, err := s.db.Get(key)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return nil, nil
		}

		return nil, fmt.Errorf("reading volume: %w", err)
	}

	defer func() { _ = closer.Close() }()

	pair := &raftcmdpb.VolumePair{}
	if err := pair.UnmarshalVT(val); err != nil {
		return nil, fmt.Errorf("unmarshaling volume: %w", err)
	}

	return pair, nil
}

// deleteVolume removes a volume entry from the replay store.
func (s *replayStore) DeleteVolume(canonicalKey []byte) error {
	key := replayKey(replayPrefixVolume, canonicalKey)

	return s.db.Delete(key, pebble.NoSync)
}

// moveVolume transfers accumulated volume from oldKey to newKey:
// newVolume += oldVolume, then deletes oldKey.
// If oldKey has no volume, this is a no-op.
func (s *replayStore) MoveVolume(oldCanonicalKey, newCanonicalKey []byte) error {
	oldVol, err := s.GetVolume(oldCanonicalKey)
	if err != nil {
		return err
	}
	if oldVol == nil {
		return nil // nothing to move
	}

	// Add old volume to new key
	inBig := oldVol.GetInput().ToBigInt()
	outBig := oldVol.GetOutput().ToBigInt()

	if err := s.AddVolumeDelta(newCanonicalKey, inBig, outBig); err != nil {
		return err
	}

	return s.DeleteVolume(oldCanonicalKey)
}

// moveMetadata transfers a metadata entry from oldKey to newKey.
// If oldKey has no metadata, this is a no-op.
func (s *replayStore) MoveMetadata(oldCanonicalKey, newCanonicalKey []byte) error {
	oldKey := replayKey(replayPrefixMetadata, oldCanonicalKey)

	val, closer, err := s.db.Get(oldKey)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return nil // nothing to move
		}

		return fmt.Errorf("reading metadata for move: %w", err)
	}
	valCopy := append([]byte(nil), val...)
	_ = closer.Close()

	newKey := replayKey(replayPrefixMetadata, newCanonicalKey)
	if err := s.db.Set(newKey, valCopy, pebble.NoSync); err != nil {
		return err
	}

	return s.db.Delete(oldKey, pebble.NoSync)
}

// setMetadata stores a metadata value in the replay store (pure write).
func (s *replayStore) SetMetadata(canonicalKey []byte, value string) error {
	key := replayKey(replayPrefixMetadata, canonicalKey)

	data := make([]byte, 1+len(value))
	data[0] = metaFlagSet
	copy(data[1:], value)

	return s.db.Set(key, data, pebble.NoSync)
}

// deleteMetadata marks a metadata key as deleted in the replay store (pure write).
func (s *replayStore) DeleteMetadata(canonicalKey []byte) error {
	key := replayKey(replayPrefixMetadata, canonicalKey)

	return s.db.Set(key, []byte{metaFlagDeleted}, pebble.NoSync)
}

// CreateTransaction records a transaction creation op via merge (no read).
// A 1-byte presence flag distinguishes a nil timestamp from a real
// Timestamp{Data: 0} (Unix epoch) — the FSM persists the latter unchanged,
// so collapsing both to 0 would surface as a CheckStore mismatch.
func (s *replayStore) CreateTransaction(canonicalKey []byte, seq uint64, timestamp *commonpb.Timestamp, metadata map[string]*commonpb.MetadataValue, postings []*commonpb.Posting, revertsTransaction uint64) error {
	key := replayKey(replayPrefixTransaction, canonicalKey)

	var metaBytes []byte
	if len(metadata) > 0 {
		mm := &commonpb.MetadataMap{Values: metadata}

		var err error

		metaBytes, err = mm.MarshalVT()
		if err != nil {
			return fmt.Errorf("marshaling tx metadata: %w", err)
		}
	}

	var postingsBytes []byte
	if len(postings) > 0 {
		// Wrap in a Transaction so vtproto handles the repeated Posting
		// framing for us; only Postings is populated. The container fields
		// (id, timestamp, metadata) stay at zero — the merger only reads
		// GetPostings().
		container := &commonpb.Transaction{Postings: postings}

		var err error

		postingsBytes, err = container.MarshalVT()
		if err != nil {
			return fmt.Errorf("marshaling tx postings: %w", err)
		}
	}

	// [txOpCreate][uint64 seq][uint64 revertsTransaction][uint8 hasTimestamp]
	// [uint64 timestamp.Data][uint32 metaLen][metaBytes][uint32 postingsLen][postingsBytes]
	buf := make([]byte, 1+8+8+1+8+4+len(metaBytes)+4+len(postingsBytes))
	buf[0] = txOpCreate
	binary.BigEndian.PutUint64(buf[1:], seq)
	binary.BigEndian.PutUint64(buf[9:], revertsTransaction)
	if timestamp != nil {
		buf[17] = 1
		binary.BigEndian.PutUint64(buf[18:], timestamp.GetData())
	}

	off := 26
	binary.BigEndian.PutUint32(buf[off:], uint32(len(metaBytes)))
	off += 4
	copy(buf[off:], metaBytes)
	off += len(metaBytes)
	binary.BigEndian.PutUint32(buf[off:], uint32(len(postingsBytes)))
	off += 4
	copy(buf[off:], postingsBytes)

	return s.db.Merge(key, buf, pebble.NoSync)
}

// setRevertedBy records that a transaction was reverted via merge (no read).
func (s *replayStore) SetRevertedBy(canonicalKey []byte, revertTxID uint64, revertedAt *commonpb.Timestamp) error {
	key := replayKey(replayPrefixTransaction, canonicalKey)

	buf := make([]byte, 1+8+1+8)
	buf[0] = txOpRevertedBy
	binary.BigEndian.PutUint64(buf[1:], revertTxID)
	if revertedAt != nil {
		buf[9] = 1
		binary.BigEndian.PutUint64(buf[10:], revertedAt.GetData())
	}

	return s.db.Merge(key, buf, pebble.NoSync)
}

// SaveTxMetadata records a metadata upsert on a transaction via merge (no read).
func (s *replayStore) SaveTxMetadata(canonicalKey []byte, metadata map[string]*commonpb.MetadataValue) error {
	key := replayKey(replayPrefixTransaction, canonicalKey)

	for metaKey, metaValue := range metadata {
		var valueBytes []byte
		if metaValue != nil {
			var err error

			valueBytes, err = metaValue.MarshalVT()
			if err != nil {
				return fmt.Errorf("marshaling metadata value: %w", err)
			}
		}

		// [txOpSetMeta][key\x00][marshaledMetadataValue]
		buf := make([]byte, 1+len(metaKey)+1+len(valueBytes))
		buf[0] = txOpSetMeta
		copy(buf[1:], metaKey)
		buf[1+len(metaKey)] = 0x00
		copy(buf[1+len(metaKey)+1:], valueBytes)

		if err := s.db.Merge(key, buf, pebble.NoSync); err != nil {
			return err
		}
	}

	return nil
}

// deleteTxMetadata records a metadata deletion on a transaction via merge (no read).
func (s *replayStore) DeleteTxMetadata(canonicalKey []byte, metaKey string) error {
	key := replayKey(replayPrefixTransaction, canonicalKey)

	buf := make([]byte, 1+len(metaKey))
	buf[0] = txOpDeleteMeta
	copy(buf[1:], metaKey)

	return s.db.Merge(key, buf, pebble.NoSync)
}

// SetMetadataFieldType / RemoveMetadataFieldType are no-ops here: the schema
// lives on LedgerInfo, not in this attribute merge store. Verifying the schema
// projection in the checker (the invariant-8 gap) is left as a follow-up.
func (s *replayStore) SetMetadataFieldType(string, commonpb.TargetType, string, commonpb.MetadataType) error {
	return nil
}

func (s *replayStore) RemoveMetadataFieldType(string, commonpb.TargetType, string) error {
	return nil
}

// newPrefixIter creates a Pebble iterator scoped to a single prefix byte.
func (s *replayStore) newPrefixIter(prefix byte) (*pebble.Iterator, error) {
	return s.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte{prefix},
		UpperBound: []byte{prefix + 1},
	})
}

// --- Merge operators ---

// replayMerger dispatches to volume or transaction merger based on key prefix.
var replayMerger = &pebble.Merger{
	Name: "checker-replay",
	Merge: func(key, value []byte) (pebble.ValueMerger, error) {
		if len(key) == 0 {
			return nil, errors.New("empty key in merge")
		}

		switch key[0] {
		case replayPrefixVolume:
			return newVolumeMerger(value)
		case replayPrefixTransaction:
			return newTxMerger(value)
		default:
			return nil, fmt.Errorf("unexpected merge prefix: %c", key[0])
		}
	},
}

// --- Volume merger: additive accumulation ---

type volumeMerger struct {
	input  big.Int
	output big.Int
}

func newVolumeMerger(value []byte) (*volumeMerger, error) {
	m := &volumeMerger{}

	return m, m.add(value)
}

func (m *volumeMerger) add(value []byte) error {
	var pair raftcmdpb.VolumePair
	if err := pair.UnmarshalVT(value); err != nil {
		return fmt.Errorf("unmarshaling volume in merge: %w", err)
	}

	m.input.Add(&m.input, pair.GetInput().ToBigInt())
	m.output.Add(&m.output, pair.GetOutput().ToBigInt())

	return nil
}

func (m *volumeMerger) MergeNewer(value []byte) error { return m.add(value) }
func (m *volumeMerger) MergeOlder(value []byte) error { return m.add(value) }

func (m *volumeMerger) Finish(_ bool) ([]byte, io.Closer, error) {
	var u256In, u256Out uint256.Int

	u256In.SetFromBig(&m.input)
	u256Out.SetFromBig(&m.output)

	result := &raftcmdpb.VolumePair{
		Input:  commonpb.NewUint256(&u256In),
		Output: commonpb.NewUint256(&u256Out),
	}

	data, err := result.MarshalVT()
	if err != nil {
		return nil, nil, err
	}

	return data, nil, nil
}

// --- Transaction merger: ops-based replay ---

// txMerger accumulates transaction ops in order and resolves them in Finish.
type txMerger struct {
	ops [][]byte // each entry is a raw op: [tag][payload]
}

func newTxMerger(value []byte) (*txMerger, error) {
	m := &txMerger{}
	m.ops = append(m.ops, append([]byte(nil), value...))

	return m, nil
}

func (m *txMerger) MergeNewer(value []byte) error {
	m.ops = append(m.ops, append([]byte(nil), value...))

	return nil
}

func (m *txMerger) MergeOlder(value []byte) error {
	// Prepend — older ops go first.
	m.ops = append([][]byte{append([]byte(nil), value...)}, m.ops...)

	return nil
}

func (m *txMerger) Finish(_ bool) ([]byte, io.Closer, error) {
	state := &commonpb.TransactionState{}

	for _, op := range m.ops {
		if len(op) == 0 {
			continue
		}

		switch op[0] {
		case txOpCreate:
			// [txOpCreate][uint64 seq][uint64 revertsTransaction][uint8 hasTimestamp]
			// [uint64 timestamp.Data][uint32 metaLen][metaBytes][uint32 postingsLen][postingsBytes]
			const headerLen = 1 + 8 + 8 + 1 + 8 + 4
			if len(op) < headerLen {
				return nil, nil, fmt.Errorf("txOpCreate too short: %d bytes", len(op))
			}

			state.CreatedByLog = binary.BigEndian.Uint64(op[1:9])
			state.RevertsTransaction = binary.BigEndian.Uint64(op[9:17])

			if op[17] == 1 {
				state.Timestamp = &commonpb.Timestamp{Data: binary.BigEndian.Uint64(op[18:26])}
			}

			metaLen := int(binary.BigEndian.Uint32(op[26:30]))
			off := 30
			if len(op) < off+metaLen+4 {
				return nil, nil, fmt.Errorf("txOpCreate: metadata length %d overruns buffer of %d bytes", metaLen, len(op))
			}

			if metaLen > 0 {
				mm := &commonpb.MetadataMap{}
				if err := mm.UnmarshalVT(op[off : off+metaLen]); err != nil {
					return nil, nil, fmt.Errorf("unmarshaling create metadata: %w", err)
				}

				state.Metadata = mm.GetValues()
			}

			off += metaLen
			postingsLen := int(binary.BigEndian.Uint32(op[off : off+4]))
			off += 4
			if len(op) < off+postingsLen {
				return nil, nil, fmt.Errorf("txOpCreate: postings length %d overruns buffer of %d bytes", postingsLen, len(op))
			}

			if postingsLen > 0 {
				container := &commonpb.Transaction{}
				if err := container.UnmarshalVT(op[off : off+postingsLen]); err != nil {
					return nil, nil, fmt.Errorf("unmarshaling create postings: %w", err)
				}

				state.Postings = container.GetPostings()
			}

		case txOpRevertedBy:
			if len(op) < 9 {
				return nil, nil, fmt.Errorf("txOpRevertedBy too short: %d bytes", len(op))
			}

			state.RevertedByTransaction = binary.BigEndian.Uint64(op[1:9])

			if len(op) >= 18 && op[9] == 1 {
				state.RevertedAt = &commonpb.Timestamp{Data: binary.BigEndian.Uint64(op[10:18])}
			}

		case txOpSetMeta:
			// Wire format: [key\x00][marshaledMetadataValue]
			payload := op[1:]
			before, after, ok := bytes.Cut(payload, []byte{0x00})
			if !ok {
				return nil, nil, errors.New("txOpSetMeta missing null separator")
			}

			metaKey := string(before)
			valueBytes := after

			value := &commonpb.MetadataValue{}
			if len(valueBytes) > 0 {
				if err := value.UnmarshalVT(valueBytes); err != nil {
					return nil, nil, fmt.Errorf("unmarshaling set-meta op: %w", err)
				}
			}

			if state.Metadata == nil {
				state.Metadata = make(map[string]*commonpb.MetadataValue)
			}

			state.Metadata[metaKey] = value

		case txOpDeleteMeta:
			metaKey := string(op[1:])

			delete(state.GetMetadata(), metaKey)

		case txOpFinalized:
			// Re-ingesting a previously finalized state (from a prior compaction).
			if err := state.UnmarshalVT(op[1:]); err != nil {
				return nil, nil, fmt.Errorf("unmarshaling finalized tx state: %w", err)
			}

		default:
			return nil, nil, fmt.Errorf("unknown tx op tag: 0x%02x", op[0])
		}
	}

	data, err := state.MarshalVT()
	if err != nil {
		return nil, nil, err
	}

	// Prefix with txOpFinalized so a subsequent compaction can re-ingest this result.
	result := make([]byte, 1+len(data))
	result[0] = txOpFinalized
	copy(result[1:], data)

	return result, nil, nil
}
