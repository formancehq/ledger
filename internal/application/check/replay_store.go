package check

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/big"
	"os"

	"github.com/cockroachdb/pebble"
	"github.com/holiman/uint256"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
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
	txOpCreate     = 0x01 // [txOpCreate][uint64 seq][marshaledMetadataSet]
	txOpRevertedBy = 0x02 // [txOpRevertedBy][uint64 revertTxId]
	txOpSetMeta    = 0x03 // [txOpSetMeta][marshaledMetadata]
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
func (s *replayStore) addVolumeDelta(canonicalKey []byte, inputDelta, outputDelta *big.Int) error {
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
func (s *replayStore) getVolume(canonicalKey []byte) (*raftcmdpb.VolumePair, error) {
	key := replayKey(replayPrefixVolume, canonicalKey)

	val, closer, err := s.db.Get(key)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return nil, nil
		}

		return nil, fmt.Errorf("reading volume: %w", err)
	}

	defer closer.Close()

	pair := &raftcmdpb.VolumePair{}
	if err := pair.UnmarshalVT(val); err != nil {
		return nil, fmt.Errorf("unmarshaling volume: %w", err)
	}

	return pair, nil
}

// deleteVolume removes a volume entry from the replay store.
func (s *replayStore) deleteVolume(canonicalKey []byte) error {
	key := replayKey(replayPrefixVolume, canonicalKey)

	return s.db.Delete(key, pebble.NoSync)
}

// setMetadata stores a metadata value in the replay store (pure write).
func (s *replayStore) setMetadata(canonicalKey []byte, value string) error {
	key := replayKey(replayPrefixMetadata, canonicalKey)

	data := make([]byte, 1+len(value))
	data[0] = metaFlagSet
	copy(data[1:], value)

	return s.db.Set(key, data, pebble.NoSync)
}

// deleteMetadata marks a metadata key as deleted in the replay store (pure write).
func (s *replayStore) deleteMetadata(canonicalKey []byte) error {
	key := replayKey(replayPrefixMetadata, canonicalKey)

	return s.db.Set(key, []byte{metaFlagDeleted}, pebble.NoSync)
}

// createTransaction records a transaction creation op via merge (no read).
func (s *replayStore) createTransaction(canonicalKey []byte, seq uint64, metadata *commonpb.MetadataSet) error {
	key := replayKey(replayPrefixTransaction, canonicalKey)

	var metaBytes []byte
	if metadata != nil {
		var err error

		metaBytes, err = metadata.MarshalVT()
		if err != nil {
			return fmt.Errorf("marshaling tx metadata: %w", err)
		}
	}

	// [txOpCreate][uint64 seq][metaBytes]
	buf := make([]byte, 1+8+len(metaBytes))
	buf[0] = txOpCreate
	binary.BigEndian.PutUint64(buf[1:], seq)
	copy(buf[9:], metaBytes)

	return s.db.Merge(key, buf, pebble.NoSync)
}

// setRevertedBy records that a transaction was reverted via merge (no read).
func (s *replayStore) setRevertedBy(canonicalKey []byte, revertTxID uint64) error {
	key := replayKey(replayPrefixTransaction, canonicalKey)

	buf := make([]byte, 1+8)
	buf[0] = txOpRevertedBy
	binary.BigEndian.PutUint64(buf[1:], revertTxID)

	return s.db.Merge(key, buf, pebble.NoSync)
}

// saveTxMetadata records a metadata upsert on a transaction via merge (no read).
func (s *replayStore) saveTxMetadata(canonicalKey []byte, metadata []*commonpb.Metadata) error {
	key := replayKey(replayPrefixTransaction, canonicalKey)

	for _, m := range metadata {
		data, err := m.MarshalVT()
		if err != nil {
			return fmt.Errorf("marshaling metadata entry: %w", err)
		}

		buf := make([]byte, 1+len(data))
		buf[0] = txOpSetMeta
		copy(buf[1:], data)

		if err := s.db.Merge(key, buf, pebble.NoSync); err != nil {
			return err
		}
	}

	return nil
}

// deleteTxMetadata records a metadata deletion on a transaction via merge (no read).
func (s *replayStore) deleteTxMetadata(canonicalKey []byte, metaKey string) error {
	key := replayKey(replayPrefixTransaction, canonicalKey)

	buf := make([]byte, 1+len(metaKey))
	buf[0] = txOpDeleteMeta
	copy(buf[1:], metaKey)

	return s.db.Merge(key, buf, pebble.NoSync)
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
			if len(op) < 9 {
				return nil, nil, fmt.Errorf("txOpCreate too short: %d bytes", len(op))
			}

			state.CreatedByLog = binary.BigEndian.Uint64(op[1:9])

			if len(op) > 9 {
				meta := &commonpb.MetadataSet{}
				if err := meta.UnmarshalVT(op[9:]); err != nil {
					return nil, nil, fmt.Errorf("unmarshaling create metadata: %w", err)
				}

				state.Metadata = meta
			}

		case txOpRevertedBy:
			if len(op) < 9 {
				return nil, nil, fmt.Errorf("txOpRevertedBy too short: %d bytes", len(op))
			}

			state.RevertedByTransaction = binary.BigEndian.Uint64(op[1:9])

		case txOpSetMeta:
			entry := &commonpb.Metadata{}
			if err := entry.UnmarshalVT(op[1:]); err != nil {
				return nil, nil, fmt.Errorf("unmarshaling set-meta op: %w", err)
			}

			if state.GetMetadata() == nil {
				state.Metadata = &commonpb.MetadataSet{}
			}

			found := false

			for i, existing := range state.GetMetadata().GetMetadata() {
				if existing.GetKey() == entry.GetKey() {
					state.Metadata.Metadata[i] = entry
					found = true

					break
				}
			}

			if !found {
				state.Metadata.Metadata = append(state.Metadata.Metadata, entry)
			}

		case txOpDeleteMeta:
			metaKey := string(op[1:])

			if state.GetMetadata() != nil {
				filtered := make([]*commonpb.Metadata, 0, len(state.GetMetadata().GetMetadata()))

				for _, existing := range state.GetMetadata().GetMetadata() {
					if existing.GetKey() != metaKey {
						filtered = append(filtered, existing)
					}
				}

				state.Metadata.Metadata = filtered
			}

		default:
			return nil, nil, fmt.Errorf("unknown tx op tag: 0x%02x", op[0])
		}
	}

	data, err := state.MarshalVT()
	if err != nil {
		return nil, nil, err
	}

	return data, nil, nil
}
