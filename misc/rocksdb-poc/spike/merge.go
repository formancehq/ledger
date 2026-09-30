package spike

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math/big"

	"github.com/linxGnu/grocksdb"
)

// Key prefixes of the checker replay store.
const (
	ReplayPrefixVolume      byte = 'v'
	ReplayPrefixTransaction byte = 't'
)

// Transaction op tags. The real replay store carries create / revert /
// metadata ops; the spike keeps the shape (ordered tagged ops, a batch
// container for deferred folds) and collapses the payload to a string.
const (
	TxOpSet   byte = 1 // [TxOpSet][payload]: replaces the state
	TxOpAdd   byte = 2 // [TxOpAdd][payload]: appends to the state
	TxOpBatch byte = 9 // [TxOpBatch][u32 len][op]...: ordered ops, unresolved
)

// ReplayMergeOperator ports the Pebble replayMerger. Pebble folds operands
// through MergeNewer / MergeOlder then Finish(includesBase); RocksDB hands
// FullMerge the base value (nil when the key does not exist, which is
// definitive) and the operands oldest-first, and calls PartialMerge to
// pre-combine operands when the base is out of reach. The Pebble
// includesBase=false branch therefore becomes PartialMerge.
type ReplayMergeOperator struct{}

var _ grocksdb.PartialMerger = ReplayMergeOperator{}

func (ReplayMergeOperator) Name() string { return "checker-replay" }

func (ReplayMergeOperator) FullMerge(key, existing []byte, operands [][]byte) ([]byte, bool) {
	if len(key) == 0 {
		return nil, false
	}

	switch key[0] {
	case ReplayPrefixVolume:
		out, err := mergeVolumes(existing, operands)

		return out, err == nil
	case ReplayPrefixTransaction:
		out, err := resolveTx(existing, operands)

		return out, err == nil
	default:
		return nil, false
	}
}

func (ReplayMergeOperator) PartialMerge(key, left, right []byte) ([]byte, bool) {
	if len(key) == 0 {
		return nil, false
	}

	switch key[0] {
	case ReplayPrefixVolume:
		// Addition is associative: two operands fold into one.
		out, err := mergeVolumes(nil, [][]byte{left, right})

		return out, err == nil
	case ReplayPrefixTransaction:
		// Ops cannot be resolved without the base: re-emit them in order.
		var ops [][]byte
		if err := flattenTxOps([][]byte{left, right}, &ops); err != nil {
			return nil, false
		}

		return EncodeTxBatch(ops), true
	default:
		return nil, false
	}
}

// --- volumes: additive accumulation of (input, output) big integers ---

// EncodeVolume encodes a volume pair as [u32 len][input bytes][u32 len][output bytes].
func EncodeVolume(input, output *big.Int) []byte {
	in, out := input.Bytes(), output.Bytes()
	buf := make([]byte, 0, 8+len(in)+len(out))
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(in)))
	buf = append(buf, in...)
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(out)))

	return append(buf, out...)
}

// DecodeVolume is the inverse of EncodeVolume.
func DecodeVolume(v []byte) (input, output *big.Int, err error) {
	if len(v) < 4 {
		return nil, nil, errors.New("volume: short input length")
	}
	n := int(binary.BigEndian.Uint32(v))
	if len(v) < 4+n+4 {
		return nil, nil, errors.New("volume: short input")
	}
	input = new(big.Int).SetBytes(v[4 : 4+n])
	rest := v[4+n:]
	m := int(binary.BigEndian.Uint32(rest))
	if len(rest) < 4+m {
		return nil, nil, errors.New("volume: short output")
	}
	output = new(big.Int).SetBytes(rest[4 : 4+m])

	return input, output, nil
}

func mergeVolumes(existing []byte, operands [][]byte) ([]byte, error) {
	var in, out big.Int
	add := func(v []byte) error {
		i, o, err := DecodeVolume(v)
		if err != nil {
			return err
		}
		in.Add(&in, i)
		out.Add(&out, o)

		return nil
	}
	if existing != nil {
		if err := add(existing); err != nil {
			return nil, err
		}
	}
	for _, op := range operands {
		if err := add(op); err != nil {
			return nil, err
		}
	}

	return EncodeVolume(&in, &out), nil
}

// --- transactions: ordered ops resolved against the base ---

// EncodeTxBatch wraps ordered ops into a single deferred operand.
func EncodeTxBatch(ops [][]byte) []byte {
	buf := []byte{TxOpBatch}
	for _, op := range ops {
		buf = binary.BigEndian.AppendUint32(buf, uint32(len(op)))
		buf = append(buf, op...)
	}

	return buf
}

func flattenTxOps(raw [][]byte, dst *[][]byte) error {
	for _, op := range raw {
		if len(op) == 0 {
			return errors.New("tx: empty op")
		}
		if op[0] != TxOpBatch {
			*dst = append(*dst, op)

			continue
		}
		rest := op[1:]
		var inner [][]byte
		for len(rest) > 0 {
			if len(rest) < 4 {
				return errors.New("tx: short batch entry")
			}
			n := int(binary.BigEndian.Uint32(rest))
			rest = rest[4:]
			if len(rest) < n {
				return errors.New("tx: batch entry overrun")
			}
			inner = append(inner, rest[:n])
			rest = rest[n:]
		}
		if err := flattenTxOps(inner, dst); err != nil {
			return err
		}
	}

	return nil
}

func resolveTx(existing []byte, operands [][]byte) ([]byte, error) {
	var ops [][]byte
	if err := flattenTxOps(operands, &ops); err != nil {
		return nil, err
	}
	state := append([]byte(nil), existing...)
	for _, op := range ops {
		switch op[0] {
		case TxOpSet:
			state = append(state[:0], op[1:]...)
		case TxOpAdd:
			state = append(state, op[1:]...)
		default:
			return nil, fmt.Errorf("tx: unknown op %d", op[0])
		}
	}

	return state, nil
}
