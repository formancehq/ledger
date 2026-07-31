package balancehistorystore

import (
	"encoding/binary"
	"errors"
	"math/big"
)

type cumulativeValue struct {
	input  *big.Int
	output *big.Int
}

func newCumulativeValue() cumulativeValue {
	return cumulativeValue{input: new(big.Int), output: new(big.Int)}
}

func (v cumulativeValue) clone() cumulativeValue {
	return cumulativeValue{
		input:  new(big.Int).Set(v.input),
		output: new(big.Int).Set(v.output),
	}
}

func (v *cumulativeValue) add(input, output *big.Int) {
	v.input.Add(v.input, input)
	v.output.Add(v.output, output)
}

func encodeCumulative(value cumulativeValue) []byte {
	input := value.input.Bytes()
	output := value.output.Bytes()

	encoded := make([]byte, 0, binary.MaxVarintLen64*2+len(input)+len(output))
	encoded = binary.AppendUvarint(encoded, uint64(len(input)))
	encoded = append(encoded, input...)
	encoded = binary.AppendUvarint(encoded, uint64(len(output)))
	encoded = append(encoded, output...)

	return encoded
}

func decodeCumulative(encoded []byte) (cumulativeValue, error) {
	inputLength, n := binary.Uvarint(encoded)
	if n <= 0 {
		return cumulativeValue{}, errors.New("invalid history input length")
	}
	encoded = encoded[n:]
	if inputLength > uint64(len(encoded)) {
		return cumulativeValue{}, errors.New("truncated history input value")
	}
	input := new(big.Int).SetBytes(encoded[:inputLength])
	encoded = encoded[inputLength:]

	outputLength, n := binary.Uvarint(encoded)
	if n <= 0 {
		return cumulativeValue{}, errors.New("invalid history output length")
	}
	encoded = encoded[n:]
	if outputLength > uint64(len(encoded)) {
		return cumulativeValue{}, errors.New("truncated history output value")
	}
	output := new(big.Int).SetBytes(encoded[:outputLength])
	encoded = encoded[outputLength:]
	if len(encoded) != 0 {
		return cumulativeValue{}, errors.New("unexpected trailing history value bytes")
	}

	return cumulativeValue{input: input, output: output}, nil
}
