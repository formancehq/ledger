package ledgerpb

import (
	"fmt"
	"math/big"
)

// Totals decodes the input and output totals.
func (v *AggregatedVolume) Totals() (input, output *big.Int, err error) {
	input, err = v.GetInput().ToBigInt()
	if err != nil {
		return nil, nil, fmt.Errorf("aggregated %s input: %w", v.GetAsset(), err)
	}

	output, err = v.GetOutput().ToBigInt()
	if err != nil {
		return nil, nil, fmt.Errorf("aggregated %s output: %w", v.GetAsset(), err)
	}

	return input, output, nil
}
