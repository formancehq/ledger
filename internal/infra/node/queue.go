package node

import (
	"math"
	"slices"
)

// yExponential calculates y using an exponential curve.
// This produces more buckets for small values and fewer for large values.
// k controls the curve steepness (higher k = more granularity at small values).
func yExponential(x, xMax, yMax, k float64) float64 {
	if x < 0 || xMax <= 0 || k <= 0 {
		return math.NaN()
	}
	// Exponential formula: y = yMax * (e^(k*x/xMax) - 1) / (e^k - 1)
	// This starts slow and accelerates, giving more buckets for small values
	t := x / xMax
	return yMax * (math.Exp(k*t) - 1) / (math.Expm1(k))
}

func expBoundaries(numberOfBuckets, bucketMaxValue int) []float64 {
	ret := make([]float64, numberOfBuckets)
	// k=3 provides good granularity at small values while still covering the full range
	k := 3.0
	for i := range numberOfBuckets {
		ret[i] = math.Floor(yExponential(float64(i), float64(numberOfBuckets-1), float64(bucketMaxValue), k))
	}
	return slices.Compact(ret)
}
