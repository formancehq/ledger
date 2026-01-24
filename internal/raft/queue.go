package raft

import (
	"math"
	"slices"
)

func yConcave(x, xMax, yMax, p float64) float64 {
	if x < 0 || xMax <= 0 || p <= 0 {
		return math.NaN()
	}
	t := math.Log(x+1) / math.Log(xMax+1)
	return yMax * math.Pow(t, p)
}

func logBoundaries(numberOfBuckets, bucketMaxValue int) []float64 {
	ret := make([]float64, numberOfBuckets)
	for i := range numberOfBuckets {
		ret[i] = math.Floor(yConcave(float64(i), float64(numberOfBuckets-1), float64(bucketMaxValue), 1))
	}
	return slices.Compact(ret)
}