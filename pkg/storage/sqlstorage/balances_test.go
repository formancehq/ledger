package sqlstorage

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestArrayToAssetsBalances(t *testing.T) {
	t.Run("panic invalid balance value", func(t *testing.T) {
		var byteArray = []byte("{\"(USD,-TEST)\",\"(EUR,-250)\"}")

		assert.PanicsWithError(
			t, `error while converting balance value into map: expected integer`,

			func() {
				_ = arrayToAssetsBalances(byteArray)
			}, "should have panicked")
	})

	t.Run("panic invalid balance value nil", func(t *testing.T) {
		var byteArray = []byte("{\"(USD,)\",\"(EUR,-250)\"}")

		assert.PanicsWithError(
			t, `error while converting balance value into map: expected integer`,

			func() {
				_ = arrayToAssetsBalances(byteArray)
			}, "should have panicked")
	})

	t.Run("success", func(t *testing.T) {
		var byteArray = []byte(`{"(USD,50)","(EUR,-250)","(CAD,5000000)","(YAN,-8000)"}`)
		resultMap := arrayToAssetsBalances(byteArray)

		assert.Equal(t, int64(50), resultMap["USD"])
		assert.Equal(t, int64(-250), resultMap["EUR"])
		assert.Equal(t, int64(5000000), resultMap["CAD"])
		assert.Equal(t, int64(-8000), resultMap["YAN"])
	})
}
