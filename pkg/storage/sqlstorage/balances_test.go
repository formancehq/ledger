package sqlstorage

import (
	"context"
	"os"
	"testing"

	"github.com/numary/ledger/pkg/storage"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/assert"
)

func TestBalances(t *testing.T) {
	d := NewDriver("sqlite", &sqliteDB{
		directory: os.TempDir(),
		dbName:    uuid.New(),
	})

	assert.NoError(t, d.Initialize(context.Background()))

	defer func(d *Driver, ctx context.Context) {
		assert.NoError(t, d.Close(ctx))
	}(d, context.Background())

	store, _, err := d.GetStore(context.Background(), "foo", true)
	assert.NoError(t, err)

	_, err = store.Initialize(context.Background())
	assert.NoError(t, err)

	t.Run("success balance", func(t *testing.T) {
		q := storage.BalancesQuery{
			Limit:   10,
			Filters: storage.BalancesQueryFilters{},
		}

		_, err := store.GetBalances(context.Background(), q)
		assert.NoError(t, err, "balance filter should not fail")
	})

	t.Run("success balance_operator", func(t *testing.T) {
		q := storage.BalancesQuery{
			Limit: 10,
			Filters: storage.BalancesQueryFilters{
				Address: "world",
			},
		}

		_, err := store.GetBalances(context.Background(), q)
		assert.NoError(t, err, "balance_operator filter should not fail")
	})
}

func TestArrayToAssetBalancet(t *testing.T) {
	t.Run("panic invalid balance value", func(t *testing.T) {
		var byteArray = []byte("{\"(USD,-TEST)\",\"(EUR,-250)\"}")

		assert.PanicsWithError(
			t, `error while converting balance value into map: expected integer`,

			func() {
				_ = arrayToAssetBalance(byteArray)
			}, "should have panicked")
	})

	t.Run("panic invalid balance value nil", func(t *testing.T) {
		var byteArray = []byte("{\"(USD,)\",\"(EUR,-250)\"}")

		assert.PanicsWithError(
			t, `error while converting balance value into map: expected integer`,

			func() {
				_ = arrayToAssetBalance(byteArray)
			}, "should have panicked")
	})

	t.Run("success", func(t *testing.T) {
		var byteArray = []byte(`{"(USD,50)","(EUR,-250)","(CAD,5000000)","(YAN,-8000)"}`)
		resultMap := arrayToAssetBalance(byteArray)

		assert.Equal(t, int64(50), resultMap["USD"])
		assert.Equal(t, int64(-250), resultMap["EUR"])
		assert.Equal(t, int64(5000000), resultMap["CAD"])
		assert.Equal(t, int64(-8000), resultMap["YAN"])
	})
}
