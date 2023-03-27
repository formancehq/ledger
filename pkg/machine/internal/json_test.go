package internal

import (
	"encoding/json"
	"math/big"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAccountTypedJSON(t *testing.T) {
	j := json.RawMessage(`{
		"type": "account",
		"value": "users:001"
	}`)
	value, err := NewValueFromTypedJSON(j)
	require.NoError(t, err)

	if !ValueEquals(value, AccountAddress("users:001")) {
		t.Fatalf("unexpected value: %v", value)
	}
}

func TestAssetTypedJSON(t *testing.T) {
	j := json.RawMessage(`{
		"type": "asset",
		"value": "EUR/2"
	}`)
	value, err := NewValueFromTypedJSON(j)
	require.NoError(t, err)

	if !ValueEquals(value, Asset("EUR/2")) {
		t.Fatalf("unexpected value: %v", value)
	}
}

func TestNumberTypedJSON(t *testing.T) {
	j := json.RawMessage(`{
		"type": "number",
		"value": 89849865111111111111111111111111111555555555555555555555555555555555555555555555555999999999999999999999
	}`)
	value, err := NewValueFromTypedJSON(j)
	require.NoError(t, err)

	num, err := ParseNumber("89849865111111111111111111111111111555555555555555555555555555555555555555555555555999999999999999999999")
	require.NoError(t, err)

	if !ValueEquals(value, num) {
		t.Fatalf("unexpected value: %v", value)
	}
}

func TestMonetaryTypedJSON(t *testing.T) {
	j := json.RawMessage(`{
		"type": "monetary",
		"value": {
			"asset": "EUR/2",
			"amount": 123456
		}
	}`)
	value, err := NewValueFromTypedJSON(j)
	require.NoError(t, err)

	if !ValueEquals(value, Monetary{
		Asset:  "EUR/2",
		Amount: NewMonetaryInt(123456),
	}) {
		t.Fatalf("unexpected value: %v", value)
	}
}

func TestPortionTypedJSON(t *testing.T) {
	j := json.RawMessage(`{
		"type": "portion",
		"value": "90%"
	}`)
	value, err := NewValueFromTypedJSON(j)
	require.NoError(t, err)

	portion, err := NewPortionSpecific(*big.NewRat(90, 100))
	require.NoError(t, err)

	if !ValueEquals(value, *portion) {
		t.Fatalf("unexpected value: %v", value)
	}
}

func TestInvalidTypedJSON(t *testing.T) {
	j := json.RawMessage(`{
		"value": {
			"asset": "EUR/2",
			"amount": 123456
		}
	}`)
	_, err := NewValueFromTypedJSON(j)
	require.Error(t, err)
}

func TestMarshalJSON(t *testing.T) {
	t.Run("account", func(t *testing.T) {
		by, err := json.Marshal(AccountAddress("platform"))
		require.NoError(t, err)
		assert.Equal(t, `"platform"`, string(by))
	})
	t.Run("asset", func(t *testing.T) {
		by, err := json.Marshal(Asset("COIN"))
		require.NoError(t, err)
		assert.Equal(t, `"COIN"`, string(by))
	})
	t.Run("number", func(t *testing.T) {
		by, err := json.Marshal(
			Number(big.NewInt(42)))
		require.NoError(t, err)
		assert.Equal(t, `42`, string(by))
	})
	t.Run("string", func(t *testing.T) {
		by, err := json.Marshal(String("test"))
		require.NoError(t, err)
		assert.Equal(t, `"test"`, string(by))
	})
	t.Run("monetary", func(t *testing.T) {
		by, err := json.Marshal(
			Monetary{
				Asset:  "COIN",
				Amount: NewMonetaryInt(42),
			})
		require.NoError(t, err)
		assert.Equal(t, `{"asset":"COIN","amount":42}`, string(by))
	})
	t.Run("portion", func(t *testing.T) {
		by, err := json.Marshal(
			Portion{
				Remaining: true,
				Specific:  big.NewRat(10, 12),
			})
		require.NoError(t, err)
		assert.Equal(t, `{"remaining":true,"specific":"5/6"}`, string(by))
	})
}
