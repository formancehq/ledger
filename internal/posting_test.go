package ledger

import (
	"math/big"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReverseMultiple(t *testing.T) {
	p := Postings{
		{
			Source:      "world",
			Destination: "users:001",
			Amount:      big.NewInt(100),
			Asset:       "COIN",
		},
		{
			Source:      "users:001",
			Destination: "payments:001",
			Amount:      big.NewInt(100),
			Asset:       "COIN",
		},
	}

	expected := Postings{
		{
			Source:      "payments:001",
			Destination: "users:001",
			Amount:      big.NewInt(100),
			Asset:       "COIN",
		},
		{
			Source:      "users:001",
			Destination: "world",
			Amount:      big.NewInt(100),
			Asset:       "COIN",
		},
	}

	p.Reverse()
	require.Equal(t, expected, p)
}

func TestReverseSingle(t *testing.T) {
	p := Postings{
		{
			Source:      "world",
			Destination: "users:001",
			Amount:      big.NewInt(100),
			Asset:       "COIN",
		},
	}

	expected := Postings{
		{
			Source:      "users:001",
			Destination: "world",
			Amount:      big.NewInt(100),
			Asset:       "COIN",
		},
	}

	p.Reverse()
	require.Equal(t, expected, p)
}
