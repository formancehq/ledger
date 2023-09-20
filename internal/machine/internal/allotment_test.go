package internal

import (
	"math/big"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAllocate(t *testing.T) {
	allotment, err := NewAllotment([]Portion{
		{Specific: big.NewRat(4, 5)},
		{Specific: big.NewRat(2, 25)},
		{Specific: big.NewRat(3, 25)},
	})
	require.NoError(t, err)

	parts := allotment.Allocate(NewMonetaryInt(15))
	expectedParts := []*MonetaryInt{NewMonetaryInt(13), NewMonetaryInt(1), NewMonetaryInt(1)}
	if len(parts) != len(expectedParts) {
		t.Fatalf("unexpected output %v != %v", parts, expectedParts)
	}
	for i := range parts {
		if !parts[i].Equal(expectedParts[i]) {
			t.Fatalf("unexpected output %v != %v", parts, expectedParts)
		}
	}
}

func TestAllocateEmptyRemainder(t *testing.T) {
	allotment, err := NewAllotment([]Portion{
		{Specific: big.NewRat(1, 2)},
		{Specific: big.NewRat(1, 2)},
		{Remaining: true},
	})
	require.NoError(t, err)

	parts := allotment.Allocate(NewMonetaryInt(15))
	expectedParts := []*MonetaryInt{NewMonetaryInt(8), NewMonetaryInt(7), NewMonetaryInt(0)}
	if len(parts) != len(expectedParts) {
		t.Fatalf("unexpected output %v != %v", parts, expectedParts)
	}
	for i := range parts {
		if !parts[i].Equal(expectedParts[i]) {
			t.Fatalf("unexpected output %v != %v", parts, expectedParts)
		}
	}

}

func TestInvalidAllotments(t *testing.T) {
	_, err := NewAllotment([]Portion{
		{Remaining: true},
		{Specific: big.NewRat(2, 25)},
		{Remaining: true},
	})
	assert.Errorf(t, err, "allowed two remainings")

	_, err = NewAllotment([]Portion{
		{Specific: big.NewRat(1, 2)},
		{Specific: big.NewRat(1, 2)},
		{Specific: big.NewRat(1, 2)},
	})
	assert.Errorf(t, err, "allowed more than 100%")
}
