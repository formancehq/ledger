package core

import (
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestReverseMultiple(t *testing.T) {
	p := Postings{
		{
			Source:      "world",
			Destination: "users:001",
			Amount:      NewMonetaryInt(100),
			Asset:       "COIN",
		},
		{
			Source:      "users:001",
			Destination: "payments:001",
			Amount:      NewMonetaryInt(100),
			Asset:       "COIN",
		},
	}

	expected := Postings{
		{
			Source:      "payments:001",
			Destination: "users:001",
			Amount:      NewMonetaryInt(100),
			Asset:       "COIN",
		},
		{
			Source:      "users:001",
			Destination: "world",
			Amount:      NewMonetaryInt(100),
			Asset:       "COIN",
		},
	}

	p.Reverse()

	if diff := cmp.Diff(expected, p); diff != "" {
		t.Errorf("Reverse() mismatch (-want +got):\n%s", diff)
	}
}

func TestReverseSingle(t *testing.T) {
	p := Postings{
		{
			Source:      "world",
			Destination: "users:001",
			Amount:      NewMonetaryInt(100),
			Asset:       "COIN",
		},
	}

	expected := Postings{
		{
			Source:      "users:001",
			Destination: "world",
			Amount:      NewMonetaryInt(100),
			Asset:       "COIN",
		},
	}

	p.Reverse()

	if diff := cmp.Diff(expected, p); diff != "" {
		t.Errorf("Reverse() mismatch (-want +got):\n%s", diff)
	}
}
