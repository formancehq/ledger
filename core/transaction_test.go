package core

import (
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestHash(t *testing.T) {
	a := Transaction{
		ID: 0,
		Postings: []Posting{
			{
				Source:      "world",
				Destination: "users:001",
				Amount:      100,
				Asset:       "COIN",
			},
		},
	}

	b := Transaction{
		ID: 1,
		Postings: []Posting{
			{
				Source:      "world",
				Destination: "users:001",
				Amount:      100,
				Asset:       "COIN",
			},
		},
	}

	h1 := Hash(nil, &a)

	if h1 != "3d60910b8f0aab20d17e3e8aa71ca9fe54634fe03466ec7ca49822bc4c5cac7f" {
		t.Fail()
	}

	a.Hash = h1
	h2 := Hash(&a, &b)

	if h2 != "b604e920f4f0d20fd2a2b09038ab9fc21d5761f05cdbd33148000a3f2ab7e65c" {
		t.Fail()
	}
}

func TestReverseTransaction(t *testing.T) {
	tx := &Transaction{
		Postings: Postings{
			{
				Source:      "world",
				Destination: "users:001",
				Amount:      100,
				Asset:       "COIN",
			},
			{
				Source:      "users:001",
				Destination: "payments:001",
				Amount:      100,
				Asset:       "COIN",
			},
		},
		Reference: "foo",
	}

	expected := Transaction{
		Postings: Postings{
			{
				Source:      "payments:001",
				Destination: "users:001",
				Amount:      100,
				Asset:       "COIN",
			},
			{
				Source:      "users:001",
				Destination: "world",
				Amount:      100,
				Asset:       "COIN",
			},
		},
		Reference: "revert_foo",
	}

	if diff := cmp.Diff(expected, tx.Reverse()); diff != "" {
		t.Errorf("Reverse() mismatch (-want +got):\n%s", diff)
	}
}
