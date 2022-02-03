package core

import (
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestHash(t *testing.T) {
	a := Transaction{
		ID: 0,
		TransactionData: TransactionData{
			Postings: []Posting{
				{
					Source:      "world",
					Destination: "users:001",
					Amount:      100,
					Asset:       "COIN",
				},
			},
		},
	}

	b := Transaction{
		ID: 1,
		TransactionData: TransactionData{
			Postings: []Posting{
				{
					Source:      "world",
					Destination: "users:001",
					Amount:      100,
					Asset:       "COIN",
				},
			},
		},
	}

	h1 := Hash(nil, &a)

	if h1 != "57618464a7b23bf22a4fce5c5943e8677ace7197c6c90bd7ad684d0769708bb2" {
		t.Fail()
	}

	a.Hash = h1
	h2 := Hash(&a, &b)

	if h2 != "dbcced909e105f24754724d5c6da62a82aa88b05b85cc424baed51ad59cbbdd1" {
		t.Fail()
	}
}

func TestReverseTransaction(t *testing.T) {
	tx := &Transaction{
		TransactionData: TransactionData{
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
		},
	}

	expected := TransactionData{
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
