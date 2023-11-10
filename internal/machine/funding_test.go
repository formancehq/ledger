package machine

import (
	"testing"
)

func TestFundingTake(t *testing.T) {
	f := Funding{
		Asset: "COIN",
		Parts: []FundingPart{
			{
				Account: "aaa",
				Amount:  NewMonetaryInt(70),
			},
			{
				Account: "bbb",
				Amount:  NewMonetaryInt(30),
			},
			{
				Account: "ccc",
				Amount:  NewMonetaryInt(50),
			},
		},
	}
	result, remainder, err := f.Take(NewMonetaryInt(80))
	if err != nil {
		t.Fatal(err)
	}
	expectedResult := Funding{
		Asset: "COIN",
		Parts: []FundingPart{
			{
				Account: "aaa",
				Amount:  NewMonetaryInt(70),
			},
			{
				Account: "bbb",
				Amount:  NewMonetaryInt(10),
			},
		},
	}
	if !ValueEquals(result, expectedResult) {
		t.Fatalf("unexpected result: %v", result)
	}
	expectedRemainder := Funding{
		Asset: "COIN",
		Parts: []FundingPart{
			{
				Account: "bbb",
				Amount:  NewMonetaryInt(20),
			},
			{
				Account: "ccc",
				Amount:  NewMonetaryInt(50),
			},
		},
	}
	if !ValueEquals(remainder, expectedRemainder) {
		t.Fatalf("unexpected remainder: %v", remainder)
	}
}

func TestFundingTakeMaxUnder(t *testing.T) {
	f := Funding{
		Asset: "COIN",
		Parts: []FundingPart{
			{
				Account: "aaa",
				Amount:  NewMonetaryInt(30),
			},
		},
	}
	result, remainder := f.TakeMax(NewMonetaryInt(80))
	if !ValueEquals(result, Funding{
		Asset: "COIN",
		Parts: []FundingPart{
			{
				Account: "aaa",
				Amount:  NewMonetaryInt(30),
			},
		},
	}) {
		t.Fatalf("unexpected result: %v", result)
	}
	if !ValueEquals(remainder, Funding{
		Asset: "COIN",
	}) {
		t.Fatalf("unexpected remainder: %v", remainder)
	}
}

func TestFundingTakeMaxAbove(t *testing.T) {
	f := Funding{
		Asset: "COIN",
		Parts: []FundingPart{
			{
				Account: "aaa",
				Amount:  NewMonetaryInt(90),
			},
		},
	}
	result, remainder := f.TakeMax(NewMonetaryInt(80))
	if !ValueEquals(result, Funding{
		Asset: "COIN",
		Parts: []FundingPart{
			{
				Account: "aaa",
				Amount:  NewMonetaryInt(80),
			},
		},
	}) {
		t.Fatalf("unexpected result: %v", result)
	}
	if !ValueEquals(remainder, Funding{
		Asset: "COIN",
		Parts: []FundingPart{
			{
				Account: "aaa",
				Amount:  NewMonetaryInt(10),
			},
		},
	}) {
		t.Fatalf("unexpected remainder: %v", remainder)
	}
}

func TestFundingReversal(t *testing.T) {
	f := Funding{
		Asset: "COIN",
		Parts: []FundingPart{
			{
				Account: "aaa",
				Amount:  NewMonetaryInt(10),
			},
			{
				Account: "bbb",
				Amount:  NewMonetaryInt(20),
			},
			{
				Account: "ccc",
				Amount:  NewMonetaryInt(30),
			},
		},
	}
	rev := f.Reverse()
	if !ValueEquals(rev, Funding{
		Asset: "COIN",
		Parts: []FundingPart{
			{
				Account: "ccc",
				Amount:  NewMonetaryInt(30),
			},
			{
				Account: "bbb",
				Amount:  NewMonetaryInt(20),
			},
			{
				Account: "aaa",
				Amount:  NewMonetaryInt(10),
			},
		},
	}) {
		t.Fatalf("unexpected result: %v", rev)
	}
}
