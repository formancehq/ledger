package internal

import (
	"math/big"
	"strings"
	"testing"
)

func TestBetween0And1Inclusive(t *testing.T) {
	tests := []struct {
		in      string
		want    *big.Rat
		wantErr bool
	}{
		{
			in:   "0%",
			want: big.NewRat(0, 1),
		},
		{
			in:   "0.0%",
			want: big.NewRat(0, 1),
		},
		{
			in:   "0/1",
			want: big.NewRat(0, 1),
		},
		{
			in:   "0/25",
			want: big.NewRat(0, 1),
		},
		{
			in:   "0/100",
			want: big.NewRat(0, 1),
		},
		{
			in:   "1%",
			want: big.NewRat(1, 100),
		},
		{
			in:   "1/100",
			want: big.NewRat(1, 100),
		},
		{
			in:   "10/1000",
			want: big.NewRat(1, 100),
		},
		{
			in:   "50/100",
			want: big.NewRat(50, 100),
		},
		{
			in:   "50%",
			want: big.NewRat(50, 100),
		},
		{
			in:   "50.0%",
			want: big.NewRat(50, 100),
		},
		{
			in:   "1/1",
			want: big.NewRat(1, 1),
		},
		{
			in:   "100/100",
			want: big.NewRat(1, 1),
		},
		{
			in:   "100.0%",
			want: big.NewRat(1, 1),
		},
		{
			in:   "100%",
			want: big.NewRat(1, 1),
		},
		// Now for the failures. We don't check negative numbers in this test because
		// those are a parsing failure, not a range failure.
		{
			in:      "100.1%",
			wantErr: true,
		},
		{
			in:      "101%",
			wantErr: true,
		},
		{
			in:      "101/100",
			wantErr: true,
		},
		{
			in:      "2/1",
			wantErr: true,
		},
		{
			in:      "3/2",
			wantErr: true,
		},
	}

	for _, test := range tests {
		t.Run(test.in, func(t *testing.T) {
			got, err := ParsePortionSpecific(test.in)
			if test.wantErr {
				if err == nil {
					t.Fatal("should have errored")
				}
				if !strings.Contains(err.Error(), "between") {
					t.Fatal("wrong error")
				}
				return
			}
			if err != nil {
				t.Fatalf("ParsePortionSpecific(%q): %v", test.in, err)
			}
			if test.want.Cmp(got.Specific) != 0 {
				t.Fatalf("ParsePortionSpecific(%q) = %q, want %q", test.in, got, test.want)
			}
		})
	}
}

func TestInvalidFormat(t *testing.T) {
	_, err := ParsePortionSpecific("this is not a portion")
	if err == nil {
		t.Fatal("should have errored")
	}
	if !strings.Contains(err.Error(), "format") {
		t.Fatal("wrong error")
	}
}
