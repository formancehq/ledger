package core

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLog(t *testing.T) {

	d := time.Unix(1648542028, 0).UTC()

	log1 := NewTransactionLogWithDate(nil, Transaction{
		TransactionData: TransactionData{
			Metadata: Metadata{},
		},
	}, d)
	log2 := NewTransactionLogWithDate(&log1, Transaction{
		TransactionData: TransactionData{
			Metadata: Metadata{},
		},
	}, d)
	if !assert.Equal(t, "3070ef3437354b5cb5ece914f8610d8d1276c6a9df127c0d2a49c48e3f81b017", log2.Hash) {
		return
	}
}

func TestLogProcessor(t *testing.T) {

	inputs := []any{
		Transaction{
			TransactionData: TransactionData{
				Postings: []Posting{
					{
						Source:      "world",
						Destination: "orders:1234",
						Amount:      100,
						Asset:       "USD",
					},
				},
				Metadata: Metadata{},
			},
			ID: 0,
		},
		Transaction{
			TransactionData: TransactionData{
				Postings: []Posting{
					{
						Source:      "orders:1234",
						Destination: "merchant:1234",
						Amount:      90,
						Asset:       "USD",
					},
					{
						Source:      "orders:1234",
						Destination: "fees",
						Amount:      10,
						Asset:       "USD",
					},
				},
				Metadata: Metadata{},
			},
			ID: 1,
		},
		SetMetadata{
			TargetType: MetaTargetTypeTransaction,
			TargetID:   0,
			Metadata: Metadata{
				"psp-ref": json.RawMessage(`"#ABCDEF"`),
			},
		},
		SetMetadata{
			TargetType: MetaTargetTypeAccount,
			TargetID:   "orders:1234",
			Metadata: Metadata{
				"booking-online": json.RawMessage(`true`),
			},
		},
	}

	p := NewLogProcessor()
	var previousLog *Log
	for _, input := range inputs {
		var log Log
		switch ob := input.(type) {
		case Transaction:
			log = NewTransactionLog(previousLog, ob)
		case SetMetadata:
			log = NewSetMetadataLog(previousLog, time.Now().Truncate(time.Second).UTC(), ob)
		}
		p.ProcessNextLog(log)
		previousLog = &log
	}

	require.Equal(t, []*ExpandedTransaction{
		{
			Transaction: Transaction{
				TransactionData: TransactionData{
					Postings: []Posting{
						{
							Source:      "world",
							Destination: "orders:1234",
							Amount:      100,
							Asset:       "USD",
						},
					},
					Metadata: Metadata{
						"psp-ref": json.RawMessage(`"#ABCDEF"`),
					},
				},
				ID: 0,
			},
			PreCommitVolumes: AccountsAssetsVolumes{
				"world": {
					"USD": {
						Input:  0,
						Output: 0,
					},
				},
				"orders:1234": {
					"USD": {
						Input:  0,
						Output: 0,
					},
				},
			},
			PostCommitVolumes: AccountsAssetsVolumes{
				"world": {
					"USD": {
						Input:  0,
						Output: 100,
					},
				},
				"orders:1234": {
					"USD": {
						Input:  100,
						Output: 0,
					},
				},
			},
		},
		{
			Transaction: Transaction{
				TransactionData: TransactionData{
					Postings: []Posting{
						{
							Source:      "orders:1234",
							Destination: "merchant:1234",
							Amount:      90,
							Asset:       "USD",
						},
						{
							Source:      "orders:1234",
							Destination: "fees",
							Amount:      10,
							Asset:       "USD",
						},
					},
					Metadata: Metadata{},
				},
				ID: 1,
			},
			PreCommitVolumes: AccountsAssetsVolumes{
				"orders:1234": {
					"USD": {
						Input:  100,
						Output: 0,
					},
				},
				"merchant:1234": {
					"USD": {
						Input:  0,
						Output: 0,
					},
				},
				"fees": {
					"USD": {
						Input:  0,
						Output: 0,
					},
				},
			},
			PostCommitVolumes: AccountsAssetsVolumes{
				"orders:1234": {
					"USD": {
						Input:  100,
						Output: 100,
					},
				},
				"merchant:1234": {
					"USD": {
						Input:  90,
						Output: 0,
					},
				},
				"fees": {
					"USD": {
						Input:  10,
						Output: 0,
					},
				},
			},
		},
	}, p.Transactions)
	require.Equal(t, AccountsAssetsVolumes{
		"world": {
			"USD": {
				Input:  0,
				Output: 100,
			},
		},
		"orders:1234": {
			"USD": {
				Input:  100,
				Output: 100,
			},
		},
		"merchant:1234": {
			"USD": {
				Input:  90,
				Output: 0,
			},
		},
		"fees": {
			"USD": {
				Input:  10,
				Output: 0,
			},
		},
	}, p.Volumes)
	require.Equal(t, Accounts{
		"world": {
			Address:  "world",
			Metadata: Metadata{},
		},
		"orders:1234": {
			Address: "orders:1234",
			Metadata: Metadata{
				"booking-online": json.RawMessage(`true`),
			},
		},
		"merchant:1234": {
			Address:  "merchant:1234",
			Metadata: Metadata{},
		},
		"fees": {
			Address:  "fees",
			Metadata: Metadata{},
		},
	}, p.Accounts)

}
