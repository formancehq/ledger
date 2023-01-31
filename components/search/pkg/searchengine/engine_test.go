package searchengine

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/aquasecurity/esquery"
	"github.com/numary/ledger/pkg/core"
	"github.com/stretchr/testify/assert"
)

func testEngine(t *testing.T) {
	ledger := "quickstart"
	insertTransaction(t, ledger, "transaction0", time.Now(),
		core.Transaction{
			TransactionData: core.TransactionData{
				Metadata: core.Metadata{
					"foo": json.RawMessage(`{"foo": "bar"}`),
				},
				Postings: core.Postings{
					{
						Source:      "world",
						Destination: "central:bank",
						Asset:       "USD",
						Amount:      core.NewMonetaryInt(100),
					},
				},
			},
		})

	q := NewSingleDocTypeSearch("TRANSACTION")
	q.WithLedgers(ledger)
	q.WithTerms("central:bank")
	response, err := q.Do(context.Background(), engine)
	assert.NoError(t, err)
	assert.Len(t, response.Items, 1)
}

func testMatchingAllFields(t *testing.T) {
	now := time.Now().Round(time.Second).UTC()
	insertTransaction(t, "quickstart", "transaction0", now.Add(-time.Minute),
		core.Transaction{
			TransactionData: core.TransactionData{
				Postings: core.Postings{
					{
						Source:      "world",
						Destination: "central_bank",
						Amount:      core.NewMonetaryInt(100),
						Asset:       "USD",
					},
				},
			},
		})
	insertTransaction(t, "quickstart", "transaction1", now, core.Transaction{})
	insertTransaction(t, "quickstart", "transaction2", now.Add(time.Minute), core.Transaction{})

	q := NewMultiDocTypeSearch()
	q.WithLedgers("quickstart")
	q.WithTerms("USD")

	response, err := q.Do(context.Background(), engine)
	assert.NoError(t, err)
	assert.Len(t, response["TRANSACTION"], 1)

	q = NewMultiDocTypeSearch()
	q.WithLedgers("quickstart")
	q.WithTerms("US")
	response, err = q.Do(context.Background(), engine)
	assert.NoError(t, err)
	assert.Len(t, response["TRANSACTION"], 1)
}

func testSort(t *testing.T) {
	now := time.Now().Round(time.Second).UTC()
	const count = 20
	for i := 0; i < count; i++ {
		insertTransaction(t, "quickstart",
			fmt.Sprintf("transaction%d", i),
			now.Add(time.Duration(i)*time.Minute), core.Transaction{})
	}

	q := NewSingleDocTypeSearch("TRANSACTION")
	q.WithLedgers("quickstart")
	q.WithPageSize(20)
	q.WithSort("txid", esquery.OrderAsc)

	_, err := openSearchClient.Indices.GetMapping()
	assert.NoError(t, err)

	response, err := q.Do(context.Background(), engine)
	assert.NoError(t, err)
	assert.Len(t, response.Items, count)
}

func testPagination(t *testing.T) {
	now := time.Now().Round(time.Second).UTC()

	for i := 0; i < 20; i++ {
		at := now.Add(time.Duration(i) * time.Minute)
		insertTransaction(t, "quickstart",
			fmt.Sprintf("transaction%d", i), at,
			core.Transaction{
				TransactionData: core.TransactionData{
					Timestamp: at,
				},
			})
	}

	searchAfter := []interface{}{}
	for i := 0; ; i++ {
		q := NewSingleDocTypeSearch("TRANSACTION")
		q.WithLedgers("quickstart")
		q.WithPageSize(5)
		q.WithSort("timestamp", esquery.OrderDesc)
		q.WithSearchAfter(searchAfter)

		_, err := openSearchClient.Indices.GetMapping()
		assert.NoError(t, err)

		response, err := q.Do(context.Background(), engine)
		assert.NoError(t, err)

		tx := core.Transaction{}
		assert.NoError(t, json.Unmarshal(response.Items[0], &tx))

		if i < 3 {
			assert.Len(t, response.Items, 5)
			assert.Equal(t, tx.Timestamp, now.Add(19*time.Minute).Add(-time.Duration(i)*5*time.Minute).UTC())
		} else {
			assert.Len(t, response.Items, 5)
			assert.Equal(t, tx.Timestamp, now.Add(19*time.Minute).Add(-time.Duration(i)*5*time.Minute).UTC())
			break
		}

		lastTx := core.Transaction{}
		assert.NoError(t, json.Unmarshal(response.Items[4], &lastTx))

		searchAfter = []interface{}{lastTx.Timestamp}
	}

}

func testMatchingSpecificField(t *testing.T) {
	now := time.Now().Round(time.Second).UTC()
	insertTransaction(t, "quickstart", "transaction0", now.Add(-time.Minute),
		core.Transaction{
			TransactionData: core.TransactionData{
				Timestamp: now.Add(-time.Minute),
				Postings: core.Postings{
					{
						Source:      "world",
						Destination: "central_bank",
						Amount:      core.NewMonetaryInt(100),
						Asset:       "USD",
					},
				},
			},
		})
	insertTransaction(t, "quickstart", "transaction1", now.Add(time.Minute),
		core.Transaction{
			TransactionData: core.TransactionData{
				Timestamp: now.Add(time.Minute),
				Postings: core.Postings{
					{
						Source:      "central_bank",
						Destination: "user:001",
						Amount:      core.NewMonetaryInt(1000),
						Asset:       "USD",
					},
					{
						Source:      "world",
						Destination: "central_bank",
						Amount:      core.NewMonetaryInt(10000),
						Asset:       "USD",
					},
				},
			},
		})

	type testCase struct {
		name          string
		term          string
		expectedCount int
	}

	testCases := []testCase{
		{
			name:          "equality-using-equal",
			term:          "amount=100",
			expectedCount: 1,
		},
		{
			name:          "greater-than-on-long",
			term:          "amount>500",
			expectedCount: 1,
		},
		{
			name:          "greater-than-on-date-millis",
			term:          fmt.Sprintf("timestamp>%d", now.UnixMilli()),
			expectedCount: 1,
		},
		{
			name:          "greater-than-on-date-rfc3339",
			term:          fmt.Sprintf("timestamp>%s", now.Format(time.RFC3339)),
			expectedCount: 1,
		},
		{
			name:          "lower-than",
			term:          "amount<5000",
			expectedCount: 2,
		},
		{
			name:          "greater-than-or-equal",
			term:          "amount>=1000",
			expectedCount: 1,
		},
		{
			name:          "lower-than-or-equal",
			term:          "amount<=100",
			expectedCount: 1,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {

			q := NewMultiDocTypeSearch()
			q.WithLedgers("quickstart")
			q.WithTerms(tc.term)

			response, err := q.Do(context.Background(), engine)
			assert.NoError(t, err)
			assert.Len(t, response["TRANSACTION"], tc.expectedCount)
		})
	}
}

func testUsingOrPolicy(t *testing.T) {
	now := time.Now().Round(time.Second).UTC()
	insertTransaction(t, "quickstart", "transaction0", now.Add(-time.Minute),
		core.Transaction{
			TransactionData: core.TransactionData{
				Postings: core.Postings{
					{
						Source:      "world",
						Destination: "central_bank1",
						Amount:      core.NewMonetaryInt(100),
						Asset:       "USD",
					},
				},
			},
		})
	insertTransaction(t, "quickstart", "transaction1", now.Add(time.Minute),
		core.Transaction{
			TransactionData: core.TransactionData{
				Postings: core.Postings{
					{
						Source:      "world",
						Destination: "central_bank2",
						Amount:      core.NewMonetaryInt(1000),
						Asset:       "USD",
					},
				},
			},
		})
	insertTransaction(t, "quickstart", "transaction2", now.Add(time.Minute),
		core.Transaction{
			TransactionData: core.TransactionData{
				Postings: core.Postings{
					{
						Source:      "world",
						Destination: "central_bank3",
						Amount:      core.NewMonetaryInt(1000),
						Asset:       "USD",
					},
				},
			},
		})

	q := NewSingleDocTypeSearch("TRANSACTION")
	q.WithLedgers("quickstart")
	q.WithTerms("destination=central_bank1", "destination=central_bank2")
	q.WithPolicy(TermPolicyOR)

	response, err := q.Do(context.Background(), engine)
	assert.NoError(t, err)
	assert.Len(t, response.Items, 2)
}

func testAssetDecimals(t *testing.T) {
	now := time.Now().Round(time.Second).UTC()
	insertTransaction(t, "quickstart", "transaction0", now.Add(-time.Minute),
		core.Transaction{
			TransactionData: core.TransactionData{
				Postings: core.Postings{
					{
						Source:      "world",
						Destination: "central_bank",
						Amount:      core.NewMonetaryInt(10045),
						Asset:       "USD/2",
					},
				},
			},
		})
	insertTransaction(t, "quickstart", "transaction1", now.Add(-time.Minute),
		core.Transaction{
			TransactionData: core.TransactionData{
				Postings: core.Postings{
					{
						Source:      "world",
						Destination: "central_bank",
						Amount:      core.NewMonetaryInt(1000),
						Asset:       "USD",
					},
				},
			},
		})

	type testCase struct {
		name          string
		term          string
		expectedCount int
	}

	testCases := []testCase{
		{
			name:          "colon",
			term:          "amount=100.45",
			expectedCount: 1,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {

			q := NewMultiDocTypeSearch()
			q.WithTerms(tc.term)
			q.WithLedgers("quickstart")

			response, err := q.Do(context.Background(), engine)
			assert.NoError(t, err)
			assert.Len(t, response["TRANSACTION"], tc.expectedCount)
		})
	}

}

func testSearchInTransactionMetadata(t *testing.T) {
	now := time.Now().Round(time.Second).UTC()
	metadata := core.Metadata{
		"Hello": "guys!",
		"John":  "Snow!",
	}
	insertTransaction(t, "quickstart", "transaction0", now,
		core.Transaction{
			TransactionData: core.TransactionData{
				Metadata: metadata,
			},
		})
	insertTransaction(t, "quickstart", "transaction1", now,
		core.Transaction{})

	q := NewMultiDocTypeSearch()
	q.WithTerms("John")
	response, err := q.Do(context.Background(), engine)
	assert.NoError(t, err)
	assert.Len(t, response["TRANSACTION"], 1)

	tx := core.Transaction{}
	assert.NoError(t, json.Unmarshal(response["TRANSACTION"][0], &tx))
	assert.Equal(t, metadata, tx.Metadata)
}

func testKeepOnlyLastDocument(t *testing.T) {
	now := time.Now().Round(time.Hour)
	for i := 0; i < 10; i++ {
		insertAccount(t, "quickstart", fmt.Sprintf("account%d", i), now, core.Account{
			Address: fmt.Sprintf("user:00%d", i),
		})
	}
	for i := 0; i < 20; i++ {
		insertTransaction(t, "quickstart", fmt.Sprintf("transaction%d", i), now.Add(2*time.Minute), core.Transaction{
			ID: uint64(i),
			TransactionData: core.TransactionData{
				Timestamp: now.Add(time.Hour),
			},
		})
	}

	q := NewMultiDocTypeSearch()
	q.WithPageSize(5)

	response, err := q.Do(context.Background(), engine)
	assert.NoError(t, err)
	assert.Len(t, response["TRANSACTION"], 5)
	assert.Len(t, response["ACCOUNT"], 5)
}
