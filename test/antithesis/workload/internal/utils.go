package internal

import (
	"context"
	"fmt"
	"math"
	"math/big"
	"math/rand"
	"net/http"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/antithesishq/antithesis-sdk-go/random"
	"github.com/formancehq/go-libs/v2/time"
	"github.com/formancehq/ledger/pkg/client"
	"github.com/formancehq/ledger/pkg/client/models/components"
	"github.com/formancehq/ledger/pkg/client/models/operations"
	"github.com/formancehq/ledger/pkg/client/retry"
)

type Details map[string]any

func RandomBigInt() *big.Int {
	v := random.GetRandom()
	ret := big.NewInt(0)
	ret.SetString(fmt.Sprintf("%d", v), 10)
	return ret
}

func AssertAlways(condition bool, message string, details map[string]any) bool {
	assert.Always(condition, message, details)
	return condition
}

func AssertAlwaysErrNil(err error, message string, details map[string]any) bool {
	return AssertAlways(err == nil, message, Details{
		"error":   fmt.Sprint(err),
		"details": details,
	})
}

func NewClient() *client.Formance {
	return client.New(
		client.WithServerURL("http://gateway:8080"),
		client.WithClient(&http.Client{
			Timeout: time.Minute,
		}),
		client.WithRetryConfig(retry.Config{
			Strategy: "backoff",
			Backoff: &retry.BackoffStrategy{
				InitialInterval: 200,
				Exponent:        1.5,
				MaxElapsedTime:  10_000,
			},
			RetryConnectionErrors: true,
		}),
	)
}

func IsServerError(httpMeta components.HTTPMetadata) bool {
	return httpMeta.Response.StatusCode >= 400 && httpMeta.Response.StatusCode < 600
}

func CreateLedger(ctx context.Context, client *client.Formance, name string, bucket string) error {
	_, err := client.Ledger.V2.CreateLedger(ctx, operations.V2CreateLedgerRequest{
		Ledger: name,
		V2CreateLedgerRequest: components.V2CreateLedgerRequest{
			Bucket: &bucket,
		},
	})

	return err
}

func ListLedgers(ctx context.Context, client *client.Formance) ([]string, error) {
	res, err := client.Ledger.V2.ListLedgers(ctx, operations.V2ListLedgersRequest{})
	if err != nil {
		return nil, err
	}

	ledgers := []string{}
	for _, ledger := range res.V2LedgerListResponse.Cursor.Data {
		ledgers = append(ledgers, ledger.Name)
	}

	return ledgers, nil
}

func GetRandomLedger(ctx context.Context, client *client.Formance) (string, error) {
	ledgers, err := ListLedgers(ctx, client)
	if err != nil {
		return "", err
	}

	if len(ledgers) == 0 {
		return "", fmt.Errorf("no ledgers found")
	}

	// Get a random index within the ledgers slice
	randomIndex := rand.Intn(len(ledgers))

	// Return the ledger at the random index
	return ledgers[randomIndex], nil
}

func RunTx(ctx context.Context, client *client.Formance, amount *big.Int, ledger string) error {
	orderID := fmt.Sprint(int64(math.Abs(float64(random.GetRandom()))))
	dest := fmt.Sprintf("orders:%s", orderID)

	_, err := client.Ledger.V2.CreateTransaction(ctx, operations.V2CreateTransactionRequest{
		V2PostTransaction: components.V2PostTransaction{
			Postings: []components.V2Posting{{
				Amount:      amount,
				Asset:       "USD/2",
				Destination: dest,
				Source:      "world",
			}},
		},
		Ledger: ledger,
	})
	return err
}
