package internal

import (
	"context"
	"fmt"
	"math"
	"math/big"
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
				MaxElapsedTime:  4000,
			},
			RetryConnectionErrors: true,
		}),
	)
}

func CreateLedger(ctx context.Context, client *client.Formance, name string) error {
	_, err := client.Ledger.V2.CreateLedger(ctx, operations.V2CreateLedgerRequest{
		Ledger: name,
	})

	return err
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
