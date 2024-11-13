package internal

import (
	"fmt"
	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/antithesishq/antithesis-sdk-go/random"
	"github.com/formancehq/go-libs/v2/time"
	"github.com/formancehq/ledger/pkg/client"
	"github.com/formancehq/ledger/pkg/client/retry"
	"math/big"
	"net/http"
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

func AssertAlwaysErrNil(err error, message string) bool {
	return AssertAlways(err == nil, message, Details{
		"error": fmt.Sprint(err),
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
