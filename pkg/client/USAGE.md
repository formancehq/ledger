<!-- Start SDK Example Usage [usage] -->
```go
package main

import (
	"context"
	"github.com/formancehq/ledger-v3-poc/pkg/client"
	"github.com/formancehq/ledger-v3-poc/pkg/client/models/components"
	"github.com/formancehq/ledger-v3-poc/pkg/client/types"
	"log"
)

func main() {
	ctx := context.Background()

	s := client.New()

	res, err := s.Transactions.CreateTransaction(ctx, components.CreateTransactionRequest{
		Postings: []components.PostingRequest{
			components.PostingRequest{
				Source:      "<value>",
				Destination: "<value>",
				Amount:      types.MustNewBigIntFromString("361192"),
				Asset:       "<value>",
			},
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	if res.CreateTransactionResponse != nil {
		// handle response
	}
}

```
<!-- End SDK Example Usage [usage] -->