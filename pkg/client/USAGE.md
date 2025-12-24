<!-- Start SDK Example Usage [usage] -->
```go
package main

import (
	"context"
	"github.com/formancehq/ledger-v3-poc/pkg/client"
	"github.com/formancehq/ledger-v3-poc/pkg/client/models/operations"
	"log"
)

func main() {
	ctx := context.Background()

	s := client.New()

	res, err := s.Ledgers.ListAllLedgers(ctx, operations.ListAllLedgersRequest{})
	if err != nil {
		log.Fatal(err)
	}
	if res.ListAllLedgersResponse != nil {
		// handle response
	}
}

```
<!-- End SDK Example Usage [usage] -->