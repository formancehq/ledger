<!-- Start SDK Example Usage [usage] -->
```go
package main

import (
	"context"
	"github.com/formancehq/ledger/pkg/client"
	"github.com/formancehq/ledger/pkg/client/models/components"
	"log"
)

func main() {
	s := client.New(
		client.WithSecurity(components.Security{
			ClientID:     "",
			ClientSecret: "",
		}),
	)

	ctx := context.Background()
	res, err := s.Ledger.V1.GetInfo(ctx)
	if err != nil {
		log.Fatal(err)
	}
	if res.ConfigInfoResponse != nil {
		// handle response
	}
}

```
<!-- End SDK Example Usage [usage] -->