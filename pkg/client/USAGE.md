<!-- Start SDK Example Usage [usage] -->
```go
package main

import (
	"context"
	"github.com/formancehq/stack/ledger/client"
	"log"
)

func main() {
	s := client.New()

	ctx := context.Background()
	res, err := s.Ledger.V2.GetInfo(ctx)
	if err != nil {
		log.Fatal(err)
	}
	if res.V2ConfigInfoResponse != nil {
		// handle response
	}
}

```
<!-- End SDK Example Usage [usage] -->