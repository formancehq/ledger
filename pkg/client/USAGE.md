<!-- Start SDK Example Usage [usage] -->
```go
package main

import (
	"context"
	"log"
	"openapi"
)

func main() {
	ctx := context.Background()

	s := openapi.New()

	res, err := s.Cluster.CreateSnapshot(ctx)
	if err != nil {
		log.Fatal(err)
	}
	if res.SnapshotResponse != nil {
		// handle response
	}
}

```
<!-- End SDK Example Usage [usage] -->