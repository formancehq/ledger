# Ledgers
(*Ledgers*)

## Overview

### Available Operations

* [CreateLedger](#createledger) - Create a new ledger

## CreateLedger

Creates a new ledger with the specified name

### Example Usage

```go
package main

import(
	"context"
	"github.com/formancehq/ledger-v3-poc/pkg/client"
	"github.com/formancehq/ledger-v3-poc/pkg/client/models/operations"
	"log"
)

func main() {
    ctx := context.Background()

    s := client.New()

    res, err := s.Ledgers.CreateLedger(ctx, operations.CreateLedgerRequest{
        LedgerName: "<value>",
    })
    if err != nil {
        log.Fatal(err)
    }
    if res.CreateLedgerResponse != nil {
        // handle response
    }
}
```

### Parameters

| Parameter                                                                        | Type                                                                             | Required                                                                         | Description                                                                      |
| -------------------------------------------------------------------------------- | -------------------------------------------------------------------------------- | -------------------------------------------------------------------------------- | -------------------------------------------------------------------------------- |
| `ctx`                                                                            | [context.Context](https://pkg.go.dev/context#Context)                            | :heavy_check_mark:                                                               | The context to use for the request.                                              |
| `request`                                                                        | [operations.CreateLedgerRequest](../../models/operations/createledgerrequest.md) | :heavy_check_mark:                                                               | The request object to use for the request.                                       |
| `opts`                                                                           | [][operations.Option](../../models/operations/option.md)                         | :heavy_minus_sign:                                                               | The options for this request.                                                    |

### Response

**[*operations.CreateLedgerResponse](../../models/operations/createledgerresponse.md), error**

### Errors

| Error Type              | Status Code             | Content Type            |
| ----------------------- | ----------------------- | ----------------------- |
| sdkerrors.ErrorResponse | 400, 409                | application/json        |
| sdkerrors.ErrorResponse | 500                     | application/json        |
| sdkerrors.SDKError      | 4XX, 5XX                | \*/\*                   |