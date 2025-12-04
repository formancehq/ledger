# Transactions
(*Transactions*)

## Overview

### Available Operations

* [CreateTransaction](#createtransaction) - Create a new transaction

## CreateTransaction

Creates a new transaction in the ledger with the specified postings

### Example Usage

```go
package main

import(
	"context"
	"github.com/formancehq/ledger-v3-poc/pkg/client"
	"github.com/formancehq/ledger-v3-poc/pkg/client/types"
	"github.com/formancehq/ledger-v3-poc/pkg/client/models/components"
	"log"
)

func main() {
    ctx := context.Background()

    s := client.New()

    res, err := s.Transactions.CreateTransaction(ctx, components.CreateTransactionRequest{
        Postings: []components.PostingRequest{
            components.PostingRequest{
                Source: "<value>",
                Destination: "<value>",
                Amount: types.MustNewBigIntFromString("361192"),
                Asset: "<value>",
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

### Parameters

| Parameter                                                                                  | Type                                                                                       | Required                                                                                   | Description                                                                                |
| ------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------ |
| `ctx`                                                                                      | [context.Context](https://pkg.go.dev/context#Context)                                      | :heavy_check_mark:                                                                         | The context to use for the request.                                                        |
| `request`                                                                                  | [components.CreateTransactionRequest](../../models/components/createtransactionrequest.md) | :heavy_check_mark:                                                                         | The request object to use for the request.                                                 |
| `opts`                                                                                     | [][operations.Option](../../models/operations/option.md)                                   | :heavy_minus_sign:                                                                         | The options for this request.                                                              |

### Response

**[*operations.CreateTransactionResponse](../../models/operations/createtransactionresponse.md), error**

### Errors

| Error Type              | Status Code             | Content Type            |
| ----------------------- | ----------------------- | ----------------------- |
| sdkerrors.ErrorResponse | 400                     | application/json        |
| sdkerrors.ErrorResponse | 500                     | application/json        |
| sdkerrors.SDKError      | 4XX, 5XX                | \*/\*                   |