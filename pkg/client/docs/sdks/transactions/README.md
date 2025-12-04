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
	"openapi"
	"openapi/pkg/models/shared"
	"log"
)

func main() {
    ctx := context.Background()

    s := openapi.New()

    res, err := s.Transactions.CreateTransaction(ctx, shared.CreateTransactionRequest{
        Postings: []shared.PostingRequest{
            shared.PostingRequest{
                Amount: "361.19",
                Asset: "<value>",
                Destination: "<value>",
                Source: "<value>",
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

| Parameter                                                                              | Type                                                                                   | Required                                                                               | Description                                                                            |
| -------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------- |
| `ctx`                                                                                  | [context.Context](https://pkg.go.dev/context#Context)                                  | :heavy_check_mark:                                                                     | The context to use for the request.                                                    |
| `request`                                                                              | [shared.CreateTransactionRequest](../../pkg/models/shared/createtransactionrequest.md) | :heavy_check_mark:                                                                     | The request object to use for the request.                                             |
| `opts`                                                                                 | [][operations.Option](../../pkg/models/operations/option.md)                           | :heavy_minus_sign:                                                                     | The options for this request.                                                          |

### Response

**[*operations.CreateTransactionResponse](../../pkg/models/operations/createtransactionresponse.md), error**

### Errors

| Error Type              | Status Code             | Content Type            |
| ----------------------- | ----------------------- | ----------------------- |
| sdkerrors.ErrorResponse | 400                     | application/json        |
| sdkerrors.ErrorResponse | 500                     | application/json        |
| sdkerrors.SDKError      | 4XX, 5XX                | \*/\*                   |