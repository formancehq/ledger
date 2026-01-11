# Transactions
(*Transactions*)

## Overview

### Available Operations

* [CreateTransaction](#createtransaction) - Create a new transaction
* [SaveTransactionMetadata](#savetransactionmetadata) - Save transaction metadata
* [DeleteTransactionMetadata](#deletetransactionmetadata) - Delete transaction metadata
* [BulkOperations](#bulkoperations) - Bulk operations

## CreateTransaction

Creates a new transaction in the specified ledger with the specified postings

### Example Usage

```go
package main

import(
	"context"
	"github.com/formancehq/ledger-v3-poc/pkg/client"
	"github.com/formancehq/ledger-v3-poc/pkg/client/models/components"
	"github.com/formancehq/ledger-v3-poc/pkg/client/models/operations"
	"log"
)

func main() {
    ctx := context.Background()

    s := client.New()

    res, err := s.Transactions.CreateTransaction(ctx, operations.CreateTransactionRequest{
        LedgerName: "<value>",
        CreateTransactionRequest: components.CreateTransactionRequest{},
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
| `request`                                                                                  | [operations.CreateTransactionRequest](../../models/operations/createtransactionrequest.md) | :heavy_check_mark:                                                                         | The request object to use for the request.                                                 |
| `opts`                                                                                     | [][operations.Option](../../models/operations/option.md)                                   | :heavy_minus_sign:                                                                         | The options for this request.                                                              |

### Response

**[*operations.CreateTransactionResponse](../../models/operations/createtransactionresponse.md), error**

### Errors

| Error Type              | Status Code             | Content Type            |
| ----------------------- | ----------------------- | ----------------------- |
| sdkerrors.ErrorResponse | 400                     | application/json        |
| sdkerrors.ErrorResponse | 500                     | application/json        |
| sdkerrors.ErrorResponse | 503                     | application/json        |
| sdkerrors.SDKError      | 4XX, 5XX                | \*/\*                   |

## SaveTransactionMetadata

Saves metadata for a specific transaction in the ledger

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

    res, err := s.Transactions.SaveTransactionMetadata(ctx, operations.SaveTransactionMetadataRequest{
        LedgerName: "<value>",
        TransactionID: 385411,
        RequestBody: map[string]string{
            "reason": "correction",
            "source": "support",
        },
    })
    if err != nil {
        log.Fatal(err)
    }
    if res != nil {
        // handle response
    }
}
```

### Parameters

| Parameter                                                                                              | Type                                                                                                   | Required                                                                                               | Description                                                                                            |
| ------------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------ |
| `ctx`                                                                                                  | [context.Context](https://pkg.go.dev/context#Context)                                                  | :heavy_check_mark:                                                                                     | The context to use for the request.                                                                    |
| `request`                                                                                              | [operations.SaveTransactionMetadataRequest](../../models/operations/savetransactionmetadatarequest.md) | :heavy_check_mark:                                                                                     | The request object to use for the request.                                                             |
| `opts`                                                                                                 | [][operations.Option](../../models/operations/option.md)                                               | :heavy_minus_sign:                                                                                     | The options for this request.                                                                          |

### Response

**[*operations.SaveTransactionMetadataResponse](../../models/operations/savetransactionmetadataresponse.md), error**

### Errors

| Error Type              | Status Code             | Content Type            |
| ----------------------- | ----------------------- | ----------------------- |
| sdkerrors.ErrorResponse | 400                     | application/json        |
| sdkerrors.ErrorResponse | 500                     | application/json        |
| sdkerrors.ErrorResponse | 503                     | application/json        |
| sdkerrors.SDKError      | 4XX, 5XX                | \*/\*                   |

## DeleteTransactionMetadata

Deletes a metadata key for a specific transaction in the ledger

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

    res, err := s.Transactions.DeleteTransactionMetadata(ctx, operations.DeleteTransactionMetadataRequest{
        LedgerName: "<value>",
        TransactionID: 220958,
        Key: "<key>",
    })
    if err != nil {
        log.Fatal(err)
    }
    if res != nil {
        // handle response
    }
}
```

### Parameters

| Parameter                                                                                                  | Type                                                                                                       | Required                                                                                                   | Description                                                                                                |
| ---------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------- |
| `ctx`                                                                                                      | [context.Context](https://pkg.go.dev/context#Context)                                                      | :heavy_check_mark:                                                                                         | The context to use for the request.                                                                        |
| `request`                                                                                                  | [operations.DeleteTransactionMetadataRequest](../../models/operations/deletetransactionmetadatarequest.md) | :heavy_check_mark:                                                                                         | The request object to use for the request.                                                                 |
| `opts`                                                                                                     | [][operations.Option](../../models/operations/option.md)                                                   | :heavy_minus_sign:                                                                                         | The options for this request.                                                                              |

### Response

**[*operations.DeleteTransactionMetadataResponse](../../models/operations/deletetransactionmetadataresponse.md), error**

### Errors

| Error Type              | Status Code             | Content Type            |
| ----------------------- | ----------------------- | ----------------------- |
| sdkerrors.ErrorResponse | 400                     | application/json        |
| sdkerrors.ErrorResponse | 500                     | application/json        |
| sdkerrors.ErrorResponse | 503                     | application/json        |
| sdkerrors.SDKError      | 4XX, 5XX                | \*/\*                   |

## BulkOperations

Execute multiple operations (create transactions, add metadata, revert transactions, delete metadata) in a single request

### Example Usage

```go
package main

import(
	"context"
	"github.com/formancehq/ledger-v3-poc/pkg/client"
	"github.com/formancehq/ledger-v3-poc/pkg/client/models/components"
	"github.com/formancehq/ledger-v3-poc/pkg/client/models/operations"
	"log"
)

func main() {
    ctx := context.Background()

    s := client.New()

    res, err := s.Transactions.BulkOperations(ctx, operations.BulkOperationsRequest{
        LedgerName: "<value>",
        RequestBody: []components.BulkElement{
            components.BulkElement{
                Action: components.ActionDeleteMetadata,
                Data: components.CreateBulkElementDataDeleteMetadataRequest(
                    components.DeleteMetadataRequest{
                        TargetType: components.DeleteMetadataRequestTargetTypeTransaction,
                        TargetID: components.CreateDeleteMetadataRequestTargetIDStr(
                            "<id>",
                        ),
                        Key: "<key>",
                    },
                ),
            },
        },
    })
    if err != nil {
        log.Fatal(err)
    }
    if res.BulkResponse != nil {
        // handle response
    }
}
```

### Parameters

| Parameter                                                                            | Type                                                                                 | Required                                                                             | Description                                                                          |
| ------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------ |
| `ctx`                                                                                | [context.Context](https://pkg.go.dev/context#Context)                                | :heavy_check_mark:                                                                   | The context to use for the request.                                                  |
| `request`                                                                            | [operations.BulkOperationsRequest](../../models/operations/bulkoperationsrequest.md) | :heavy_check_mark:                                                                   | The request object to use for the request.                                           |
| `opts`                                                                               | [][operations.Option](../../models/operations/option.md)                             | :heavy_minus_sign:                                                                   | The options for this request.                                                        |

### Response

**[*operations.BulkOperationsResponse](../../models/operations/bulkoperationsresponse.md), error**

### Errors

| Error Type              | Status Code             | Content Type            |
| ----------------------- | ----------------------- | ----------------------- |
| sdkerrors.ErrorResponse | 400, 413                | application/json        |
| sdkerrors.ErrorResponse | 500                     | application/json        |
| sdkerrors.ErrorResponse | 503                     | application/json        |
| sdkerrors.SDKError      | 4XX, 5XX                | \*/\*                   |