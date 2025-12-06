# Ledgers
(*Ledgers*)

## Overview

### Available Operations

* [CreateLedger](#createledger) - Create a new ledger
* [ListLedgersInBucket](#listledgersinbucket) - List all ledgers in a bucket
* [GetLedgerFromBucket](#getledgerfrombucket) - Get a ledger from a bucket
* [CreateLedgerInBucket](#createledgerinbucket) - Create a new ledger in a bucket

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

## ListLedgersInBucket

Returns a list of all ledgers in the specified bucket

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

    res, err := s.Ledgers.ListLedgersInBucket(ctx, operations.ListLedgersInBucketRequest{
        BucketName: "<value>",
    })
    if err != nil {
        log.Fatal(err)
    }
    if res.ListLedgersInBucketResponse != nil {
        // handle response
    }
}
```

### Parameters

| Parameter                                                                                      | Type                                                                                           | Required                                                                                       | Description                                                                                    |
| ---------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------- |
| `ctx`                                                                                          | [context.Context](https://pkg.go.dev/context#Context)                                          | :heavy_check_mark:                                                                             | The context to use for the request.                                                            |
| `request`                                                                                      | [operations.ListLedgersInBucketRequest](../../models/operations/listledgersinbucketrequest.md) | :heavy_check_mark:                                                                             | The request object to use for the request.                                                     |
| `opts`                                                                                         | [][operations.Option](../../models/operations/option.md)                                       | :heavy_minus_sign:                                                                             | The options for this request.                                                                  |

### Response

**[*operations.ListLedgersInBucketResponse](../../models/operations/listledgersinbucketresponse.md), error**

### Errors

| Error Type              | Status Code             | Content Type            |
| ----------------------- | ----------------------- | ----------------------- |
| sdkerrors.ErrorResponse | 500                     | application/json        |
| sdkerrors.SDKError      | 4XX, 5XX                | \*/\*                   |

## GetLedgerFromBucket

Retrieves a ledger from the specified bucket

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

    res, err := s.Ledgers.GetLedgerFromBucket(ctx, operations.GetLedgerFromBucketRequest{
        BucketName: "<value>",
        LedgerName: "<value>",
    })
    if err != nil {
        log.Fatal(err)
    }
    if res.GetLedgerFromBucketResponse != nil {
        // handle response
    }
}
```

### Parameters

| Parameter                                                                                      | Type                                                                                           | Required                                                                                       | Description                                                                                    |
| ---------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------- |
| `ctx`                                                                                          | [context.Context](https://pkg.go.dev/context#Context)                                          | :heavy_check_mark:                                                                             | The context to use for the request.                                                            |
| `request`                                                                                      | [operations.GetLedgerFromBucketRequest](../../models/operations/getledgerfrombucketrequest.md) | :heavy_check_mark:                                                                             | The request object to use for the request.                                                     |
| `opts`                                                                                         | [][operations.Option](../../models/operations/option.md)                                       | :heavy_minus_sign:                                                                             | The options for this request.                                                                  |

### Response

**[*operations.GetLedgerFromBucketResponse](../../models/operations/getledgerfrombucketresponse.md), error**

### Errors

| Error Type              | Status Code             | Content Type            |
| ----------------------- | ----------------------- | ----------------------- |
| sdkerrors.ErrorResponse | 404                     | application/json        |
| sdkerrors.ErrorResponse | 500                     | application/json        |
| sdkerrors.SDKError      | 4XX, 5XX                | \*/\*                   |

## CreateLedgerInBucket

Creates a new ledger in the specified bucket

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

    res, err := s.Ledgers.CreateLedgerInBucket(ctx, operations.CreateLedgerInBucketRequest{
        BucketName: "<value>",
        LedgerName: "<value>",
    })
    if err != nil {
        log.Fatal(err)
    }
    if res.CreateLedgerInBucketResponse != nil {
        // handle response
    }
}
```

### Parameters

| Parameter                                                                                        | Type                                                                                             | Required                                                                                         | Description                                                                                      |
| ------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------ |
| `ctx`                                                                                            | [context.Context](https://pkg.go.dev/context#Context)                                            | :heavy_check_mark:                                                                               | The context to use for the request.                                                              |
| `request`                                                                                        | [operations.CreateLedgerInBucketRequest](../../models/operations/createledgerinbucketrequest.md) | :heavy_check_mark:                                                                               | The request object to use for the request.                                                       |
| `opts`                                                                                           | [][operations.Option](../../models/operations/option.md)                                         | :heavy_minus_sign:                                                                               | The options for this request.                                                                    |

### Response

**[*operations.CreateLedgerInBucketResponse](../../models/operations/createledgerinbucketresponse.md), error**

### Errors

| Error Type              | Status Code             | Content Type            |
| ----------------------- | ----------------------- | ----------------------- |
| sdkerrors.ErrorResponse | 400, 409                | application/json        |
| sdkerrors.ErrorResponse | 500                     | application/json        |
| sdkerrors.SDKError      | 4XX, 5XX                | \*/\*                   |