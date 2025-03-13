# V2
(*Ledger.V2*)

## Overview

### Available Operations

* [GetInfo](#getinfo) - Show server information
* [GetMetrics](#getmetrics) - Read in memory metrics
* [ListLedgers](#listledgers) - List ledgers
* [GetLedger](#getledger) - Get a ledger
* [CreateLedger](#createledger) - Create a ledger
* [UpdateLedgerMetadata](#updateledgermetadata) - Update ledger metadata
* [DeleteLedgerMetadata](#deleteledgermetadata) - Delete ledger metadata by key
* [GetLedgerInfo](#getledgerinfo) - Get information about a ledger
* [CreateBulk](#createbulk) - Bulk request
* [CountAccounts](#countaccounts) - Count the accounts from a ledger
* [ListAccounts](#listaccounts) - List accounts from a ledger
* [GetAccount](#getaccount) - Get account by its address
* [AddMetadataToAccount](#addmetadatatoaccount) - Add metadata to an account
* [DeleteAccountMetadata](#deleteaccountmetadata) - Delete metadata by key
* [ReadStats](#readstats) - Get statistics from a ledger
* [CountTransactions](#counttransactions) - Count the transactions from a ledger
* [ListTransactions](#listtransactions) - List transactions from a ledger
* [CreateTransaction](#createtransaction) - Create a new transaction to a ledger
* [GetTransaction](#gettransaction) - Get transaction from a ledger by its ID
* [AddMetadataOnTransaction](#addmetadataontransaction) - Set the metadata of a transaction by its ID
* [DeleteTransactionMetadata](#deletetransactionmetadata) - Delete metadata by key
* [RevertTransaction](#reverttransaction) - Revert a ledger transaction by its ID
* [GetBalancesAggregated](#getbalancesaggregated) - Get the aggregated balances from selected accounts
* [GetVolumesWithBalances](#getvolumeswithbalances) - Get list of volumes with balances for (account/asset)
* [ListLogs](#listlogs) - List the logs from a ledger
* [ImportLogs](#importlogs)
* [ExportLogs](#exportlogs) - Export logs

## GetInfo

Show server information

### Example Usage

```go
package main

import(
	"context"
	"os"
	"github.com/formancehq/ledger/pkg/client/models/components"
	"github.com/formancehq/ledger/pkg/client"
	"log"
)

func main() {
    ctx := context.Background()

    s := client.New(
        client.WithSecurity(components.Security{
            ClientID: client.String(os.Getenv("FORMANCE_CLIENT_ID")),
            ClientSecret: client.String(os.Getenv("FORMANCE_CLIENT_SECRET")),
        }),
    )

    res, err := s.Ledger.V2.GetInfo(ctx)
    if err != nil {
        log.Fatal(err)
    }
    if res.V2ConfigInfoResponse != nil {
        // handle response
    }
}
```

### Parameters

| Parameter                                                | Type                                                     | Required                                                 | Description                                              |
| -------------------------------------------------------- | -------------------------------------------------------- | -------------------------------------------------------- | -------------------------------------------------------- |
| `ctx`                                                    | [context.Context](https://pkg.go.dev/context#Context)    | :heavy_check_mark:                                       | The context to use for the request.                      |
| `opts`                                                   | [][operations.Option](../../models/operations/option.md) | :heavy_minus_sign:                                       | The options for this request.                            |

### Response

**[*operations.V2GetInfoResponse](../../models/operations/v2getinforesponse.md), error**

### Errors

| Error Type                | Status Code               | Content Type              |
| ------------------------- | ------------------------- | ------------------------- |
| sdkerrors.V2ErrorResponse | default                   | application/json          |
| sdkerrors.SDKError        | 4XX, 5XX                  | \*/\*                     |

## GetMetrics

Read in memory metrics

### Example Usage

```go
package main

import(
	"context"
	"os"
	"github.com/formancehq/ledger/pkg/client/models/components"
	"github.com/formancehq/ledger/pkg/client"
	"log"
)

func main() {
    ctx := context.Background()

    s := client.New(
        client.WithSecurity(components.Security{
            ClientID: client.String(os.Getenv("FORMANCE_CLIENT_ID")),
            ClientSecret: client.String(os.Getenv("FORMANCE_CLIENT_SECRET")),
        }),
    )

    res, err := s.Ledger.V2.GetMetrics(ctx)
    if err != nil {
        log.Fatal(err)
    }
    if res.Object != nil {
        // handle response
    }
}
```

### Parameters

| Parameter                                                | Type                                                     | Required                                                 | Description                                              |
| -------------------------------------------------------- | -------------------------------------------------------- | -------------------------------------------------------- | -------------------------------------------------------- |
| `ctx`                                                    | [context.Context](https://pkg.go.dev/context#Context)    | :heavy_check_mark:                                       | The context to use for the request.                      |
| `opts`                                                   | [][operations.Option](../../models/operations/option.md) | :heavy_minus_sign:                                       | The options for this request.                            |

### Response

**[*operations.GetMetricsResponse](../../models/operations/getmetricsresponse.md), error**

### Errors

| Error Type                | Status Code               | Content Type              |
| ------------------------- | ------------------------- | ------------------------- |
| sdkerrors.V2ErrorResponse | default                   | application/json          |
| sdkerrors.SDKError        | 4XX, 5XX                  | \*/\*                     |

## ListLedgers

List ledgers

### Example Usage

```go
package main

import(
	"context"
	"os"
	"github.com/formancehq/ledger/pkg/client/models/components"
	"github.com/formancehq/ledger/pkg/client"
	"github.com/formancehq/ledger/pkg/client/models/operations"
	"log"
)

func main() {
    ctx := context.Background()

    s := client.New(
        client.WithSecurity(components.Security{
            ClientID: client.String(os.Getenv("FORMANCE_CLIENT_ID")),
            ClientSecret: client.String(os.Getenv("FORMANCE_CLIENT_SECRET")),
        }),
    )

    res, err := s.Ledger.V2.ListLedgers(ctx, operations.V2ListLedgersRequest{
        PageSize: client.Int64(100),
        Cursor: client.String("aHR0cHM6Ly9nLnBhZ2UvTmVrby1SYW1lbj9zaGFyZQ=="),
    })
    if err != nil {
        log.Fatal(err)
    }
    if res.V2LedgerListResponse != nil {
        // handle response
    }
}
```

### Parameters

| Parameter                                                                          | Type                                                                               | Required                                                                           | Description                                                                        |
| ---------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------- |
| `ctx`                                                                              | [context.Context](https://pkg.go.dev/context#Context)                              | :heavy_check_mark:                                                                 | The context to use for the request.                                                |
| `request`                                                                          | [operations.V2ListLedgersRequest](../../models/operations/v2listledgersrequest.md) | :heavy_check_mark:                                                                 | The request object to use for the request.                                         |
| `opts`                                                                             | [][operations.Option](../../models/operations/option.md)                           | :heavy_minus_sign:                                                                 | The options for this request.                                                      |

### Response

**[*operations.V2ListLedgersResponse](../../models/operations/v2listledgersresponse.md), error**

### Errors

| Error Type                | Status Code               | Content Type              |
| ------------------------- | ------------------------- | ------------------------- |
| sdkerrors.V2ErrorResponse | default                   | application/json          |
| sdkerrors.SDKError        | 4XX, 5XX                  | \*/\*                     |

## GetLedger

Get a ledger

### Example Usage

```go
package main

import(
	"context"
	"os"
	"github.com/formancehq/ledger/pkg/client/models/components"
	"github.com/formancehq/ledger/pkg/client"
	"github.com/formancehq/ledger/pkg/client/models/operations"
	"log"
)

func main() {
    ctx := context.Background()

    s := client.New(
        client.WithSecurity(components.Security{
            ClientID: client.String(os.Getenv("FORMANCE_CLIENT_ID")),
            ClientSecret: client.String(os.Getenv("FORMANCE_CLIENT_SECRET")),
        }),
    )

    res, err := s.Ledger.V2.GetLedger(ctx, operations.V2GetLedgerRequest{
        Ledger: "ledger001",
    })
    if err != nil {
        log.Fatal(err)
    }
    if res.V2GetLedgerResponse != nil {
        // handle response
    }
}
```

### Parameters

| Parameter                                                                      | Type                                                                           | Required                                                                       | Description                                                                    |
| ------------------------------------------------------------------------------ | ------------------------------------------------------------------------------ | ------------------------------------------------------------------------------ | ------------------------------------------------------------------------------ |
| `ctx`                                                                          | [context.Context](https://pkg.go.dev/context#Context)                          | :heavy_check_mark:                                                             | The context to use for the request.                                            |
| `request`                                                                      | [operations.V2GetLedgerRequest](../../models/operations/v2getledgerrequest.md) | :heavy_check_mark:                                                             | The request object to use for the request.                                     |
| `opts`                                                                         | [][operations.Option](../../models/operations/option.md)                       | :heavy_minus_sign:                                                             | The options for this request.                                                  |

### Response

**[*operations.V2GetLedgerResponse](../../models/operations/v2getledgerresponse.md), error**

### Errors

| Error Type                | Status Code               | Content Type              |
| ------------------------- | ------------------------- | ------------------------- |
| sdkerrors.V2ErrorResponse | default                   | application/json          |
| sdkerrors.SDKError        | 4XX, 5XX                  | \*/\*                     |

## CreateLedger

Create a ledger

### Example Usage

```go
package main

import(
	"context"
	"os"
	"github.com/formancehq/ledger/pkg/client/models/components"
	"github.com/formancehq/ledger/pkg/client"
	"github.com/formancehq/ledger/pkg/client/models/operations"
	"log"
)

func main() {
    ctx := context.Background()

    s := client.New(
        client.WithSecurity(components.Security{
            ClientID: client.String(os.Getenv("FORMANCE_CLIENT_ID")),
            ClientSecret: client.String(os.Getenv("FORMANCE_CLIENT_SECRET")),
        }),
    )

    res, err := s.Ledger.V2.CreateLedger(ctx, operations.V2CreateLedgerRequest{
        Ledger: "ledger001",
        V2CreateLedgerRequest: components.V2CreateLedgerRequest{
            Metadata: map[string]string{
                "admin": "true",
            },
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

| Parameter                                                                            | Type                                                                                 | Required                                                                             | Description                                                                          |
| ------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------ |
| `ctx`                                                                                | [context.Context](https://pkg.go.dev/context#Context)                                | :heavy_check_mark:                                                                   | The context to use for the request.                                                  |
| `request`                                                                            | [operations.V2CreateLedgerRequest](../../models/operations/v2createledgerrequest.md) | :heavy_check_mark:                                                                   | The request object to use for the request.                                           |
| `opts`                                                                               | [][operations.Option](../../models/operations/option.md)                             | :heavy_minus_sign:                                                                   | The options for this request.                                                        |

### Response

**[*operations.V2CreateLedgerResponse](../../models/operations/v2createledgerresponse.md), error**

### Errors

| Error Type                | Status Code               | Content Type              |
| ------------------------- | ------------------------- | ------------------------- |
| sdkerrors.V2ErrorResponse | default                   | application/json          |
| sdkerrors.SDKError        | 4XX, 5XX                  | \*/\*                     |

## UpdateLedgerMetadata

Update ledger metadata

### Example Usage

```go
package main

import(
	"context"
	"os"
	"github.com/formancehq/ledger/pkg/client/models/components"
	"github.com/formancehq/ledger/pkg/client"
	"github.com/formancehq/ledger/pkg/client/models/operations"
	"log"
)

func main() {
    ctx := context.Background()

    s := client.New(
        client.WithSecurity(components.Security{
            ClientID: client.String(os.Getenv("FORMANCE_CLIENT_ID")),
            ClientSecret: client.String(os.Getenv("FORMANCE_CLIENT_SECRET")),
        }),
    )

    res, err := s.Ledger.V2.UpdateLedgerMetadata(ctx, operations.V2UpdateLedgerMetadataRequest{
        Ledger: "ledger001",
        RequestBody: map[string]string{
            "admin": "true",
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

| Parameter                                                                                            | Type                                                                                                 | Required                                                                                             | Description                                                                                          |
| ---------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------- |
| `ctx`                                                                                                | [context.Context](https://pkg.go.dev/context#Context)                                                | :heavy_check_mark:                                                                                   | The context to use for the request.                                                                  |
| `request`                                                                                            | [operations.V2UpdateLedgerMetadataRequest](../../models/operations/v2updateledgermetadatarequest.md) | :heavy_check_mark:                                                                                   | The request object to use for the request.                                                           |
| `opts`                                                                                               | [][operations.Option](../../models/operations/option.md)                                             | :heavy_minus_sign:                                                                                   | The options for this request.                                                                        |

### Response

**[*operations.V2UpdateLedgerMetadataResponse](../../models/operations/v2updateledgermetadataresponse.md), error**

### Errors

| Error Type                | Status Code               | Content Type              |
| ------------------------- | ------------------------- | ------------------------- |
| sdkerrors.V2ErrorResponse | default                   | application/json          |
| sdkerrors.SDKError        | 4XX, 5XX                  | \*/\*                     |

## DeleteLedgerMetadata

Delete ledger metadata by key

### Example Usage

```go
package main

import(
	"context"
	"os"
	"github.com/formancehq/ledger/pkg/client/models/components"
	"github.com/formancehq/ledger/pkg/client"
	"github.com/formancehq/ledger/pkg/client/models/operations"
	"log"
)

func main() {
    ctx := context.Background()

    s := client.New(
        client.WithSecurity(components.Security{
            ClientID: client.String(os.Getenv("FORMANCE_CLIENT_ID")),
            ClientSecret: client.String(os.Getenv("FORMANCE_CLIENT_SECRET")),
        }),
    )

    res, err := s.Ledger.V2.DeleteLedgerMetadata(ctx, operations.V2DeleteLedgerMetadataRequest{
        Ledger: "ledger001",
        Key: "foo",
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

| Parameter                                                                                            | Type                                                                                                 | Required                                                                                             | Description                                                                                          |
| ---------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------- |
| `ctx`                                                                                                | [context.Context](https://pkg.go.dev/context#Context)                                                | :heavy_check_mark:                                                                                   | The context to use for the request.                                                                  |
| `request`                                                                                            | [operations.V2DeleteLedgerMetadataRequest](../../models/operations/v2deleteledgermetadatarequest.md) | :heavy_check_mark:                                                                                   | The request object to use for the request.                                                           |
| `opts`                                                                                               | [][operations.Option](../../models/operations/option.md)                                             | :heavy_minus_sign:                                                                                   | The options for this request.                                                                        |

### Response

**[*operations.V2DeleteLedgerMetadataResponse](../../models/operations/v2deleteledgermetadataresponse.md), error**

### Errors

| Error Type                | Status Code               | Content Type              |
| ------------------------- | ------------------------- | ------------------------- |
| sdkerrors.V2ErrorResponse | default                   | application/json          |
| sdkerrors.SDKError        | 4XX, 5XX                  | \*/\*                     |

## GetLedgerInfo

Get information about a ledger

### Example Usage

```go
package main

import(
	"context"
	"os"
	"github.com/formancehq/ledger/pkg/client/models/components"
	"github.com/formancehq/ledger/pkg/client"
	"github.com/formancehq/ledger/pkg/client/models/operations"
	"log"
)

func main() {
    ctx := context.Background()

    s := client.New(
        client.WithSecurity(components.Security{
            ClientID: client.String(os.Getenv("FORMANCE_CLIENT_ID")),
            ClientSecret: client.String(os.Getenv("FORMANCE_CLIENT_SECRET")),
        }),
    )

    res, err := s.Ledger.V2.GetLedgerInfo(ctx, operations.V2GetLedgerInfoRequest{
        Ledger: "ledger001",
    })
    if err != nil {
        log.Fatal(err)
    }
    if res.V2LedgerInfoResponse != nil {
        // handle response
    }
}
```

### Parameters

| Parameter                                                                              | Type                                                                                   | Required                                                                               | Description                                                                            |
| -------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------- |
| `ctx`                                                                                  | [context.Context](https://pkg.go.dev/context#Context)                                  | :heavy_check_mark:                                                                     | The context to use for the request.                                                    |
| `request`                                                                              | [operations.V2GetLedgerInfoRequest](../../models/operations/v2getledgerinforequest.md) | :heavy_check_mark:                                                                     | The request object to use for the request.                                             |
| `opts`                                                                                 | [][operations.Option](../../models/operations/option.md)                               | :heavy_minus_sign:                                                                     | The options for this request.                                                          |

### Response

**[*operations.V2GetLedgerInfoResponse](../../models/operations/v2getledgerinforesponse.md), error**

### Errors

| Error Type                | Status Code               | Content Type              |
| ------------------------- | ------------------------- | ------------------------- |
| sdkerrors.V2ErrorResponse | default                   | application/json          |
| sdkerrors.SDKError        | 4XX, 5XX                  | \*/\*                     |

## CreateBulk

Bulk request

### Example Usage

```go
package main

import(
	"context"
	"os"
	"github.com/formancehq/ledger/pkg/client/models/components"
	"github.com/formancehq/ledger/pkg/client"
	"math/big"
	"github.com/formancehq/ledger/pkg/client/models/operations"
	"log"
)

func main() {
    ctx := context.Background()

    s := client.New(
        client.WithSecurity(components.Security{
            ClientID: client.String(os.Getenv("FORMANCE_CLIENT_ID")),
            ClientSecret: client.String(os.Getenv("FORMANCE_CLIENT_SECRET")),
        }),
    )

    res, err := s.Ledger.V2.CreateBulk(ctx, operations.V2CreateBulkRequest{
        Ledger: "ledger001",
        ContinueOnFailure: client.Bool(true),
        Atomic: client.Bool(true),
        Parallel: client.Bool(true),
        RequestBody: []components.V2BulkElement{
            components.CreateV2BulkElementRevertTransaction(
                components.V2BulkElementRevertTransaction{
                    Action: "<value>",
                },
            ),
        },
    })
    if err != nil {
        log.Fatal(err)
    }
    if res.V2BulkResponse != nil {
        // handle response
    }
}
```

### Parameters

| Parameter                                                                        | Type                                                                             | Required                                                                         | Description                                                                      |
| -------------------------------------------------------------------------------- | -------------------------------------------------------------------------------- | -------------------------------------------------------------------------------- | -------------------------------------------------------------------------------- |
| `ctx`                                                                            | [context.Context](https://pkg.go.dev/context#Context)                            | :heavy_check_mark:                                                               | The context to use for the request.                                              |
| `request`                                                                        | [operations.V2CreateBulkRequest](../../models/operations/v2createbulkrequest.md) | :heavy_check_mark:                                                               | The request object to use for the request.                                       |
| `opts`                                                                           | [][operations.Option](../../models/operations/option.md)                         | :heavy_minus_sign:                                                               | The options for this request.                                                    |

### Response

**[*operations.V2CreateBulkResponse](../../models/operations/v2createbulkresponse.md), error**

### Errors

| Error Type                | Status Code               | Content Type              |
| ------------------------- | ------------------------- | ------------------------- |
| sdkerrors.V2ErrorResponse | default                   | application/json          |
| sdkerrors.SDKError        | 4XX, 5XX                  | \*/\*                     |

## CountAccounts

Count the accounts from a ledger

### Example Usage

```go
package main

import(
	"context"
	"os"
	"github.com/formancehq/ledger/pkg/client/models/components"
	"github.com/formancehq/ledger/pkg/client"
	"github.com/formancehq/ledger/pkg/client/models/operations"
	"log"
)

func main() {
    ctx := context.Background()

    s := client.New(
        client.WithSecurity(components.Security{
            ClientID: client.String(os.Getenv("FORMANCE_CLIENT_ID")),
            ClientSecret: client.String(os.Getenv("FORMANCE_CLIENT_SECRET")),
        }),
    )

    res, err := s.Ledger.V2.CountAccounts(ctx, operations.V2CountAccountsRequest{
        Ledger: "ledger001",
        RequestBody: map[string]any{
            "key": "<value>",
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

| Parameter                                                                              | Type                                                                                   | Required                                                                               | Description                                                                            |
| -------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------- |
| `ctx`                                                                                  | [context.Context](https://pkg.go.dev/context#Context)                                  | :heavy_check_mark:                                                                     | The context to use for the request.                                                    |
| `request`                                                                              | [operations.V2CountAccountsRequest](../../models/operations/v2countaccountsrequest.md) | :heavy_check_mark:                                                                     | The request object to use for the request.                                             |
| `opts`                                                                                 | [][operations.Option](../../models/operations/option.md)                               | :heavy_minus_sign:                                                                     | The options for this request.                                                          |

### Response

**[*operations.V2CountAccountsResponse](../../models/operations/v2countaccountsresponse.md), error**

### Errors

| Error Type                | Status Code               | Content Type              |
| ------------------------- | ------------------------- | ------------------------- |
| sdkerrors.V2ErrorResponse | default                   | application/json          |
| sdkerrors.SDKError        | 4XX, 5XX                  | \*/\*                     |

## ListAccounts

List accounts from a ledger, sorted by address in descending order.

### Example Usage

```go
package main

import(
	"context"
	"os"
	"github.com/formancehq/ledger/pkg/client/models/components"
	"github.com/formancehq/ledger/pkg/client"
	"github.com/formancehq/ledger/pkg/client/models/operations"
	"log"
)

func main() {
    ctx := context.Background()

    s := client.New(
        client.WithSecurity(components.Security{
            ClientID: client.String(os.Getenv("FORMANCE_CLIENT_ID")),
            ClientSecret: client.String(os.Getenv("FORMANCE_CLIENT_SECRET")),
        }),
    )

    res, err := s.Ledger.V2.ListAccounts(ctx, operations.V2ListAccountsRequest{
        Ledger: "ledger001",
        PageSize: client.Int64(100),
        Cursor: client.String("aHR0cHM6Ly9nLnBhZ2UvTmVrby1SYW1lbj9zaGFyZQ=="),
        RequestBody: map[string]any{

        },
    })
    if err != nil {
        log.Fatal(err)
    }
    if res.V2AccountsCursorResponse != nil {
        // handle response
    }
}
```

### Parameters

| Parameter                                                                            | Type                                                                                 | Required                                                                             | Description                                                                          |
| ------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------ |
| `ctx`                                                                                | [context.Context](https://pkg.go.dev/context#Context)                                | :heavy_check_mark:                                                                   | The context to use for the request.                                                  |
| `request`                                                                            | [operations.V2ListAccountsRequest](../../models/operations/v2listaccountsrequest.md) | :heavy_check_mark:                                                                   | The request object to use for the request.                                           |
| `opts`                                                                               | [][operations.Option](../../models/operations/option.md)                             | :heavy_minus_sign:                                                                   | The options for this request.                                                        |

### Response

**[*operations.V2ListAccountsResponse](../../models/operations/v2listaccountsresponse.md), error**

### Errors

| Error Type                | Status Code               | Content Type              |
| ------------------------- | ------------------------- | ------------------------- |
| sdkerrors.V2ErrorResponse | default                   | application/json          |
| sdkerrors.SDKError        | 4XX, 5XX                  | \*/\*                     |

## GetAccount

Get account by its address

### Example Usage

```go
package main

import(
	"context"
	"os"
	"github.com/formancehq/ledger/pkg/client/models/components"
	"github.com/formancehq/ledger/pkg/client"
	"github.com/formancehq/ledger/pkg/client/models/operations"
	"log"
)

func main() {
    ctx := context.Background()

    s := client.New(
        client.WithSecurity(components.Security{
            ClientID: client.String(os.Getenv("FORMANCE_CLIENT_ID")),
            ClientSecret: client.String(os.Getenv("FORMANCE_CLIENT_SECRET")),
        }),
    )

    res, err := s.Ledger.V2.GetAccount(ctx, operations.V2GetAccountRequest{
        Ledger: "ledger001",
        Address: "users:001",
    })
    if err != nil {
        log.Fatal(err)
    }
    if res.V2AccountResponse != nil {
        // handle response
    }
}
```

### Parameters

| Parameter                                                                        | Type                                                                             | Required                                                                         | Description                                                                      |
| -------------------------------------------------------------------------------- | -------------------------------------------------------------------------------- | -------------------------------------------------------------------------------- | -------------------------------------------------------------------------------- |
| `ctx`                                                                            | [context.Context](https://pkg.go.dev/context#Context)                            | :heavy_check_mark:                                                               | The context to use for the request.                                              |
| `request`                                                                        | [operations.V2GetAccountRequest](../../models/operations/v2getaccountrequest.md) | :heavy_check_mark:                                                               | The request object to use for the request.                                       |
| `opts`                                                                           | [][operations.Option](../../models/operations/option.md)                         | :heavy_minus_sign:                                                               | The options for this request.                                                    |

### Response

**[*operations.V2GetAccountResponse](../../models/operations/v2getaccountresponse.md), error**

### Errors

| Error Type                | Status Code               | Content Type              |
| ------------------------- | ------------------------- | ------------------------- |
| sdkerrors.V2ErrorResponse | default                   | application/json          |
| sdkerrors.SDKError        | 4XX, 5XX                  | \*/\*                     |

## AddMetadataToAccount

Add metadata to an account

### Example Usage

```go
package main

import(
	"context"
	"os"
	"github.com/formancehq/ledger/pkg/client/models/components"
	"github.com/formancehq/ledger/pkg/client"
	"github.com/formancehq/ledger/pkg/client/models/operations"
	"log"
)

func main() {
    ctx := context.Background()

    s := client.New(
        client.WithSecurity(components.Security{
            ClientID: client.String(os.Getenv("FORMANCE_CLIENT_ID")),
            ClientSecret: client.String(os.Getenv("FORMANCE_CLIENT_SECRET")),
        }),
    )

    res, err := s.Ledger.V2.AddMetadataToAccount(ctx, operations.V2AddMetadataToAccountRequest{
        Ledger: "ledger001",
        Address: "users:001",
        DryRun: client.Bool(true),
        RequestBody: map[string]string{
            "admin": "true",
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

| Parameter                                                                                            | Type                                                                                                 | Required                                                                                             | Description                                                                                          |
| ---------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------- |
| `ctx`                                                                                                | [context.Context](https://pkg.go.dev/context#Context)                                                | :heavy_check_mark:                                                                                   | The context to use for the request.                                                                  |
| `request`                                                                                            | [operations.V2AddMetadataToAccountRequest](../../models/operations/v2addmetadatatoaccountrequest.md) | :heavy_check_mark:                                                                                   | The request object to use for the request.                                                           |
| `opts`                                                                                               | [][operations.Option](../../models/operations/option.md)                                             | :heavy_minus_sign:                                                                                   | The options for this request.                                                                        |

### Response

**[*operations.V2AddMetadataToAccountResponse](../../models/operations/v2addmetadatatoaccountresponse.md), error**

### Errors

| Error Type                | Status Code               | Content Type              |
| ------------------------- | ------------------------- | ------------------------- |
| sdkerrors.V2ErrorResponse | default                   | application/json          |
| sdkerrors.SDKError        | 4XX, 5XX                  | \*/\*                     |

## DeleteAccountMetadata

Delete metadata by key

### Example Usage

```go
package main

import(
	"context"
	"os"
	"github.com/formancehq/ledger/pkg/client/models/components"
	"github.com/formancehq/ledger/pkg/client"
	"github.com/formancehq/ledger/pkg/client/models/operations"
	"log"
)

func main() {
    ctx := context.Background()

    s := client.New(
        client.WithSecurity(components.Security{
            ClientID: client.String(os.Getenv("FORMANCE_CLIENT_ID")),
            ClientSecret: client.String(os.Getenv("FORMANCE_CLIENT_SECRET")),
        }),
    )

    res, err := s.Ledger.V2.DeleteAccountMetadata(ctx, operations.V2DeleteAccountMetadataRequest{
        Ledger: "ledger001",
        Address: "96609 Cummings Canyon",
        Key: "foo",
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
| `request`                                                                                              | [operations.V2DeleteAccountMetadataRequest](../../models/operations/v2deleteaccountmetadatarequest.md) | :heavy_check_mark:                                                                                     | The request object to use for the request.                                                             |
| `opts`                                                                                                 | [][operations.Option](../../models/operations/option.md)                                               | :heavy_minus_sign:                                                                                     | The options for this request.                                                                          |

### Response

**[*operations.V2DeleteAccountMetadataResponse](../../models/operations/v2deleteaccountmetadataresponse.md), error**

### Errors

| Error Type                | Status Code               | Content Type              |
| ------------------------- | ------------------------- | ------------------------- |
| sdkerrors.V2ErrorResponse | default                   | application/json          |
| sdkerrors.SDKError        | 4XX, 5XX                  | \*/\*                     |

## ReadStats

Get statistics from a ledger. (aggregate metrics on accounts and transactions)


### Example Usage

```go
package main

import(
	"context"
	"os"
	"github.com/formancehq/ledger/pkg/client/models/components"
	"github.com/formancehq/ledger/pkg/client"
	"github.com/formancehq/ledger/pkg/client/models/operations"
	"log"
)

func main() {
    ctx := context.Background()

    s := client.New(
        client.WithSecurity(components.Security{
            ClientID: client.String(os.Getenv("FORMANCE_CLIENT_ID")),
            ClientSecret: client.String(os.Getenv("FORMANCE_CLIENT_SECRET")),
        }),
    )

    res, err := s.Ledger.V2.ReadStats(ctx, operations.V2ReadStatsRequest{
        Ledger: "ledger001",
    })
    if err != nil {
        log.Fatal(err)
    }
    if res.V2StatsResponse != nil {
        // handle response
    }
}
```

### Parameters

| Parameter                                                                      | Type                                                                           | Required                                                                       | Description                                                                    |
| ------------------------------------------------------------------------------ | ------------------------------------------------------------------------------ | ------------------------------------------------------------------------------ | ------------------------------------------------------------------------------ |
| `ctx`                                                                          | [context.Context](https://pkg.go.dev/context#Context)                          | :heavy_check_mark:                                                             | The context to use for the request.                                            |
| `request`                                                                      | [operations.V2ReadStatsRequest](../../models/operations/v2readstatsrequest.md) | :heavy_check_mark:                                                             | The request object to use for the request.                                     |
| `opts`                                                                         | [][operations.Option](../../models/operations/option.md)                       | :heavy_minus_sign:                                                             | The options for this request.                                                  |

### Response

**[*operations.V2ReadStatsResponse](../../models/operations/v2readstatsresponse.md), error**

### Errors

| Error Type                | Status Code               | Content Type              |
| ------------------------- | ------------------------- | ------------------------- |
| sdkerrors.V2ErrorResponse | default                   | application/json          |
| sdkerrors.SDKError        | 4XX, 5XX                  | \*/\*                     |

## CountTransactions

Count the transactions from a ledger

### Example Usage

```go
package main

import(
	"context"
	"os"
	"github.com/formancehq/ledger/pkg/client/models/components"
	"github.com/formancehq/ledger/pkg/client"
	"github.com/formancehq/ledger/pkg/client/models/operations"
	"log"
)

func main() {
    ctx := context.Background()

    s := client.New(
        client.WithSecurity(components.Security{
            ClientID: client.String(os.Getenv("FORMANCE_CLIENT_ID")),
            ClientSecret: client.String(os.Getenv("FORMANCE_CLIENT_SECRET")),
        }),
    )

    res, err := s.Ledger.V2.CountTransactions(ctx, operations.V2CountTransactionsRequest{
        Ledger: "ledger001",
        RequestBody: map[string]any{
            "key": "<value>",
            "key1": "<value>",
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

| Parameter                                                                                      | Type                                                                                           | Required                                                                                       | Description                                                                                    |
| ---------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------- |
| `ctx`                                                                                          | [context.Context](https://pkg.go.dev/context#Context)                                          | :heavy_check_mark:                                                                             | The context to use for the request.                                                            |
| `request`                                                                                      | [operations.V2CountTransactionsRequest](../../models/operations/v2counttransactionsrequest.md) | :heavy_check_mark:                                                                             | The request object to use for the request.                                                     |
| `opts`                                                                                         | [][operations.Option](../../models/operations/option.md)                                       | :heavy_minus_sign:                                                                             | The options for this request.                                                                  |

### Response

**[*operations.V2CountTransactionsResponse](../../models/operations/v2counttransactionsresponse.md), error**

### Errors

| Error Type                | Status Code               | Content Type              |
| ------------------------- | ------------------------- | ------------------------- |
| sdkerrors.V2ErrorResponse | default                   | application/json          |
| sdkerrors.SDKError        | 4XX, 5XX                  | \*/\*                     |

## ListTransactions

List transactions from a ledger, sorted by id in descending order.

### Example Usage

```go
package main

import(
	"context"
	"os"
	"github.com/formancehq/ledger/pkg/client/models/components"
	"github.com/formancehq/ledger/pkg/client"
	"github.com/formancehq/ledger/pkg/client/models/operations"
	"log"
)

func main() {
    ctx := context.Background()

    s := client.New(
        client.WithSecurity(components.Security{
            ClientID: client.String(os.Getenv("FORMANCE_CLIENT_ID")),
            ClientSecret: client.String(os.Getenv("FORMANCE_CLIENT_SECRET")),
        }),
    )

    res, err := s.Ledger.V2.ListTransactions(ctx, operations.V2ListTransactionsRequest{
        Ledger: "ledger001",
        PageSize: client.Int64(100),
        Cursor: client.String("aHR0cHM6Ly9nLnBhZ2UvTmVrby1SYW1lbj9zaGFyZQ=="),
        RequestBody: map[string]any{

        },
    })
    if err != nil {
        log.Fatal(err)
    }
    if res.V2TransactionsCursorResponse != nil {
        // handle response
    }
}
```

### Parameters

| Parameter                                                                                    | Type                                                                                         | Required                                                                                     | Description                                                                                  |
| -------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------- |
| `ctx`                                                                                        | [context.Context](https://pkg.go.dev/context#Context)                                        | :heavy_check_mark:                                                                           | The context to use for the request.                                                          |
| `request`                                                                                    | [operations.V2ListTransactionsRequest](../../models/operations/v2listtransactionsrequest.md) | :heavy_check_mark:                                                                           | The request object to use for the request.                                                   |
| `opts`                                                                                       | [][operations.Option](../../models/operations/option.md)                                     | :heavy_minus_sign:                                                                           | The options for this request.                                                                |

### Response

**[*operations.V2ListTransactionsResponse](../../models/operations/v2listtransactionsresponse.md), error**

### Errors

| Error Type                | Status Code               | Content Type              |
| ------------------------- | ------------------------- | ------------------------- |
| sdkerrors.V2ErrorResponse | default                   | application/json          |
| sdkerrors.SDKError        | 4XX, 5XX                  | \*/\*                     |

## CreateTransaction

Create a new transaction to a ledger

### Example Usage

```go
package main

import(
	"context"
	"os"
	"github.com/formancehq/ledger/pkg/client/models/components"
	"github.com/formancehq/ledger/pkg/client"
	"math/big"
	"github.com/formancehq/ledger/pkg/client/models/operations"
	"log"
)

func main() {
    ctx := context.Background()

    s := client.New(
        client.WithSecurity(components.Security{
            ClientID: client.String(os.Getenv("FORMANCE_CLIENT_ID")),
            ClientSecret: client.String(os.Getenv("FORMANCE_CLIENT_SECRET")),
        }),
    )

    res, err := s.Ledger.V2.CreateTransaction(ctx, operations.V2CreateTransactionRequest{
        Ledger: "ledger001",
        DryRun: client.Bool(true),
        Force: client.Bool(true),
        V2PostTransaction: components.V2PostTransaction{
            Postings: []components.V2Posting{
                components.V2Posting{
                    Amount: big.NewInt(100),
                    Asset: "COIN",
                    Destination: "users:002",
                    Source: "users:001",
                },
                components.V2Posting{
                    Amount: big.NewInt(100),
                    Asset: "COIN",
                    Destination: "users:002",
                    Source: "users:001",
                },
            },
            Script: &components.V2PostTransactionScript{
                Plain: "vars {\n" +
                "account $user\n" +
                "}\n" +
                "send [COIN 10] (\n" +
                "	source = @world\n" +
                "	destination = $user\n" +
                ")\n" +
                "",
                Vars: map[string]string{
                    "user": "users:042",
                },
            },
            Reference: client.String("ref:001"),
            Metadata: map[string]string{
                "admin": "true",
            },
        },
    })
    if err != nil {
        log.Fatal(err)
    }
    if res.V2CreateTransactionResponse != nil {
        // handle response
    }
}
```

### Parameters

| Parameter                                                                                      | Type                                                                                           | Required                                                                                       | Description                                                                                    |
| ---------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------- |
| `ctx`                                                                                          | [context.Context](https://pkg.go.dev/context#Context)                                          | :heavy_check_mark:                                                                             | The context to use for the request.                                                            |
| `request`                                                                                      | [operations.V2CreateTransactionRequest](../../models/operations/v2createtransactionrequest.md) | :heavy_check_mark:                                                                             | The request object to use for the request.                                                     |
| `opts`                                                                                         | [][operations.Option](../../models/operations/option.md)                                       | :heavy_minus_sign:                                                                             | The options for this request.                                                                  |

### Response

**[*operations.V2CreateTransactionResponse](../../models/operations/v2createtransactionresponse.md), error**

### Errors

| Error Type                | Status Code               | Content Type              |
| ------------------------- | ------------------------- | ------------------------- |
| sdkerrors.V2ErrorResponse | default                   | application/json          |
| sdkerrors.SDKError        | 4XX, 5XX                  | \*/\*                     |

## GetTransaction

Get transaction from a ledger by its ID

### Example Usage

```go
package main

import(
	"context"
	"os"
	"github.com/formancehq/ledger/pkg/client/models/components"
	"github.com/formancehq/ledger/pkg/client"
	"math/big"
	"github.com/formancehq/ledger/pkg/client/models/operations"
	"log"
)

func main() {
    ctx := context.Background()

    s := client.New(
        client.WithSecurity(components.Security{
            ClientID: client.String(os.Getenv("FORMANCE_CLIENT_ID")),
            ClientSecret: client.String(os.Getenv("FORMANCE_CLIENT_SECRET")),
        }),
    )

    res, err := s.Ledger.V2.GetTransaction(ctx, operations.V2GetTransactionRequest{
        Ledger: "ledger001",
        ID: big.NewInt(1234),
    })
    if err != nil {
        log.Fatal(err)
    }
    if res.V2GetTransactionResponse != nil {
        // handle response
    }
}
```

### Parameters

| Parameter                                                                                | Type                                                                                     | Required                                                                                 | Description                                                                              |
| ---------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------- |
| `ctx`                                                                                    | [context.Context](https://pkg.go.dev/context#Context)                                    | :heavy_check_mark:                                                                       | The context to use for the request.                                                      |
| `request`                                                                                | [operations.V2GetTransactionRequest](../../models/operations/v2gettransactionrequest.md) | :heavy_check_mark:                                                                       | The request object to use for the request.                                               |
| `opts`                                                                                   | [][operations.Option](../../models/operations/option.md)                                 | :heavy_minus_sign:                                                                       | The options for this request.                                                            |

### Response

**[*operations.V2GetTransactionResponse](../../models/operations/v2gettransactionresponse.md), error**

### Errors

| Error Type                | Status Code               | Content Type              |
| ------------------------- | ------------------------- | ------------------------- |
| sdkerrors.V2ErrorResponse | default                   | application/json          |
| sdkerrors.SDKError        | 4XX, 5XX                  | \*/\*                     |

## AddMetadataOnTransaction

Set the metadata of a transaction by its ID

### Example Usage

```go
package main

import(
	"context"
	"os"
	"github.com/formancehq/ledger/pkg/client/models/components"
	"github.com/formancehq/ledger/pkg/client"
	"math/big"
	"github.com/formancehq/ledger/pkg/client/models/operations"
	"log"
)

func main() {
    ctx := context.Background()

    s := client.New(
        client.WithSecurity(components.Security{
            ClientID: client.String(os.Getenv("FORMANCE_CLIENT_ID")),
            ClientSecret: client.String(os.Getenv("FORMANCE_CLIENT_SECRET")),
        }),
    )

    res, err := s.Ledger.V2.AddMetadataOnTransaction(ctx, operations.V2AddMetadataOnTransactionRequest{
        Ledger: "ledger001",
        ID: big.NewInt(1234),
        DryRun: client.Bool(true),
        RequestBody: map[string]string{
            "admin": "true",
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

| Parameter                                                                                                    | Type                                                                                                         | Required                                                                                                     | Description                                                                                                  |
| ------------------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------ |
| `ctx`                                                                                                        | [context.Context](https://pkg.go.dev/context#Context)                                                        | :heavy_check_mark:                                                                                           | The context to use for the request.                                                                          |
| `request`                                                                                                    | [operations.V2AddMetadataOnTransactionRequest](../../models/operations/v2addmetadataontransactionrequest.md) | :heavy_check_mark:                                                                                           | The request object to use for the request.                                                                   |
| `opts`                                                                                                       | [][operations.Option](../../models/operations/option.md)                                                     | :heavy_minus_sign:                                                                                           | The options for this request.                                                                                |

### Response

**[*operations.V2AddMetadataOnTransactionResponse](../../models/operations/v2addmetadataontransactionresponse.md), error**

### Errors

| Error Type                | Status Code               | Content Type              |
| ------------------------- | ------------------------- | ------------------------- |
| sdkerrors.V2ErrorResponse | default                   | application/json          |
| sdkerrors.SDKError        | 4XX, 5XX                  | \*/\*                     |

## DeleteTransactionMetadata

Delete metadata by key

### Example Usage

```go
package main

import(
	"context"
	"os"
	"github.com/formancehq/ledger/pkg/client/models/components"
	"github.com/formancehq/ledger/pkg/client"
	"math/big"
	"github.com/formancehq/ledger/pkg/client/models/operations"
	"log"
)

func main() {
    ctx := context.Background()

    s := client.New(
        client.WithSecurity(components.Security{
            ClientID: client.String(os.Getenv("FORMANCE_CLIENT_ID")),
            ClientSecret: client.String(os.Getenv("FORMANCE_CLIENT_SECRET")),
        }),
    )

    res, err := s.Ledger.V2.DeleteTransactionMetadata(ctx, operations.V2DeleteTransactionMetadataRequest{
        Ledger: "ledger001",
        ID: big.NewInt(1234),
        Key: "foo",
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

| Parameter                                                                                                      | Type                                                                                                           | Required                                                                                                       | Description                                                                                                    |
| -------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------- |
| `ctx`                                                                                                          | [context.Context](https://pkg.go.dev/context#Context)                                                          | :heavy_check_mark:                                                                                             | The context to use for the request.                                                                            |
| `request`                                                                                                      | [operations.V2DeleteTransactionMetadataRequest](../../models/operations/v2deletetransactionmetadatarequest.md) | :heavy_check_mark:                                                                                             | The request object to use for the request.                                                                     |
| `opts`                                                                                                         | [][operations.Option](../../models/operations/option.md)                                                       | :heavy_minus_sign:                                                                                             | The options for this request.                                                                                  |

### Response

**[*operations.V2DeleteTransactionMetadataResponse](../../models/operations/v2deletetransactionmetadataresponse.md), error**

### Errors

| Error Type                | Status Code               | Content Type              |
| ------------------------- | ------------------------- | ------------------------- |
| sdkerrors.V2ErrorResponse | default                   | application/json          |
| sdkerrors.SDKError        | 4XX, 5XX                  | \*/\*                     |

## RevertTransaction

Revert a ledger transaction by its ID

### Example Usage

```go
package main

import(
	"context"
	"os"
	"github.com/formancehq/ledger/pkg/client/models/components"
	"github.com/formancehq/ledger/pkg/client"
	"math/big"
	"github.com/formancehq/ledger/pkg/client/models/operations"
	"log"
)

func main() {
    ctx := context.Background()

    s := client.New(
        client.WithSecurity(components.Security{
            ClientID: client.String(os.Getenv("FORMANCE_CLIENT_ID")),
            ClientSecret: client.String(os.Getenv("FORMANCE_CLIENT_SECRET")),
        }),
    )

    res, err := s.Ledger.V2.RevertTransaction(ctx, operations.V2RevertTransactionRequest{
        Ledger: "ledger001",
        ID: big.NewInt(1234),
        DryRun: client.Bool(true),
    })
    if err != nil {
        log.Fatal(err)
    }
    if res.V2RevertTransactionResponse != nil {
        // handle response
    }
}
```

### Parameters

| Parameter                                                                                      | Type                                                                                           | Required                                                                                       | Description                                                                                    |
| ---------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------- |
| `ctx`                                                                                          | [context.Context](https://pkg.go.dev/context#Context)                                          | :heavy_check_mark:                                                                             | The context to use for the request.                                                            |
| `request`                                                                                      | [operations.V2RevertTransactionRequest](../../models/operations/v2reverttransactionrequest.md) | :heavy_check_mark:                                                                             | The request object to use for the request.                                                     |
| `opts`                                                                                         | [][operations.Option](../../models/operations/option.md)                                       | :heavy_minus_sign:                                                                             | The options for this request.                                                                  |

### Response

**[*operations.V2RevertTransactionResponse](../../models/operations/v2reverttransactionresponse.md), error**

### Errors

| Error Type                | Status Code               | Content Type              |
| ------------------------- | ------------------------- | ------------------------- |
| sdkerrors.V2ErrorResponse | default                   | application/json          |
| sdkerrors.SDKError        | 4XX, 5XX                  | \*/\*                     |

## GetBalancesAggregated

Get the aggregated balances from selected accounts

### Example Usage

```go
package main

import(
	"context"
	"os"
	"github.com/formancehq/ledger/pkg/client/models/components"
	"github.com/formancehq/ledger/pkg/client"
	"github.com/formancehq/ledger/pkg/client/models/operations"
	"log"
)

func main() {
    ctx := context.Background()

    s := client.New(
        client.WithSecurity(components.Security{
            ClientID: client.String(os.Getenv("FORMANCE_CLIENT_ID")),
            ClientSecret: client.String(os.Getenv("FORMANCE_CLIENT_SECRET")),
        }),
    )

    res, err := s.Ledger.V2.GetBalancesAggregated(ctx, operations.V2GetBalancesAggregatedRequest{
        Ledger: "ledger001",
        RequestBody: map[string]any{
            "key": "<value>",
        },
    })
    if err != nil {
        log.Fatal(err)
    }
    if res.V2AggregateBalancesResponse != nil {
        // handle response
    }
}
```

### Parameters

| Parameter                                                                                              | Type                                                                                                   | Required                                                                                               | Description                                                                                            |
| ------------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------ |
| `ctx`                                                                                                  | [context.Context](https://pkg.go.dev/context#Context)                                                  | :heavy_check_mark:                                                                                     | The context to use for the request.                                                                    |
| `request`                                                                                              | [operations.V2GetBalancesAggregatedRequest](../../models/operations/v2getbalancesaggregatedrequest.md) | :heavy_check_mark:                                                                                     | The request object to use for the request.                                                             |
| `opts`                                                                                                 | [][operations.Option](../../models/operations/option.md)                                               | :heavy_minus_sign:                                                                                     | The options for this request.                                                                          |

### Response

**[*operations.V2GetBalancesAggregatedResponse](../../models/operations/v2getbalancesaggregatedresponse.md), error**

### Errors

| Error Type                | Status Code               | Content Type              |
| ------------------------- | ------------------------- | ------------------------- |
| sdkerrors.V2ErrorResponse | default                   | application/json          |
| sdkerrors.SDKError        | 4XX, 5XX                  | \*/\*                     |

## GetVolumesWithBalances

Get list of volumes with balances for (account/asset)

### Example Usage

```go
package main

import(
	"context"
	"os"
	"github.com/formancehq/ledger/pkg/client/models/components"
	"github.com/formancehq/ledger/pkg/client"
	"github.com/formancehq/ledger/pkg/client/models/operations"
	"log"
)

func main() {
    ctx := context.Background()

    s := client.New(
        client.WithSecurity(components.Security{
            ClientID: client.String(os.Getenv("FORMANCE_CLIENT_ID")),
            ClientSecret: client.String(os.Getenv("FORMANCE_CLIENT_SECRET")),
        }),
    )

    res, err := s.Ledger.V2.GetVolumesWithBalances(ctx, operations.V2GetVolumesWithBalancesRequest{
        PageSize: client.Int64(100),
        Cursor: client.String("aHR0cHM6Ly9nLnBhZ2UvTmVrby1SYW1lbj9zaGFyZQ=="),
        Ledger: "ledger001",
        GroupBy: client.Int64(3),
        RequestBody: map[string]any{
            "key": "<value>",
        },
    })
    if err != nil {
        log.Fatal(err)
    }
    if res.V2VolumesWithBalanceCursorResponse != nil {
        // handle response
    }
}
```

### Parameters

| Parameter                                                                                                | Type                                                                                                     | Required                                                                                                 | Description                                                                                              |
| -------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------- |
| `ctx`                                                                                                    | [context.Context](https://pkg.go.dev/context#Context)                                                    | :heavy_check_mark:                                                                                       | The context to use for the request.                                                                      |
| `request`                                                                                                | [operations.V2GetVolumesWithBalancesRequest](../../models/operations/v2getvolumeswithbalancesrequest.md) | :heavy_check_mark:                                                                                       | The request object to use for the request.                                                               |
| `opts`                                                                                                   | [][operations.Option](../../models/operations/option.md)                                                 | :heavy_minus_sign:                                                                                       | The options for this request.                                                                            |

### Response

**[*operations.V2GetVolumesWithBalancesResponse](../../models/operations/v2getvolumeswithbalancesresponse.md), error**

### Errors

| Error Type                | Status Code               | Content Type              |
| ------------------------- | ------------------------- | ------------------------- |
| sdkerrors.V2ErrorResponse | default                   | application/json          |
| sdkerrors.SDKError        | 4XX, 5XX                  | \*/\*                     |

## ListLogs

List the logs from a ledger, sorted by ID in descending order.

### Example Usage

```go
package main

import(
	"context"
	"os"
	"github.com/formancehq/ledger/pkg/client/models/components"
	"github.com/formancehq/ledger/pkg/client"
	"github.com/formancehq/ledger/pkg/client/models/operations"
	"log"
)

func main() {
    ctx := context.Background()

    s := client.New(
        client.WithSecurity(components.Security{
            ClientID: client.String(os.Getenv("FORMANCE_CLIENT_ID")),
            ClientSecret: client.String(os.Getenv("FORMANCE_CLIENT_SECRET")),
        }),
    )

    res, err := s.Ledger.V2.ListLogs(ctx, operations.V2ListLogsRequest{
        Ledger: "ledger001",
        PageSize: client.Int64(100),
        Cursor: client.String("aHR0cHM6Ly9nLnBhZ2UvTmVrby1SYW1lbj9zaGFyZQ=="),
        RequestBody: map[string]any{
            "key": "<value>",
        },
    })
    if err != nil {
        log.Fatal(err)
    }
    if res.V2LogsCursorResponse != nil {
        // handle response
    }
}
```

### Parameters

| Parameter                                                                    | Type                                                                         | Required                                                                     | Description                                                                  |
| ---------------------------------------------------------------------------- | ---------------------------------------------------------------------------- | ---------------------------------------------------------------------------- | ---------------------------------------------------------------------------- |
| `ctx`                                                                        | [context.Context](https://pkg.go.dev/context#Context)                        | :heavy_check_mark:                                                           | The context to use for the request.                                          |
| `request`                                                                    | [operations.V2ListLogsRequest](../../models/operations/v2listlogsrequest.md) | :heavy_check_mark:                                                           | The request object to use for the request.                                   |
| `opts`                                                                       | [][operations.Option](../../models/operations/option.md)                     | :heavy_minus_sign:                                                           | The options for this request.                                                |

### Response

**[*operations.V2ListLogsResponse](../../models/operations/v2listlogsresponse.md), error**

### Errors

| Error Type                | Status Code               | Content Type              |
| ------------------------- | ------------------------- | ------------------------- |
| sdkerrors.V2ErrorResponse | default                   | application/json          |
| sdkerrors.SDKError        | 4XX, 5XX                  | \*/\*                     |

## ImportLogs

### Example Usage

```go
package main

import(
	"context"
	"os"
	"github.com/formancehq/ledger/pkg/client/models/components"
	"github.com/formancehq/ledger/pkg/client"
	"github.com/formancehq/ledger/pkg/client/models/operations"
	"log"
)

func main() {
    ctx := context.Background()

    s := client.New(
        client.WithSecurity(components.Security{
            ClientID: client.String(os.Getenv("FORMANCE_CLIENT_ID")),
            ClientSecret: client.String(os.Getenv("FORMANCE_CLIENT_SECRET")),
        }),
    )

    v2ImportLogsRequest, fileErr := os.Open("example.file")
    if fileErr != nil {
        panic(fileErr)
    }


    res, err := s.Ledger.V2.ImportLogs(ctx, operations.V2ImportLogsRequest{
        Ledger: "ledger001",
        V2ImportLogsRequest: v2ImportLogsRequest,
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

| Parameter                                                                        | Type                                                                             | Required                                                                         | Description                                                                      |
| -------------------------------------------------------------------------------- | -------------------------------------------------------------------------------- | -------------------------------------------------------------------------------- | -------------------------------------------------------------------------------- |
| `ctx`                                                                            | [context.Context](https://pkg.go.dev/context#Context)                            | :heavy_check_mark:                                                               | The context to use for the request.                                              |
| `request`                                                                        | [operations.V2ImportLogsRequest](../../models/operations/v2importlogsrequest.md) | :heavy_check_mark:                                                               | The request object to use for the request.                                       |
| `opts`                                                                           | [][operations.Option](../../models/operations/option.md)                         | :heavy_minus_sign:                                                               | The options for this request.                                                    |

### Response

**[*operations.V2ImportLogsResponse](../../models/operations/v2importlogsresponse.md), error**

### Errors

| Error Type                | Status Code               | Content Type              |
| ------------------------- | ------------------------- | ------------------------- |
| sdkerrors.V2ErrorResponse | default                   | application/json          |
| sdkerrors.SDKError        | 4XX, 5XX                  | \*/\*                     |

## ExportLogs

Export logs

### Example Usage

```go
package main

import(
	"context"
	"os"
	"github.com/formancehq/ledger/pkg/client/models/components"
	"github.com/formancehq/ledger/pkg/client"
	"github.com/formancehq/ledger/pkg/client/models/operations"
	"log"
)

func main() {
    ctx := context.Background()

    s := client.New(
        client.WithSecurity(components.Security{
            ClientID: client.String(os.Getenv("FORMANCE_CLIENT_ID")),
            ClientSecret: client.String(os.Getenv("FORMANCE_CLIENT_SECRET")),
        }),
    )

    res, err := s.Ledger.V2.ExportLogs(ctx, operations.V2ExportLogsRequest{
        Ledger: "ledger001",
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

| Parameter                                                                        | Type                                                                             | Required                                                                         | Description                                                                      |
| -------------------------------------------------------------------------------- | -------------------------------------------------------------------------------- | -------------------------------------------------------------------------------- | -------------------------------------------------------------------------------- |
| `ctx`                                                                            | [context.Context](https://pkg.go.dev/context#Context)                            | :heavy_check_mark:                                                               | The context to use for the request.                                              |
| `request`                                                                        | [operations.V2ExportLogsRequest](../../models/operations/v2exportlogsrequest.md) | :heavy_check_mark:                                                               | The request object to use for the request.                                       |
| `opts`                                                                           | [][operations.Option](../../models/operations/option.md)                         | :heavy_minus_sign:                                                               | The options for this request.                                                    |

### Response

**[*operations.V2ExportLogsResponse](../../models/operations/v2exportlogsresponse.md), error**

### Errors

| Error Type         | Status Code        | Content Type       |
| ------------------ | ------------------ | ------------------ |
| sdkerrors.SDKError | 4XX, 5XX           | \*/\*              |