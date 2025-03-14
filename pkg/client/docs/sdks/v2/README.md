# V2
(*Ledger.V2*)

### Available Operations

* [DeleteBucket](#deletebucket) - Delete a bucket and mark all its ledgers as deleted
* [ListBuckets](#listbuckets) - List all buckets with their associated ledgers
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

## DeleteBucket

Delete a bucket and mark all its ledgers as deleted

### Example Usage

```go
package main

import(
	"github.com/formancehq/ledger/pkg/client/models/components"
	"github.com/formancehq/ledger/pkg/client"
	"context"
	"log"
)

func main() {
    s := client.New(
        client.WithSecurity(components.Security{
            ClientID: "",
            ClientSecret: "",
        }),
    )
    var name string = "<value>"
    ctx := context.Background()
    res, err := s.Ledger.V2.DeleteBucket(ctx, name)
    if err != nil {
        log.Fatal(err)
    }
    if res != nil {
        // handle response
    }
}
```

### Parameters

| Parameter                                                | Type                                                     | Required                                                 | Description                                              |
| -------------------------------------------------------- | -------------------------------------------------------- | -------------------------------------------------------- | -------------------------------------------------------- |
| `ctx`                                                    | [context.Context](https://pkg.go.dev/context#Context)    | :heavy_check_mark:                                       | The context to use for the request.                      |
| `name`                                                   | *string*                                                 | :heavy_check_mark:                                       | The name of the bucket to delete                         |
| `opts`                                                   | [][operations.Option](../../models/operations/option.md) | :heavy_minus_sign:                                       | The options for this request.                            |


### Response

**[*operations.V2DeleteBucketResponse](../../models/operations/v2deletebucketresponse.md), error**
| Error Object              | Status Code               | Content Type              |
| ------------------------- | ------------------------- | ------------------------- |
| sdkerrors.V2ErrorResponse | default                   | application/json          |
| sdkerrors.SDKError        | 4xx-5xx                   | */*                       |

## ListBuckets

List all buckets with their associated ledgers

### Example Usage

```go
package main

import(
	"github.com/formancehq/ledger/pkg/client/models/components"
	"github.com/formancehq/ledger/pkg/client"
	"context"
	"log"
)

func main() {
    s := client.New(
        client.WithSecurity(components.Security{
            ClientID: "",
            ClientSecret: "",
        }),
    )

    ctx := context.Background()
    res, err := s.Ledger.V2.ListBuckets(ctx)
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

**[*operations.V2ListBucketsResponse](../../models/operations/v2listbucketsresponse.md), error**
| Error Object              | Status Code               | Content Type              |
| ------------------------- | ------------------------- | ------------------------- |
| sdkerrors.V2ErrorResponse | default                   | application/json          |
| sdkerrors.SDKError        | 4xx-5xx                   | */*                       |

## ListLedgers

List ledgers

### Example Usage

```go
package main

import(
	"github.com/formancehq/ledger/pkg/client/models/components"
	"github.com/formancehq/ledger/pkg/client"
	"context"
	"log"
)

func main() {
    s := client.New(
        client.WithSecurity(components.Security{
            ClientID: "",
            ClientSecret: "",
        }),
    )
    var pageSize *int64 = client.Int64(100)

    var cursor *string = client.String("aHR0cHM6Ly9nLnBhZ2UvTmVrby1SYW1lbj9zaGFyZQ==")
    ctx := context.Background()
    res, err := s.Ledger.V2.ListLedgers(ctx, pageSize, cursor)
    if err != nil {
        log.Fatal(err)
    }
    if res.V2LedgerListResponse != nil {
        // handle response
    }
}
```

### Parameters

| Parameter                                                                                                                                                                                                                                                | Type                                                                                                                                                                                                                                                     | Required                                                                                                                                                                                                                                                 | Description                                                                                                                                                                                                                                              | Example                                                                                                                                                                                                                                                  |
| -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `ctx`                                                                                                                                                                                                                                                    | [context.Context](https://pkg.go.dev/context#Context)                                                                                                                                                                                                    | :heavy_check_mark:                                                                                                                                                                                                                                       | The context to use for the request.                                                                                                                                                                                                                      |                                                                                                                                                                                                                                                          |
| `pageSize`                                                                                                                                                                                                                                               | **int64*                                                                                                                                                                                                                                                 | :heavy_minus_sign:                                                                                                                                                                                                                                       | The maximum number of results to return per page.<br/>                                                                                                                                                                                                   | 100                                                                                                                                                                                                                                                      |
| `cursor`                                                                                                                                                                                                                                                 | **string*                                                                                                                                                                                                                                                | :heavy_minus_sign:                                                                                                                                                                                                                                       | Parameter used in pagination requests. Maximum page size is set to 15.<br/>Set to the value of next for the next page of results.<br/>Set to the value of previous for the previous page of results.<br/>No other parameters can be set when this parameter is set.<br/> | aHR0cHM6Ly9nLnBhZ2UvTmVrby1SYW1lbj9zaGFyZQ==                                                                                                                                                                                                             |
| `opts`                                                                                                                                                                                                                                                   | [][operations.Option](../../models/operations/option.md)                                                                                                                                                                                                 | :heavy_minus_sign:                                                                                                                                                                                                                                       | The options for this request.                                                                                                                                                                                                                            |                                                                                                                                                                                                                                                          |


### Response

**[*operations.V2ListLedgersResponse](../../models/operations/v2listledgersresponse.md), error**
| Error Object              | Status Code               | Content Type              |
| ------------------------- | ------------------------- | ------------------------- |
| sdkerrors.V2ErrorResponse | default                   | application/json          |
| sdkerrors.SDKError        | 4xx-5xx                   | */*                       |

## GetLedger

Get a ledger

### Example Usage

```go
package main

import(
	"github.com/formancehq/ledger/pkg/client/models/components"
	"github.com/formancehq/ledger/pkg/client"
	"context"
	"log"
)

func main() {
    s := client.New(
        client.WithSecurity(components.Security{
            ClientID: "",
            ClientSecret: "",
        }),
    )
    var ledger string = "ledger001"
    ctx := context.Background()
    res, err := s.Ledger.V2.GetLedger(ctx, ledger)
    if err != nil {
        log.Fatal(err)
    }
    if res.V2GetLedgerResponse != nil {
        // handle response
    }
}
```

### Parameters

| Parameter                                                | Type                                                     | Required                                                 | Description                                              | Example                                                  |
| -------------------------------------------------------- | -------------------------------------------------------- | -------------------------------------------------------- | -------------------------------------------------------- | -------------------------------------------------------- |
| `ctx`                                                    | [context.Context](https://pkg.go.dev/context#Context)    | :heavy_check_mark:                                       | The context to use for the request.                      |                                                          |
| `ledger`                                                 | *string*                                                 | :heavy_check_mark:                                       | Name of the ledger.                                      | ledger001                                                |
| `opts`                                                   | [][operations.Option](../../models/operations/option.md) | :heavy_minus_sign:                                       | The options for this request.                            |                                                          |


### Response

**[*operations.V2GetLedgerResponse](../../models/operations/v2getledgerresponse.md), error**
| Error Object              | Status Code               | Content Type              |
| ------------------------- | ------------------------- | ------------------------- |
| sdkerrors.V2ErrorResponse | default                   | application/json          |
| sdkerrors.SDKError        | 4xx-5xx                   | */*                       |

## CreateLedger

Create a ledger

### Example Usage

```go
package main

import(
	"github.com/formancehq/ledger/pkg/client/models/components"
	"github.com/formancehq/ledger/pkg/client"
	"context"
	"log"
)

func main() {
    s := client.New(
        client.WithSecurity(components.Security{
            ClientID: "",
            ClientSecret: "",
        }),
    )
    var ledger string = "ledger001"

    var v2CreateLedgerRequest *components.V2CreateLedgerRequest = &components.V2CreateLedgerRequest{
        Metadata: map[string]string{
            "admin": "true",
        },
    }
    ctx := context.Background()
    res, err := s.Ledger.V2.CreateLedger(ctx, ledger, v2CreateLedgerRequest)
    if err != nil {
        log.Fatal(err)
    }
    if res != nil {
        // handle response
    }
}
```

### Parameters

| Parameter                                                                             | Type                                                                                  | Required                                                                              | Description                                                                           | Example                                                                               |
| ------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------- |
| `ctx`                                                                                 | [context.Context](https://pkg.go.dev/context#Context)                                 | :heavy_check_mark:                                                                    | The context to use for the request.                                                   |                                                                                       |
| `ledger`                                                                              | *string*                                                                              | :heavy_check_mark:                                                                    | Name of the ledger.                                                                   | ledger001                                                                             |
| `v2CreateLedgerRequest`                                                               | [*components.V2CreateLedgerRequest](../../models/components/v2createledgerrequest.md) | :heavy_minus_sign:                                                                    | N/A                                                                                   |                                                                                       |
| `opts`                                                                                | [][operations.Option](../../models/operations/option.md)                              | :heavy_minus_sign:                                                                    | The options for this request.                                                         |                                                                                       |


### Response

**[*operations.V2CreateLedgerResponse](../../models/operations/v2createledgerresponse.md), error**
| Error Object              | Status Code               | Content Type              |
| ------------------------- | ------------------------- | ------------------------- |
| sdkerrors.V2ErrorResponse | default                   | application/json          |
| sdkerrors.SDKError        | 4xx-5xx                   | */*                       |

## UpdateLedgerMetadata

Update ledger metadata

### Example Usage

```go
package main

import(
	"github.com/formancehq/ledger/pkg/client/models/components"
	"github.com/formancehq/ledger/pkg/client"
	"context"
	"log"
)

func main() {
    s := client.New(
        client.WithSecurity(components.Security{
            ClientID: "",
            ClientSecret: "",
        }),
    )
    var ledger string = "ledger001"

    var requestBody map[string]string = map[string]string{
        "admin": "true",
    }
    ctx := context.Background()
    res, err := s.Ledger.V2.UpdateLedgerMetadata(ctx, ledger, requestBody)
    if err != nil {
        log.Fatal(err)
    }
    if res != nil {
        // handle response
    }
}
```

### Parameters

| Parameter                                                | Type                                                     | Required                                                 | Description                                              | Example                                                  |
| -------------------------------------------------------- | -------------------------------------------------------- | -------------------------------------------------------- | -------------------------------------------------------- | -------------------------------------------------------- |
| `ctx`                                                    | [context.Context](https://pkg.go.dev/context#Context)    | :heavy_check_mark:                                       | The context to use for the request.                      |                                                          |
| `ledger`                                                 | *string*                                                 | :heavy_check_mark:                                       | Name of the ledger.                                      | ledger001                                                |
| `requestBody`                                            | map[string]*string*                                      | :heavy_minus_sign:                                       | N/A                                                      | {<br/>"admin": "true"<br/>}                              |
| `opts`                                                   | [][operations.Option](../../models/operations/option.md) | :heavy_minus_sign:                                       | The options for this request.                            |                                                          |


### Response

**[*operations.V2UpdateLedgerMetadataResponse](../../models/operations/v2updateledgermetadataresponse.md), error**
| Error Object              | Status Code               | Content Type              |
| ------------------------- | ------------------------- | ------------------------- |
| sdkerrors.V2ErrorResponse | default                   | application/json          |
| sdkerrors.SDKError        | 4xx-5xx                   | */*                       |

## DeleteLedgerMetadata

Delete ledger metadata by key

### Example Usage

```go
package main

import(
	"github.com/formancehq/ledger/pkg/client/models/components"
	"github.com/formancehq/ledger/pkg/client"
	"context"
	"log"
)

func main() {
    s := client.New(
        client.WithSecurity(components.Security{
            ClientID: "",
            ClientSecret: "",
        }),
    )
    var ledger string = "ledger001"

    var key string = "foo"
    ctx := context.Background()
    res, err := s.Ledger.V2.DeleteLedgerMetadata(ctx, ledger, key)
    if err != nil {
        log.Fatal(err)
    }
    if res != nil {
        // handle response
    }
}
```

### Parameters

| Parameter                                                | Type                                                     | Required                                                 | Description                                              | Example                                                  |
| -------------------------------------------------------- | -------------------------------------------------------- | -------------------------------------------------------- | -------------------------------------------------------- | -------------------------------------------------------- |
| `ctx`                                                    | [context.Context](https://pkg.go.dev/context#Context)    | :heavy_check_mark:                                       | The context to use for the request.                      |                                                          |
| `ledger`                                                 | *string*                                                 | :heavy_check_mark:                                       | Name of the ledger.                                      | ledger001                                                |
| `key`                                                    | *string*                                                 | :heavy_check_mark:                                       | Key to remove.                                           | foo                                                      |
| `opts`                                                   | [][operations.Option](../../models/operations/option.md) | :heavy_minus_sign:                                       | The options for this request.                            |                                                          |


### Response

**[*operations.V2DeleteLedgerMetadataResponse](../../models/operations/v2deleteledgermetadataresponse.md), error**
| Error Object              | Status Code               | Content Type              |
| ------------------------- | ------------------------- | ------------------------- |
| sdkerrors.V2ErrorResponse | default                   | application/json          |
| sdkerrors.SDKError        | 4xx-5xx                   | */*                       |

## GetLedgerInfo

Get information about a ledger

### Example Usage

```go
package main

import(
	"github.com/formancehq/ledger/pkg/client/models/components"
	"github.com/formancehq/ledger/pkg/client"
	"context"
	"log"
)

func main() {
    s := client.New(
        client.WithSecurity(components.Security{
            ClientID: "",
            ClientSecret: "",
        }),
    )
    var ledger string = "ledger001"
    ctx := context.Background()
    res, err := s.Ledger.V2.GetLedgerInfo(ctx, ledger)
    if err != nil {
        log.Fatal(err)
    }
    if res.V2LedgerInfoResponse != nil {
        // handle response
    }
}
```

### Parameters

| Parameter                                                | Type                                                     | Required                                                 | Description                                              | Example                                                  |
| -------------------------------------------------------- | -------------------------------------------------------- | -------------------------------------------------------- | -------------------------------------------------------- | -------------------------------------------------------- |
| `ctx`                                                    | [context.Context](https://pkg.go.dev/context#Context)    | :heavy_check_mark:                                       | The context to use for the request.                      |                                                          |
| `ledger`                                                 | *string*                                                 | :heavy_check_mark:                                       | Name of the ledger.                                      | ledger001                                                |
| `opts`                                                   | [][operations.Option](../../models/operations/option.md) | :heavy_minus_sign:                                       | The options for this request.                            |                                                          |


### Response

**[*operations.V2GetLedgerInfoResponse](../../models/operations/v2getledgerinforesponse.md), error**
| Error Object              | Status Code               | Content Type              |
| ------------------------- | ------------------------- | ------------------------- |
| sdkerrors.V2ErrorResponse | default                   | application/json          |
| sdkerrors.SDKError        | 4xx-5xx                   | */*                       |

## CreateBulk

Bulk request

### Example Usage

```go
package main

import(
	"github.com/formancehq/ledger/pkg/client/models/components"
	"github.com/formancehq/ledger/pkg/client"
	"github.com/formancehq/ledger/pkg/client/models/operations"
	"math/big"
	"context"
	"log"
)

func main() {
    s := client.New(
        client.WithSecurity(components.Security{
            ClientID: "",
            ClientSecret: "",
        }),
    )
    request := operations.V2CreateBulkRequest{
        Ledger: "ledger001",
        ContinueOnFailure: client.Bool(true),
        Atomic: client.Bool(true),
        Parallel: client.Bool(true),
        RequestBody: []components.V2BulkElement{
            components.CreateV2BulkElementV2BulkElementCreateTransaction(
                components.V2BulkElementCreateTransaction{
                    Action: "<value>",
                    Data: &components.V2PostTransaction{
                        Postings: []components.V2Posting{
                            components.V2Posting{
                                Amount: big.NewInt(100),
                                Asset: "COIN",
                                Destination: "users:002",
                                Source: "users:001",
                            },
                        },
                        Script: &components.V2PostTransactionScript{
                            Plain: "vars {
                        account $user
                        }
                        send [COIN 10] (
                        	source = @world
                        	destination = $user
                        )
                        ",
                            Vars: map[string]string{
                                "user": "users:042",
                            },
                        },
                        Reference: client.String("ref:001"),
                        Metadata: map[string]string{
                            "admin": "true",
                        },
                    },
                },
            ),
        },
    }
    ctx := context.Background()
    res, err := s.Ledger.V2.CreateBulk(ctx, request)
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
| Error Object              | Status Code               | Content Type              |
| ------------------------- | ------------------------- | ------------------------- |
| sdkerrors.V2ErrorResponse | default                   | application/json          |
| sdkerrors.SDKError        | 4xx-5xx                   | */*                       |

## CountAccounts

Count the accounts from a ledger

### Example Usage

```go
package main

import(
	"github.com/formancehq/ledger/pkg/client/models/components"
	"github.com/formancehq/ledger/pkg/client"
	"time"
	"context"
	"log"
)

func main() {
    s := client.New(
        client.WithSecurity(components.Security{
            ClientID: "",
            ClientSecret: "",
        }),
    )
    var ledger string = "ledger001"
    ctx := context.Background()
    res, err := s.Ledger.V2.CountAccounts(ctx, ledger, nil, nil)
    if err != nil {
        log.Fatal(err)
    }
    if res != nil {
        // handle response
    }
}
```

### Parameters

| Parameter                                                | Type                                                     | Required                                                 | Description                                              | Example                                                  |
| -------------------------------------------------------- | -------------------------------------------------------- | -------------------------------------------------------- | -------------------------------------------------------- | -------------------------------------------------------- |
| `ctx`                                                    | [context.Context](https://pkg.go.dev/context#Context)    | :heavy_check_mark:                                       | The context to use for the request.                      |                                                          |
| `ledger`                                                 | *string*                                                 | :heavy_check_mark:                                       | Name of the ledger.                                      | ledger001                                                |
| `pit`                                                    | [*time.Time](https://pkg.go.dev/time#Time)               | :heavy_minus_sign:                                       | N/A                                                      |                                                          |
| `requestBody`                                            | map[string]*any*                                         | :heavy_minus_sign:                                       | N/A                                                      |                                                          |
| `opts`                                                   | [][operations.Option](../../models/operations/option.md) | :heavy_minus_sign:                                       | The options for this request.                            |                                                          |


### Response

**[*operations.V2CountAccountsResponse](../../models/operations/v2countaccountsresponse.md), error**
| Error Object              | Status Code               | Content Type              |
| ------------------------- | ------------------------- | ------------------------- |
| sdkerrors.V2ErrorResponse | default                   | application/json          |
| sdkerrors.SDKError        | 4xx-5xx                   | */*                       |

## ListAccounts

List accounts from a ledger, sorted by address in descending order.

### Example Usage

```go
package main

import(
	"github.com/formancehq/ledger/pkg/client/models/components"
	"github.com/formancehq/ledger/pkg/client"
	"github.com/formancehq/ledger/pkg/client/models/operations"
	"context"
	"log"
)

func main() {
    s := client.New(
        client.WithSecurity(components.Security{
            ClientID: "",
            ClientSecret: "",
        }),
    )
    request := operations.V2ListAccountsRequest{
        Ledger: "ledger001",
        PageSize: client.Int64(100),
        Cursor: client.String("aHR0cHM6Ly9nLnBhZ2UvTmVrby1SYW1lbj9zaGFyZQ=="),
    }
    ctx := context.Background()
    res, err := s.Ledger.V2.ListAccounts(ctx, request)
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
| Error Object              | Status Code               | Content Type              |
| ------------------------- | ------------------------- | ------------------------- |
| sdkerrors.V2ErrorResponse | default                   | application/json          |
| sdkerrors.SDKError        | 4xx-5xx                   | */*                       |

## GetAccount

Get account by its address

### Example Usage

```go
package main

import(
	"github.com/formancehq/ledger/pkg/client/models/components"
	"github.com/formancehq/ledger/pkg/client"
	"time"
	"context"
	"log"
)

func main() {
    s := client.New(
        client.WithSecurity(components.Security{
            ClientID: "",
            ClientSecret: "",
        }),
    )
    var ledger string = "ledger001"

    var address string = "users:001"
    ctx := context.Background()
    res, err := s.Ledger.V2.GetAccount(ctx, ledger, address, nil, nil)
    if err != nil {
        log.Fatal(err)
    }
    if res.V2AccountResponse != nil {
        // handle response
    }
}
```

### Parameters

| Parameter                                                                                                    | Type                                                                                                         | Required                                                                                                     | Description                                                                                                  | Example                                                                                                      |
| ------------------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------ |
| `ctx`                                                                                                        | [context.Context](https://pkg.go.dev/context#Context)                                                        | :heavy_check_mark:                                                                                           | The context to use for the request.                                                                          |                                                                                                              |
| `ledger`                                                                                                     | *string*                                                                                                     | :heavy_check_mark:                                                                                           | Name of the ledger.                                                                                          | ledger001                                                                                                    |
| `address`                                                                                                    | *string*                                                                                                     | :heavy_check_mark:                                                                                           | Exact address of the account. It must match the following regular expressions pattern:<br/>```<br/>^\w+(:\w+)*$<br/>```<br/> | users:001                                                                                                    |
| `expand`                                                                                                     | **string*                                                                                                    | :heavy_minus_sign:                                                                                           | N/A                                                                                                          |                                                                                                              |
| `pit`                                                                                                        | [*time.Time](https://pkg.go.dev/time#Time)                                                                   | :heavy_minus_sign:                                                                                           | N/A                                                                                                          |                                                                                                              |
| `opts`                                                                                                       | [][operations.Option](../../models/operations/option.md)                                                     | :heavy_minus_sign:                                                                                           | The options for this request.                                                                                |                                                                                                              |


### Response

**[*operations.V2GetAccountResponse](../../models/operations/v2getaccountresponse.md), error**
| Error Object              | Status Code               | Content Type              |
| ------------------------- | ------------------------- | ------------------------- |
| sdkerrors.V2ErrorResponse | default                   | application/json          |
| sdkerrors.SDKError        | 4xx-5xx                   | */*                       |

## AddMetadataToAccount

Add metadata to an account

### Example Usage

```go
package main

import(
	"github.com/formancehq/ledger/pkg/client/models/components"
	"github.com/formancehq/ledger/pkg/client"
	"github.com/formancehq/ledger/pkg/client/models/operations"
	"context"
	"log"
)

func main() {
    s := client.New(
        client.WithSecurity(components.Security{
            ClientID: "",
            ClientSecret: "",
        }),
    )
    request := operations.V2AddMetadataToAccountRequest{
        Ledger: "ledger001",
        Address: "users:001",
        DryRun: client.Bool(true),
        RequestBody: map[string]string{
            "admin": "true",
        },
    }
    ctx := context.Background()
    res, err := s.Ledger.V2.AddMetadataToAccount(ctx, request)
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
| Error Object              | Status Code               | Content Type              |
| ------------------------- | ------------------------- | ------------------------- |
| sdkerrors.V2ErrorResponse | default                   | application/json          |
| sdkerrors.SDKError        | 4xx-5xx                   | */*                       |

## DeleteAccountMetadata

Delete metadata by key

### Example Usage

```go
package main

import(
	"github.com/formancehq/ledger/pkg/client/models/components"
	"github.com/formancehq/ledger/pkg/client"
	"context"
	"log"
)

func main() {
    s := client.New(
        client.WithSecurity(components.Security{
            ClientID: "",
            ClientSecret: "",
        }),
    )
    var ledger string = "ledger001"

    var address string = "<value>"

    var key string = "foo"
    ctx := context.Background()
    res, err := s.Ledger.V2.DeleteAccountMetadata(ctx, ledger, address, key)
    if err != nil {
        log.Fatal(err)
    }
    if res != nil {
        // handle response
    }
}
```

### Parameters

| Parameter                                                | Type                                                     | Required                                                 | Description                                              | Example                                                  |
| -------------------------------------------------------- | -------------------------------------------------------- | -------------------------------------------------------- | -------------------------------------------------------- | -------------------------------------------------------- |
| `ctx`                                                    | [context.Context](https://pkg.go.dev/context#Context)    | :heavy_check_mark:                                       | The context to use for the request.                      |                                                          |
| `ledger`                                                 | *string*                                                 | :heavy_check_mark:                                       | Name of the ledger.                                      | ledger001                                                |
| `address`                                                | *string*                                                 | :heavy_check_mark:                                       | Account address                                          |                                                          |
| `key`                                                    | *string*                                                 | :heavy_check_mark:                                       | The key to remove.                                       | foo                                                      |
| `opts`                                                   | [][operations.Option](../../models/operations/option.md) | :heavy_minus_sign:                                       | The options for this request.                            |                                                          |


### Response

**[*operations.V2DeleteAccountMetadataResponse](../../models/operations/v2deleteaccountmetadataresponse.md), error**
| Error Object              | Status Code               | Content Type              |
| ------------------------- | ------------------------- | ------------------------- |
| sdkerrors.V2ErrorResponse | default                   | application/json          |
| sdkerrors.SDKError        | 4xx-5xx                   | */*                       |

## ReadStats

Get statistics from a ledger. (aggregate metrics on accounts and transactions)


### Example Usage

```go
package main

import(
	"github.com/formancehq/ledger/pkg/client/models/components"
	"github.com/formancehq/ledger/pkg/client"
	"context"
	"log"
)

func main() {
    s := client.New(
        client.WithSecurity(components.Security{
            ClientID: "",
            ClientSecret: "",
        }),
    )
    var ledger string = "ledger001"
    ctx := context.Background()
    res, err := s.Ledger.V2.ReadStats(ctx, ledger)
    if err != nil {
        log.Fatal(err)
    }
    if res.V2StatsResponse != nil {
        // handle response
    }
}
```

### Parameters

| Parameter                                                | Type                                                     | Required                                                 | Description                                              | Example                                                  |
| -------------------------------------------------------- | -------------------------------------------------------- | -------------------------------------------------------- | -------------------------------------------------------- | -------------------------------------------------------- |
| `ctx`                                                    | [context.Context](https://pkg.go.dev/context#Context)    | :heavy_check_mark:                                       | The context to use for the request.                      |                                                          |
| `ledger`                                                 | *string*                                                 | :heavy_check_mark:                                       | name of the ledger                                       | ledger001                                                |
| `opts`                                                   | [][operations.Option](../../models/operations/option.md) | :heavy_minus_sign:                                       | The options for this request.                            |                                                          |


### Response

**[*operations.V2ReadStatsResponse](../../models/operations/v2readstatsresponse.md), error**
| Error Object              | Status Code               | Content Type              |
| ------------------------- | ------------------------- | ------------------------- |
| sdkerrors.V2ErrorResponse | default                   | application/json          |
| sdkerrors.SDKError        | 4xx-5xx                   | */*                       |

## CountTransactions

Count the transactions from a ledger

### Example Usage

```go
package main

import(
	"github.com/formancehq/ledger/pkg/client/models/components"
	"github.com/formancehq/ledger/pkg/client"
	"time"
	"context"
	"log"
)

func main() {
    s := client.New(
        client.WithSecurity(components.Security{
            ClientID: "",
            ClientSecret: "",
        }),
    )
    var ledger string = "ledger001"
    ctx := context.Background()
    res, err := s.Ledger.V2.CountTransactions(ctx, ledger, nil, nil)
    if err != nil {
        log.Fatal(err)
    }
    if res != nil {
        // handle response
    }
}
```

### Parameters

| Parameter                                                | Type                                                     | Required                                                 | Description                                              | Example                                                  |
| -------------------------------------------------------- | -------------------------------------------------------- | -------------------------------------------------------- | -------------------------------------------------------- | -------------------------------------------------------- |
| `ctx`                                                    | [context.Context](https://pkg.go.dev/context#Context)    | :heavy_check_mark:                                       | The context to use for the request.                      |                                                          |
| `ledger`                                                 | *string*                                                 | :heavy_check_mark:                                       | Name of the ledger.                                      | ledger001                                                |
| `pit`                                                    | [*time.Time](https://pkg.go.dev/time#Time)               | :heavy_minus_sign:                                       | N/A                                                      |                                                          |
| `requestBody`                                            | map[string]*any*                                         | :heavy_minus_sign:                                       | N/A                                                      |                                                          |
| `opts`                                                   | [][operations.Option](../../models/operations/option.md) | :heavy_minus_sign:                                       | The options for this request.                            |                                                          |


### Response

**[*operations.V2CountTransactionsResponse](../../models/operations/v2counttransactionsresponse.md), error**
| Error Object              | Status Code               | Content Type              |
| ------------------------- | ------------------------- | ------------------------- |
| sdkerrors.V2ErrorResponse | default                   | application/json          |
| sdkerrors.SDKError        | 4xx-5xx                   | */*                       |

## ListTransactions

List transactions from a ledger, sorted by id in descending order.

### Example Usage

```go
package main

import(
	"github.com/formancehq/ledger/pkg/client/models/components"
	"github.com/formancehq/ledger/pkg/client"
	"github.com/formancehq/ledger/pkg/client/models/operations"
	"context"
	"log"
)

func main() {
    s := client.New(
        client.WithSecurity(components.Security{
            ClientID: "",
            ClientSecret: "",
        }),
    )
    request := operations.V2ListTransactionsRequest{
        Ledger: "ledger001",
        PageSize: client.Int64(100),
        Cursor: client.String("aHR0cHM6Ly9nLnBhZ2UvTmVrby1SYW1lbj9zaGFyZQ=="),
    }
    ctx := context.Background()
    res, err := s.Ledger.V2.ListTransactions(ctx, request)
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
| Error Object              | Status Code               | Content Type              |
| ------------------------- | ------------------------- | ------------------------- |
| sdkerrors.V2ErrorResponse | default                   | application/json          |
| sdkerrors.SDKError        | 4xx-5xx                   | */*                       |

## CreateTransaction

Create a new transaction to a ledger

### Example Usage

```go
package main

import(
	"github.com/formancehq/ledger/pkg/client/models/components"
	"github.com/formancehq/ledger/pkg/client"
	"github.com/formancehq/ledger/pkg/client/models/operations"
	"math/big"
	"context"
	"log"
)

func main() {
    s := client.New(
        client.WithSecurity(components.Security{
            ClientID: "",
            ClientSecret: "",
        }),
    )
    request := operations.V2CreateTransactionRequest{
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
            },
            Script: &components.V2PostTransactionScript{
                Plain: "vars {
            account $user
            }
            send [COIN 10] (
            	source = @world
            	destination = $user
            )
            ",
                Vars: map[string]string{
                    "user": "users:042",
                },
            },
            Reference: client.String("ref:001"),
            Metadata: map[string]string{
                "admin": "true",
            },
        },
    }
    ctx := context.Background()
    res, err := s.Ledger.V2.CreateTransaction(ctx, request)
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
| Error Object              | Status Code               | Content Type              |
| ------------------------- | ------------------------- | ------------------------- |
| sdkerrors.V2ErrorResponse | default                   | application/json          |
| sdkerrors.SDKError        | 4xx-5xx                   | */*                       |

## GetTransaction

Get transaction from a ledger by its ID

### Example Usage

```go
package main

import(
	"github.com/formancehq/ledger/pkg/client/models/components"
	"github.com/formancehq/ledger/pkg/client"
	"math/big"
	"time"
	"context"
	"log"
)

func main() {
    s := client.New(
        client.WithSecurity(components.Security{
            ClientID: "",
            ClientSecret: "",
        }),
    )
    var ledger string = "ledger001"

    var id *big.Int = big.NewInt(1234)
    ctx := context.Background()
    res, err := s.Ledger.V2.GetTransaction(ctx, ledger, id, nil, nil)
    if err != nil {
        log.Fatal(err)
    }
    if res.V2GetTransactionResponse != nil {
        // handle response
    }
}
```

### Parameters

| Parameter                                                | Type                                                     | Required                                                 | Description                                              | Example                                                  |
| -------------------------------------------------------- | -------------------------------------------------------- | -------------------------------------------------------- | -------------------------------------------------------- | -------------------------------------------------------- |
| `ctx`                                                    | [context.Context](https://pkg.go.dev/context#Context)    | :heavy_check_mark:                                       | The context to use for the request.                      |                                                          |
| `ledger`                                                 | *string*                                                 | :heavy_check_mark:                                       | Name of the ledger.                                      | ledger001                                                |
| `id`                                                     | [*big.Int](https://pkg.go.dev/math/big#Int)              | :heavy_check_mark:                                       | Transaction ID.                                          | 1234                                                     |
| `expand`                                                 | **string*                                                | :heavy_minus_sign:                                       | N/A                                                      |                                                          |
| `pit`                                                    | [*time.Time](https://pkg.go.dev/time#Time)               | :heavy_minus_sign:                                       | N/A                                                      |                                                          |
| `opts`                                                   | [][operations.Option](../../models/operations/option.md) | :heavy_minus_sign:                                       | The options for this request.                            |                                                          |


### Response

**[*operations.V2GetTransactionResponse](../../models/operations/v2gettransactionresponse.md), error**
| Error Object              | Status Code               | Content Type              |
| ------------------------- | ------------------------- | ------------------------- |
| sdkerrors.V2ErrorResponse | default                   | application/json          |
| sdkerrors.SDKError        | 4xx-5xx                   | */*                       |

## AddMetadataOnTransaction

Set the metadata of a transaction by its ID

### Example Usage

```go
package main

import(
	"github.com/formancehq/ledger/pkg/client/models/components"
	"github.com/formancehq/ledger/pkg/client"
	"github.com/formancehq/ledger/pkg/client/models/operations"
	"math/big"
	"context"
	"log"
)

func main() {
    s := client.New(
        client.WithSecurity(components.Security{
            ClientID: "",
            ClientSecret: "",
        }),
    )
    request := operations.V2AddMetadataOnTransactionRequest{
        Ledger: "ledger001",
        ID: big.NewInt(1234),
        DryRun: client.Bool(true),
        RequestBody: map[string]string{
            "admin": "true",
        },
    }
    ctx := context.Background()
    res, err := s.Ledger.V2.AddMetadataOnTransaction(ctx, request)
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
| Error Object              | Status Code               | Content Type              |
| ------------------------- | ------------------------- | ------------------------- |
| sdkerrors.V2ErrorResponse | default                   | application/json          |
| sdkerrors.SDKError        | 4xx-5xx                   | */*                       |

## DeleteTransactionMetadata

Delete metadata by key

### Example Usage

```go
package main

import(
	"github.com/formancehq/ledger/pkg/client/models/components"
	"github.com/formancehq/ledger/pkg/client"
	"math/big"
	"context"
	"log"
)

func main() {
    s := client.New(
        client.WithSecurity(components.Security{
            ClientID: "",
            ClientSecret: "",
        }),
    )
    var ledger string = "ledger001"

    var id *big.Int = big.NewInt(1234)

    var key string = "foo"
    ctx := context.Background()
    res, err := s.Ledger.V2.DeleteTransactionMetadata(ctx, ledger, id, key)
    if err != nil {
        log.Fatal(err)
    }
    if res != nil {
        // handle response
    }
}
```

### Parameters

| Parameter                                                | Type                                                     | Required                                                 | Description                                              | Example                                                  |
| -------------------------------------------------------- | -------------------------------------------------------- | -------------------------------------------------------- | -------------------------------------------------------- | -------------------------------------------------------- |
| `ctx`                                                    | [context.Context](https://pkg.go.dev/context#Context)    | :heavy_check_mark:                                       | The context to use for the request.                      |                                                          |
| `ledger`                                                 | *string*                                                 | :heavy_check_mark:                                       | Name of the ledger.                                      | ledger001                                                |
| `id`                                                     | [*big.Int](https://pkg.go.dev/math/big#Int)              | :heavy_check_mark:                                       | Transaction ID.                                          | 1234                                                     |
| `key`                                                    | *string*                                                 | :heavy_check_mark:                                       | The key to remove.                                       | foo                                                      |
| `opts`                                                   | [][operations.Option](../../models/operations/option.md) | :heavy_minus_sign:                                       | The options for this request.                            |                                                          |


### Response

**[*operations.V2DeleteTransactionMetadataResponse](../../models/operations/v2deletetransactionmetadataresponse.md), error**
| Error Object              | Status Code               | Content Type              |
| ------------------------- | ------------------------- | ------------------------- |
| sdkerrors.V2ErrorResponse | default                   | application/json          |
| sdkerrors.SDKError        | 4xx-5xx                   | */*                       |

## RevertTransaction

Revert a ledger transaction by its ID

### Example Usage

```go
package main

import(
	"github.com/formancehq/ledger/pkg/client/models/components"
	"github.com/formancehq/ledger/pkg/client"
	"github.com/formancehq/ledger/pkg/client/models/operations"
	"math/big"
	"context"
	"log"
)

func main() {
    s := client.New(
        client.WithSecurity(components.Security{
            ClientID: "",
            ClientSecret: "",
        }),
    )
    request := operations.V2RevertTransactionRequest{
        Ledger: "ledger001",
        ID: big.NewInt(1234),
        DryRun: client.Bool(true),
    }
    ctx := context.Background()
    res, err := s.Ledger.V2.RevertTransaction(ctx, request)
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
| Error Object              | Status Code               | Content Type              |
| ------------------------- | ------------------------- | ------------------------- |
| sdkerrors.V2ErrorResponse | default                   | application/json          |
| sdkerrors.SDKError        | 4xx-5xx                   | */*                       |

## GetBalancesAggregated

Get the aggregated balances from selected accounts

### Example Usage

```go
package main

import(
	"github.com/formancehq/ledger/pkg/client/models/components"
	"github.com/formancehq/ledger/pkg/client"
	"time"
	"context"
	"log"
)

func main() {
    s := client.New(
        client.WithSecurity(components.Security{
            ClientID: "",
            ClientSecret: "",
        }),
    )
    var ledger string = "ledger001"
    ctx := context.Background()
    res, err := s.Ledger.V2.GetBalancesAggregated(ctx, ledger, nil, nil, nil)
    if err != nil {
        log.Fatal(err)
    }
    if res.V2AggregateBalancesResponse != nil {
        // handle response
    }
}
```

### Parameters

| Parameter                                                | Type                                                     | Required                                                 | Description                                              | Example                                                  |
| -------------------------------------------------------- | -------------------------------------------------------- | -------------------------------------------------------- | -------------------------------------------------------- | -------------------------------------------------------- |
| `ctx`                                                    | [context.Context](https://pkg.go.dev/context#Context)    | :heavy_check_mark:                                       | The context to use for the request.                      |                                                          |
| `ledger`                                                 | *string*                                                 | :heavy_check_mark:                                       | Name of the ledger.                                      | ledger001                                                |
| `pit`                                                    | [*time.Time](https://pkg.go.dev/time#Time)               | :heavy_minus_sign:                                       | N/A                                                      |                                                          |
| `useInsertionDate`                                       | **bool*                                                  | :heavy_minus_sign:                                       | Use insertion date instead of effective date             |                                                          |
| `requestBody`                                            | map[string]*any*                                         | :heavy_minus_sign:                                       | N/A                                                      |                                                          |
| `opts`                                                   | [][operations.Option](../../models/operations/option.md) | :heavy_minus_sign:                                       | The options for this request.                            |                                                          |


### Response

**[*operations.V2GetBalancesAggregatedResponse](../../models/operations/v2getbalancesaggregatedresponse.md), error**
| Error Object              | Status Code               | Content Type              |
| ------------------------- | ------------------------- | ------------------------- |
| sdkerrors.V2ErrorResponse | default                   | application/json          |
| sdkerrors.SDKError        | 4xx-5xx                   | */*                       |

## GetVolumesWithBalances

Get list of volumes with balances for (account/asset)

### Example Usage

```go
package main

import(
	"github.com/formancehq/ledger/pkg/client/models/components"
	"github.com/formancehq/ledger/pkg/client"
	"github.com/formancehq/ledger/pkg/client/models/operations"
	"context"
	"log"
)

func main() {
    s := client.New(
        client.WithSecurity(components.Security{
            ClientID: "",
            ClientSecret: "",
        }),
    )
    request := operations.V2GetVolumesWithBalancesRequest{
        PageSize: client.Int64(100),
        Cursor: client.String("aHR0cHM6Ly9nLnBhZ2UvTmVrby1SYW1lbj9zaGFyZQ=="),
        Ledger: "ledger001",
        GroupBy: client.Int64(3),
    }
    ctx := context.Background()
    res, err := s.Ledger.V2.GetVolumesWithBalances(ctx, request)
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
| Error Object              | Status Code               | Content Type              |
| ------------------------- | ------------------------- | ------------------------- |
| sdkerrors.V2ErrorResponse | default                   | application/json          |
| sdkerrors.SDKError        | 4xx-5xx                   | */*                       |

## ListLogs

List the logs from a ledger, sorted by ID in descending order.

### Example Usage

```go
package main

import(
	"github.com/formancehq/ledger/pkg/client/models/components"
	"github.com/formancehq/ledger/pkg/client"
	"github.com/formancehq/ledger/pkg/client/models/operations"
	"context"
	"log"
)

func main() {
    s := client.New(
        client.WithSecurity(components.Security{
            ClientID: "",
            ClientSecret: "",
        }),
    )
    request := operations.V2ListLogsRequest{
        Ledger: "ledger001",
        PageSize: client.Int64(100),
        Cursor: client.String("aHR0cHM6Ly9nLnBhZ2UvTmVrby1SYW1lbj9zaGFyZQ=="),
    }
    ctx := context.Background()
    res, err := s.Ledger.V2.ListLogs(ctx, request)
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
| Error Object              | Status Code               | Content Type              |
| ------------------------- | ------------------------- | ------------------------- |
| sdkerrors.V2ErrorResponse | default                   | application/json          |
| sdkerrors.SDKError        | 4xx-5xx                   | */*                       |

## ImportLogs

### Example Usage

```go
package main

import(
	"github.com/formancehq/ledger/pkg/client/models/components"
	"github.com/formancehq/ledger/pkg/client"
	"context"
	"log"
)

func main() {
    s := client.New(
        client.WithSecurity(components.Security{
            ClientID: "",
            ClientSecret: "",
        }),
    )
    var ledger string = "ledger001"
    ctx := context.Background()
    res, err := s.Ledger.V2.ImportLogs(ctx, ledger, nil)
    if err != nil {
        log.Fatal(err)
    }
    if res != nil {
        // handle response
    }
}
```

### Parameters

| Parameter                                                | Type                                                     | Required                                                 | Description                                              | Example                                                  |
| -------------------------------------------------------- | -------------------------------------------------------- | -------------------------------------------------------- | -------------------------------------------------------- | -------------------------------------------------------- |
| `ctx`                                                    | [context.Context](https://pkg.go.dev/context#Context)    | :heavy_check_mark:                                       | The context to use for the request.                      |                                                          |
| `ledger`                                                 | *string*                                                 | :heavy_check_mark:                                       | Name of the ledger.                                      | ledger001                                                |
| `requestBody`                                            | **string*                                                | :heavy_minus_sign:                                       | N/A                                                      |                                                          |
| `opts`                                                   | [][operations.Option](../../models/operations/option.md) | :heavy_minus_sign:                                       | The options for this request.                            |                                                          |


### Response

**[*operations.V2ImportLogsResponse](../../models/operations/v2importlogsresponse.md), error**
| Error Object              | Status Code               | Content Type              |
| ------------------------- | ------------------------- | ------------------------- |
| sdkerrors.V2ErrorResponse | default                   | application/json          |
| sdkerrors.SDKError        | 4xx-5xx                   | */*                       |

## ExportLogs

Export logs

### Example Usage

```go
package main

import(
	"github.com/formancehq/ledger/pkg/client/models/components"
	"github.com/formancehq/ledger/pkg/client"
	"context"
	"log"
)

func main() {
    s := client.New(
        client.WithSecurity(components.Security{
            ClientID: "",
            ClientSecret: "",
        }),
    )
    var ledger string = "ledger001"
    ctx := context.Background()
    res, err := s.Ledger.V2.ExportLogs(ctx, ledger)
    if err != nil {
        log.Fatal(err)
    }
    if res != nil {
        // handle response
    }
}
```

### Parameters

| Parameter                                                | Type                                                     | Required                                                 | Description                                              | Example                                                  |
| -------------------------------------------------------- | -------------------------------------------------------- | -------------------------------------------------------- | -------------------------------------------------------- | -------------------------------------------------------- |
| `ctx`                                                    | [context.Context](https://pkg.go.dev/context#Context)    | :heavy_check_mark:                                       | The context to use for the request.                      |                                                          |
| `ledger`                                                 | *string*                                                 | :heavy_check_mark:                                       | Name of the ledger.                                      | ledger001                                                |
| `opts`                                                   | [][operations.Option](../../models/operations/option.md) | :heavy_minus_sign:                                       | The options for this request.                            |                                                          |


### Response

**[*operations.V2ExportLogsResponse](../../models/operations/v2exportlogsresponse.md), error**
| Error Object       | Status Code        | Content Type       |
| ------------------ | ------------------ | ------------------ |
| sdkerrors.SDKError | 4xx-5xx            | */*                |
