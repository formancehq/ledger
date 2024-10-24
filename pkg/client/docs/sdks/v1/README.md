# V1
(*Ledger.V1*)

### Available Operations

* [GetInfo](#getinfo) - Show server information
* [GetLedgerInfo](#getledgerinfo) - Get information about a ledger
* [CountAccounts](#countaccounts) - Count the accounts from a ledger
* [ListAccounts](#listaccounts) - List accounts from a ledger
* [GetAccount](#getaccount) - Get account by its address
* [AddMetadataToAccount](#addmetadatatoaccount) - Add metadata to an account
* [GetMapping](#getmapping) - Get the mapping of a ledger
* [UpdateMapping](#updatemapping) - Update the mapping of a ledger
* [~~RunScript~~](#runscript) - Execute a Numscript :warning: **Deprecated**
* [ReadStats](#readstats) - Get statistics from a ledger
* [CountTransactions](#counttransactions) - Count the transactions from a ledger
* [ListTransactions](#listtransactions) - List transactions from a ledger
* [CreateTransaction](#createtransaction) - Create a new transaction to a ledger
* [GetTransaction](#gettransaction) - Get transaction from a ledger by its ID
* [AddMetadataOnTransaction](#addmetadataontransaction) - Set the metadata of a transaction by its ID
* [RevertTransaction](#reverttransaction) - Revert a ledger transaction by its ID
* [CreateTransactions](#createtransactions) - Create a new batch of transactions to a ledger
* [GetBalances](#getbalances) - Get the balances from a ledger's account
* [GetBalancesAggregated](#getbalancesaggregated) - Get the aggregated balances from selected accounts
* [ListLogs](#listlogs) - List the logs from a ledger

## GetInfo

Show server information

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
    res, err := s.Ledger.V1.GetInfo(ctx)
    if err != nil {
        log.Fatal(err)
    }
    if res.ConfigInfoResponse != nil {
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

**[*operations.GetInfoResponse](../../models/operations/getinforesponse.md), error**
| Error Object            | Status Code             | Content Type            |
| ----------------------- | ----------------------- | ----------------------- |
| sdkerrors.ErrorResponse | default                 | application/json        |
| sdkerrors.SDKError      | 4xx-5xx                 | */*                     |

## GetLedgerInfo

Get information about a ledger

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
    request := operations.GetLedgerInfoRequest{
        Ledger: "ledger001",
    }
    ctx := context.Background()
    res, err := s.Ledger.V1.GetLedgerInfo(ctx, request)
    if err != nil {
        log.Fatal(err)
    }
    if res.LedgerInfoResponse != nil {
        // handle response
    }
}
```

### Parameters

| Parameter                                                                          | Type                                                                               | Required                                                                           | Description                                                                        |
| ---------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------- |
| `ctx`                                                                              | [context.Context](https://pkg.go.dev/context#Context)                              | :heavy_check_mark:                                                                 | The context to use for the request.                                                |
| `request`                                                                          | [operations.GetLedgerInfoRequest](../../models/operations/getledgerinforequest.md) | :heavy_check_mark:                                                                 | The request object to use for the request.                                         |
| `opts`                                                                             | [][operations.Option](../../models/operations/option.md)                           | :heavy_minus_sign:                                                                 | The options for this request.                                                      |


### Response

**[*operations.GetLedgerInfoResponse](../../models/operations/getledgerinforesponse.md), error**
| Error Object            | Status Code             | Content Type            |
| ----------------------- | ----------------------- | ----------------------- |
| sdkerrors.ErrorResponse | default                 | application/json        |
| sdkerrors.SDKError      | 4xx-5xx                 | */*                     |

## CountAccounts

Count the accounts from a ledger

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
    request := operations.CountAccountsRequest{
        Ledger: "ledger001",
        Address: client.String("users:.+"),
        Metadata: map[string]any{
            "0": "m",
            "1": "e",
            "2": "t",
            "3": "a",
            "4": "d",
            "5": "a",
            "6": "t",
            "7": "a",
            "8": "[",
            "9": "k",
            "10": "e",
            "11": "y",
            "12": "]",
            "13": "=",
            "14": "v",
            "15": "a",
            "16": "l",
            "17": "u",
            "18": "e",
            "19": "1",
            "20": "&",
            "21": "m",
            "22": "e",
            "23": "t",
            "24": "a",
            "25": "d",
            "26": "a",
            "27": "t",
            "28": "a",
            "29": "[",
            "30": "a",
            "31": ".",
            "32": "n",
            "33": "e",
            "34": "s",
            "35": "t",
            "36": "e",
            "37": "d",
            "38": ".",
            "39": "k",
            "40": "e",
            "41": "y",
            "42": "]",
            "43": "=",
            "44": "v",
            "45": "a",
            "46": "l",
            "47": "u",
            "48": "e",
            "49": "2",
        },
    }
    ctx := context.Background()
    res, err := s.Ledger.V1.CountAccounts(ctx, request)
    if err != nil {
        log.Fatal(err)
    }
    if res != nil {
        // handle response
    }
}
```

### Parameters

| Parameter                                                                          | Type                                                                               | Required                                                                           | Description                                                                        |
| ---------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------- |
| `ctx`                                                                              | [context.Context](https://pkg.go.dev/context#Context)                              | :heavy_check_mark:                                                                 | The context to use for the request.                                                |
| `request`                                                                          | [operations.CountAccountsRequest](../../models/operations/countaccountsrequest.md) | :heavy_check_mark:                                                                 | The request object to use for the request.                                         |
| `opts`                                                                             | [][operations.Option](../../models/operations/option.md)                           | :heavy_minus_sign:                                                                 | The options for this request.                                                      |


### Response

**[*operations.CountAccountsResponse](../../models/operations/countaccountsresponse.md), error**
| Error Object            | Status Code             | Content Type            |
| ----------------------- | ----------------------- | ----------------------- |
| sdkerrors.ErrorResponse | default                 | application/json        |
| sdkerrors.SDKError      | 4xx-5xx                 | */*                     |

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
    request := operations.ListAccountsRequest{
        Ledger: "ledger001",
        PageSize: client.Int64(100),
        After: client.String("users:003"),
        Address: client.String("users:.+"),
        Metadata: map[string]any{
            "0": "m",
            "1": "e",
            "2": "t",
            "3": "a",
            "4": "d",
            "5": "a",
            "6": "t",
            "7": "a",
            "8": "[",
            "9": "k",
            "10": "e",
            "11": "y",
            "12": "]",
            "13": "=",
            "14": "v",
            "15": "a",
            "16": "l",
            "17": "u",
            "18": "e",
            "19": "1",
            "20": "&",
            "21": "m",
            "22": "e",
            "23": "t",
            "24": "a",
            "25": "d",
            "26": "a",
            "27": "t",
            "28": "a",
            "29": "[",
            "30": "a",
            "31": ".",
            "32": "n",
            "33": "e",
            "34": "s",
            "35": "t",
            "36": "e",
            "37": "d",
            "38": ".",
            "39": "k",
            "40": "e",
            "41": "y",
            "42": "]",
            "43": "=",
            "44": "v",
            "45": "a",
            "46": "l",
            "47": "u",
            "48": "e",
            "49": "2",
        },
        Balance: client.Int64(2400),
        Cursor: client.String("aHR0cHM6Ly9nLnBhZ2UvTmVrby1SYW1lbj9zaGFyZQ=="),
    }
    ctx := context.Background()
    res, err := s.Ledger.V1.ListAccounts(ctx, request)
    if err != nil {
        log.Fatal(err)
    }
    if res.AccountsCursorResponse != nil {
        // handle response
    }
}
```

### Parameters

| Parameter                                                                        | Type                                                                             | Required                                                                         | Description                                                                      |
| -------------------------------------------------------------------------------- | -------------------------------------------------------------------------------- | -------------------------------------------------------------------------------- | -------------------------------------------------------------------------------- |
| `ctx`                                                                            | [context.Context](https://pkg.go.dev/context#Context)                            | :heavy_check_mark:                                                               | The context to use for the request.                                              |
| `request`                                                                        | [operations.ListAccountsRequest](../../models/operations/listaccountsrequest.md) | :heavy_check_mark:                                                               | The request object to use for the request.                                       |
| `opts`                                                                           | [][operations.Option](../../models/operations/option.md)                         | :heavy_minus_sign:                                                               | The options for this request.                                                    |


### Response

**[*operations.ListAccountsResponse](../../models/operations/listaccountsresponse.md), error**
| Error Object            | Status Code             | Content Type            |
| ----------------------- | ----------------------- | ----------------------- |
| sdkerrors.ErrorResponse | default                 | application/json        |
| sdkerrors.SDKError      | 4xx-5xx                 | */*                     |

## GetAccount

Get account by its address

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
    request := operations.GetAccountRequest{
        Ledger: "ledger001",
        Address: "users:001",
    }
    ctx := context.Background()
    res, err := s.Ledger.V1.GetAccount(ctx, request)
    if err != nil {
        log.Fatal(err)
    }
    if res.AccountResponse != nil {
        // handle response
    }
}
```

### Parameters

| Parameter                                                                    | Type                                                                         | Required                                                                     | Description                                                                  |
| ---------------------------------------------------------------------------- | ---------------------------------------------------------------------------- | ---------------------------------------------------------------------------- | ---------------------------------------------------------------------------- |
| `ctx`                                                                        | [context.Context](https://pkg.go.dev/context#Context)                        | :heavy_check_mark:                                                           | The context to use for the request.                                          |
| `request`                                                                    | [operations.GetAccountRequest](../../models/operations/getaccountrequest.md) | :heavy_check_mark:                                                           | The request object to use for the request.                                   |
| `opts`                                                                       | [][operations.Option](../../models/operations/option.md)                     | :heavy_minus_sign:                                                           | The options for this request.                                                |


### Response

**[*operations.GetAccountResponse](../../models/operations/getaccountresponse.md), error**
| Error Object            | Status Code             | Content Type            |
| ----------------------- | ----------------------- | ----------------------- |
| sdkerrors.ErrorResponse | default                 | application/json        |
| sdkerrors.SDKError      | 4xx-5xx                 | */*                     |

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
    request := operations.AddMetadataToAccountRequest{
        Ledger: "ledger001",
        Address: "users:001",
        RequestBody: map[string]any{
            "key": "<value>",
        },
    }
    ctx := context.Background()
    res, err := s.Ledger.V1.AddMetadataToAccount(ctx, request)
    if err != nil {
        log.Fatal(err)
    }
    if res != nil {
        // handle response
    }
}
```

### Parameters

| Parameter                                                                                        | Type                                                                                             | Required                                                                                         | Description                                                                                      |
| ------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------ |
| `ctx`                                                                                            | [context.Context](https://pkg.go.dev/context#Context)                                            | :heavy_check_mark:                                                                               | The context to use for the request.                                                              |
| `request`                                                                                        | [operations.AddMetadataToAccountRequest](../../models/operations/addmetadatatoaccountrequest.md) | :heavy_check_mark:                                                                               | The request object to use for the request.                                                       |
| `opts`                                                                                           | [][operations.Option](../../models/operations/option.md)                                         | :heavy_minus_sign:                                                                               | The options for this request.                                                                    |


### Response

**[*operations.AddMetadataToAccountResponse](../../models/operations/addmetadatatoaccountresponse.md), error**
| Error Object            | Status Code             | Content Type            |
| ----------------------- | ----------------------- | ----------------------- |
| sdkerrors.ErrorResponse | default                 | application/json        |
| sdkerrors.SDKError      | 4xx-5xx                 | */*                     |

## GetMapping

Get the mapping of a ledger

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
    request := operations.GetMappingRequest{
        Ledger: "ledger001",
    }
    ctx := context.Background()
    res, err := s.Ledger.V1.GetMapping(ctx, request)
    if err != nil {
        log.Fatal(err)
    }
    if res.MappingResponse != nil {
        // handle response
    }
}
```

### Parameters

| Parameter                                                                    | Type                                                                         | Required                                                                     | Description                                                                  |
| ---------------------------------------------------------------------------- | ---------------------------------------------------------------------------- | ---------------------------------------------------------------------------- | ---------------------------------------------------------------------------- |
| `ctx`                                                                        | [context.Context](https://pkg.go.dev/context#Context)                        | :heavy_check_mark:                                                           | The context to use for the request.                                          |
| `request`                                                                    | [operations.GetMappingRequest](../../models/operations/getmappingrequest.md) | :heavy_check_mark:                                                           | The request object to use for the request.                                   |
| `opts`                                                                       | [][operations.Option](../../models/operations/option.md)                     | :heavy_minus_sign:                                                           | The options for this request.                                                |


### Response

**[*operations.GetMappingResponse](../../models/operations/getmappingresponse.md), error**
| Error Object            | Status Code             | Content Type            |
| ----------------------- | ----------------------- | ----------------------- |
| sdkerrors.ErrorResponse | default                 | application/json        |
| sdkerrors.SDKError      | 4xx-5xx                 | */*                     |

## UpdateMapping

Update the mapping of a ledger

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
    request := operations.UpdateMappingRequest{
        Ledger: "ledger001",
        Mapping: &components.Mapping{
            Contracts: []components.Contract{
                components.Contract{
                    Account: client.String("users:001"),
                    Expr: components.Expr{},
                },
            },
        },
    }
    ctx := context.Background()
    res, err := s.Ledger.V1.UpdateMapping(ctx, request)
    if err != nil {
        log.Fatal(err)
    }
    if res.MappingResponse != nil {
        // handle response
    }
}
```

### Parameters

| Parameter                                                                          | Type                                                                               | Required                                                                           | Description                                                                        |
| ---------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------- |
| `ctx`                                                                              | [context.Context](https://pkg.go.dev/context#Context)                              | :heavy_check_mark:                                                                 | The context to use for the request.                                                |
| `request`                                                                          | [operations.UpdateMappingRequest](../../models/operations/updatemappingrequest.md) | :heavy_check_mark:                                                                 | The request object to use for the request.                                         |
| `opts`                                                                             | [][operations.Option](../../models/operations/option.md)                           | :heavy_minus_sign:                                                                 | The options for this request.                                                      |


### Response

**[*operations.UpdateMappingResponse](../../models/operations/updatemappingresponse.md), error**
| Error Object            | Status Code             | Content Type            |
| ----------------------- | ----------------------- | ----------------------- |
| sdkerrors.ErrorResponse | default                 | application/json        |
| sdkerrors.SDKError      | 4xx-5xx                 | */*                     |

## ~~RunScript~~

This route is deprecated, and has been merged into `POST /{ledger}/transactions`.


> :warning: **DEPRECATED**: This will be removed in a future release, please migrate away from it as soon as possible.

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
    request := operations.RunScriptRequest{
        Ledger: "ledger001",
        Preview: client.Bool(true),
        Script: components.Script{
            Plain: "vars {
        account $user
        }
        send [COIN 10] (
        	source = @world
        	destination = $user
        )
        ",
            Vars: map[string]any{
                "user": "users:042",
            },
            Reference: client.String("order_1234"),
        },
    }
    ctx := context.Background()
    res, err := s.Ledger.V1.RunScript(ctx, request)
    if err != nil {
        log.Fatal(err)
    }
    if res.ScriptResponse != nil {
        // handle response
    }
}
```

### Parameters

| Parameter                                                                  | Type                                                                       | Required                                                                   | Description                                                                |
| -------------------------------------------------------------------------- | -------------------------------------------------------------------------- | -------------------------------------------------------------------------- | -------------------------------------------------------------------------- |
| `ctx`                                                                      | [context.Context](https://pkg.go.dev/context#Context)                      | :heavy_check_mark:                                                         | The context to use for the request.                                        |
| `request`                                                                  | [operations.RunScriptRequest](../../models/operations/runscriptrequest.md) | :heavy_check_mark:                                                         | The request object to use for the request.                                 |
| `opts`                                                                     | [][operations.Option](../../models/operations/option.md)                   | :heavy_minus_sign:                                                         | The options for this request.                                              |


### Response

**[*operations.RunScriptResponse](../../models/operations/runscriptresponse.md), error**
| Error Object       | Status Code        | Content Type       |
| ------------------ | ------------------ | ------------------ |
| sdkerrors.SDKError | 4xx-5xx            | */*                |

## ReadStats

Get statistics from a ledger. (aggregate metrics on accounts and transactions)


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
    request := operations.ReadStatsRequest{
        Ledger: "ledger001",
    }
    ctx := context.Background()
    res, err := s.Ledger.V1.ReadStats(ctx, request)
    if err != nil {
        log.Fatal(err)
    }
    if res.StatsResponse != nil {
        // handle response
    }
}
```

### Parameters

| Parameter                                                                  | Type                                                                       | Required                                                                   | Description                                                                |
| -------------------------------------------------------------------------- | -------------------------------------------------------------------------- | -------------------------------------------------------------------------- | -------------------------------------------------------------------------- |
| `ctx`                                                                      | [context.Context](https://pkg.go.dev/context#Context)                      | :heavy_check_mark:                                                         | The context to use for the request.                                        |
| `request`                                                                  | [operations.ReadStatsRequest](../../models/operations/readstatsrequest.md) | :heavy_check_mark:                                                         | The request object to use for the request.                                 |
| `opts`                                                                     | [][operations.Option](../../models/operations/option.md)                   | :heavy_minus_sign:                                                         | The options for this request.                                              |


### Response

**[*operations.ReadStatsResponse](../../models/operations/readstatsresponse.md), error**
| Error Object            | Status Code             | Content Type            |
| ----------------------- | ----------------------- | ----------------------- |
| sdkerrors.ErrorResponse | default                 | application/json        |
| sdkerrors.SDKError      | 4xx-5xx                 | */*                     |

## CountTransactions

Count the transactions from a ledger

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
    request := operations.CountTransactionsRequest{
        Ledger: "ledger001",
        Reference: client.String("ref:001"),
        Account: client.String("users:001"),
        Source: client.String("users:001"),
        Destination: client.String("users:001"),
        Metadata: &operations.Metadata{},
    }
    ctx := context.Background()
    res, err := s.Ledger.V1.CountTransactions(ctx, request)
    if err != nil {
        log.Fatal(err)
    }
    if res != nil {
        // handle response
    }
}
```

### Parameters

| Parameter                                                                                  | Type                                                                                       | Required                                                                                   | Description                                                                                |
| ------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------ |
| `ctx`                                                                                      | [context.Context](https://pkg.go.dev/context#Context)                                      | :heavy_check_mark:                                                                         | The context to use for the request.                                                        |
| `request`                                                                                  | [operations.CountTransactionsRequest](../../models/operations/counttransactionsrequest.md) | :heavy_check_mark:                                                                         | The request object to use for the request.                                                 |
| `opts`                                                                                     | [][operations.Option](../../models/operations/option.md)                                   | :heavy_minus_sign:                                                                         | The options for this request.                                                              |


### Response

**[*operations.CountTransactionsResponse](../../models/operations/counttransactionsresponse.md), error**
| Error Object            | Status Code             | Content Type            |
| ----------------------- | ----------------------- | ----------------------- |
| sdkerrors.ErrorResponse | default                 | application/json        |
| sdkerrors.SDKError      | 4xx-5xx                 | */*                     |

## ListTransactions

List transactions from a ledger, sorted by txid in descending order.

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
    request := operations.ListTransactionsRequest{
        Ledger: "ledger001",
        PageSize: client.Int64(100),
        After: client.String("1234"),
        Reference: client.String("ref:001"),
        Account: client.String("users:001"),
        Source: client.String("users:001"),
        Destination: client.String("users:001"),
        Cursor: client.String("aHR0cHM6Ly9nLnBhZ2UvTmVrby1SYW1lbj9zaGFyZQ=="),
    }
    ctx := context.Background()
    res, err := s.Ledger.V1.ListTransactions(ctx, request)
    if err != nil {
        log.Fatal(err)
    }
    if res.TransactionsCursorResponse != nil {
        // handle response
    }
}
```

### Parameters

| Parameter                                                                                | Type                                                                                     | Required                                                                                 | Description                                                                              |
| ---------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------- |
| `ctx`                                                                                    | [context.Context](https://pkg.go.dev/context#Context)                                    | :heavy_check_mark:                                                                       | The context to use for the request.                                                      |
| `request`                                                                                | [operations.ListTransactionsRequest](../../models/operations/listtransactionsrequest.md) | :heavy_check_mark:                                                                       | The request object to use for the request.                                               |
| `opts`                                                                                   | [][operations.Option](../../models/operations/option.md)                                 | :heavy_minus_sign:                                                                       | The options for this request.                                                            |


### Response

**[*operations.ListTransactionsResponse](../../models/operations/listtransactionsresponse.md), error**
| Error Object            | Status Code             | Content Type            |
| ----------------------- | ----------------------- | ----------------------- |
| sdkerrors.ErrorResponse | default                 | application/json        |
| sdkerrors.SDKError      | 4xx-5xx                 | */*                     |

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
    request := operations.CreateTransactionRequest{
        Ledger: "ledger001",
        Preview: client.Bool(true),
        PostTransaction: components.PostTransaction{
            Postings: []components.Posting{
                components.Posting{
                    Amount: big.NewInt(100),
                    Asset: "COIN",
                    Destination: "users:002",
                    Source: "users:001",
                },
            },
            Script: &components.PostTransactionScript{
                Plain: "vars {
            account $user
            }
            send [COIN 10] (
            	source = @world
            	destination = $user
            )
            ",
                Vars: map[string]any{
                    "user": "users:042",
                },
            },
            Reference: client.String("ref:001"),
        },
    }
    ctx := context.Background()
    res, err := s.Ledger.V1.CreateTransaction(ctx, request)
    if err != nil {
        log.Fatal(err)
    }
    if res.TransactionsResponse != nil {
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
| Error Object            | Status Code             | Content Type            |
| ----------------------- | ----------------------- | ----------------------- |
| sdkerrors.ErrorResponse | default                 | application/json        |
| sdkerrors.SDKError      | 4xx-5xx                 | */*                     |

## GetTransaction

Get transaction from a ledger by its ID

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
    request := operations.GetTransactionRequest{
        Ledger: "ledger001",
        Txid: big.NewInt(1234),
    }
    ctx := context.Background()
    res, err := s.Ledger.V1.GetTransaction(ctx, request)
    if err != nil {
        log.Fatal(err)
    }
    if res.TransactionResponse != nil {
        // handle response
    }
}
```

### Parameters

| Parameter                                                                            | Type                                                                                 | Required                                                                             | Description                                                                          |
| ------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------ |
| `ctx`                                                                                | [context.Context](https://pkg.go.dev/context#Context)                                | :heavy_check_mark:                                                                   | The context to use for the request.                                                  |
| `request`                                                                            | [operations.GetTransactionRequest](../../models/operations/gettransactionrequest.md) | :heavy_check_mark:                                                                   | The request object to use for the request.                                           |
| `opts`                                                                               | [][operations.Option](../../models/operations/option.md)                             | :heavy_minus_sign:                                                                   | The options for this request.                                                        |


### Response

**[*operations.GetTransactionResponse](../../models/operations/gettransactionresponse.md), error**
| Error Object            | Status Code             | Content Type            |
| ----------------------- | ----------------------- | ----------------------- |
| sdkerrors.ErrorResponse | default                 | application/json        |
| sdkerrors.SDKError      | 4xx-5xx                 | */*                     |

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
    request := operations.AddMetadataOnTransactionRequest{
        Ledger: "ledger001",
        Txid: big.NewInt(1234),
    }
    ctx := context.Background()
    res, err := s.Ledger.V1.AddMetadataOnTransaction(ctx, request)
    if err != nil {
        log.Fatal(err)
    }
    if res != nil {
        // handle response
    }
}
```

### Parameters

| Parameter                                                                                                | Type                                                                                                     | Required                                                                                                 | Description                                                                                              |
| -------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------- |
| `ctx`                                                                                                    | [context.Context](https://pkg.go.dev/context#Context)                                                    | :heavy_check_mark:                                                                                       | The context to use for the request.                                                                      |
| `request`                                                                                                | [operations.AddMetadataOnTransactionRequest](../../models/operations/addmetadataontransactionrequest.md) | :heavy_check_mark:                                                                                       | The request object to use for the request.                                                               |
| `opts`                                                                                                   | [][operations.Option](../../models/operations/option.md)                                                 | :heavy_minus_sign:                                                                                       | The options for this request.                                                                            |


### Response

**[*operations.AddMetadataOnTransactionResponse](../../models/operations/addmetadataontransactionresponse.md), error**
| Error Object            | Status Code             | Content Type            |
| ----------------------- | ----------------------- | ----------------------- |
| sdkerrors.ErrorResponse | default                 | application/json        |
| sdkerrors.SDKError      | 4xx-5xx                 | */*                     |

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
    request := operations.RevertTransactionRequest{
        Ledger: "ledger001",
        Txid: big.NewInt(1234),
    }
    ctx := context.Background()
    res, err := s.Ledger.V1.RevertTransaction(ctx, request)
    if err != nil {
        log.Fatal(err)
    }
    if res.TransactionResponse != nil {
        // handle response
    }
}
```

### Parameters

| Parameter                                                                                  | Type                                                                                       | Required                                                                                   | Description                                                                                |
| ------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------ |
| `ctx`                                                                                      | [context.Context](https://pkg.go.dev/context#Context)                                      | :heavy_check_mark:                                                                         | The context to use for the request.                                                        |
| `request`                                                                                  | [operations.RevertTransactionRequest](../../models/operations/reverttransactionrequest.md) | :heavy_check_mark:                                                                         | The request object to use for the request.                                                 |
| `opts`                                                                                     | [][operations.Option](../../models/operations/option.md)                                   | :heavy_minus_sign:                                                                         | The options for this request.                                                              |


### Response

**[*operations.RevertTransactionResponse](../../models/operations/reverttransactionresponse.md), error**
| Error Object            | Status Code             | Content Type            |
| ----------------------- | ----------------------- | ----------------------- |
| sdkerrors.ErrorResponse | default                 | application/json        |
| sdkerrors.SDKError      | 4xx-5xx                 | */*                     |

## CreateTransactions

Create a new batch of transactions to a ledger

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
    request := operations.CreateTransactionsRequest{
        Ledger: "ledger001",
        Transactions: components.Transactions{
            Transactions: []components.TransactionData{
                components.TransactionData{
                    Postings: []components.Posting{
                        components.Posting{
                            Amount: big.NewInt(100),
                            Asset: "COIN",
                            Destination: "users:002",
                            Source: "users:001",
                        },
                    },
                    Reference: client.String("ref:001"),
                },
            },
        },
    }
    ctx := context.Background()
    res, err := s.Ledger.V1.CreateTransactions(ctx, request)
    if err != nil {
        log.Fatal(err)
    }
    if res.TransactionsResponse != nil {
        // handle response
    }
}
```

### Parameters

| Parameter                                                                                    | Type                                                                                         | Required                                                                                     | Description                                                                                  |
| -------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------- |
| `ctx`                                                                                        | [context.Context](https://pkg.go.dev/context#Context)                                        | :heavy_check_mark:                                                                           | The context to use for the request.                                                          |
| `request`                                                                                    | [operations.CreateTransactionsRequest](../../models/operations/createtransactionsrequest.md) | :heavy_check_mark:                                                                           | The request object to use for the request.                                                   |
| `opts`                                                                                       | [][operations.Option](../../models/operations/option.md)                                     | :heavy_minus_sign:                                                                           | The options for this request.                                                                |


### Response

**[*operations.CreateTransactionsResponse](../../models/operations/createtransactionsresponse.md), error**
| Error Object            | Status Code             | Content Type            |
| ----------------------- | ----------------------- | ----------------------- |
| sdkerrors.ErrorResponse | default                 | application/json        |
| sdkerrors.SDKError      | 4xx-5xx                 | */*                     |

## GetBalances

Get the balances from a ledger's account

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
    request := operations.GetBalancesRequest{
        Ledger: "ledger001",
        Address: client.String("users:001"),
        After: client.String("users:003"),
        Cursor: client.String("aHR0cHM6Ly9nLnBhZ2UvTmVrby1SYW1lbj9zaGFyZQ=="),
    }
    ctx := context.Background()
    res, err := s.Ledger.V1.GetBalances(ctx, request)
    if err != nil {
        log.Fatal(err)
    }
    if res.BalancesCursorResponse != nil {
        // handle response
    }
}
```

### Parameters

| Parameter                                                                      | Type                                                                           | Required                                                                       | Description                                                                    |
| ------------------------------------------------------------------------------ | ------------------------------------------------------------------------------ | ------------------------------------------------------------------------------ | ------------------------------------------------------------------------------ |
| `ctx`                                                                          | [context.Context](https://pkg.go.dev/context#Context)                          | :heavy_check_mark:                                                             | The context to use for the request.                                            |
| `request`                                                                      | [operations.GetBalancesRequest](../../models/operations/getbalancesrequest.md) | :heavy_check_mark:                                                             | The request object to use for the request.                                     |
| `opts`                                                                         | [][operations.Option](../../models/operations/option.md)                       | :heavy_minus_sign:                                                             | The options for this request.                                                  |


### Response

**[*operations.GetBalancesResponse](../../models/operations/getbalancesresponse.md), error**
| Error Object            | Status Code             | Content Type            |
| ----------------------- | ----------------------- | ----------------------- |
| sdkerrors.ErrorResponse | default                 | application/json        |
| sdkerrors.SDKError      | 4xx-5xx                 | */*                     |

## GetBalancesAggregated

Get the aggregated balances from selected accounts

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
    request := operations.GetBalancesAggregatedRequest{
        Ledger: "ledger001",
        Address: client.String("users:001"),
    }
    ctx := context.Background()
    res, err := s.Ledger.V1.GetBalancesAggregated(ctx, request)
    if err != nil {
        log.Fatal(err)
    }
    if res.AggregateBalancesResponse != nil {
        // handle response
    }
}
```

### Parameters

| Parameter                                                                                          | Type                                                                                               | Required                                                                                           | Description                                                                                        |
| -------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------- |
| `ctx`                                                                                              | [context.Context](https://pkg.go.dev/context#Context)                                              | :heavy_check_mark:                                                                                 | The context to use for the request.                                                                |
| `request`                                                                                          | [operations.GetBalancesAggregatedRequest](../../models/operations/getbalancesaggregatedrequest.md) | :heavy_check_mark:                                                                                 | The request object to use for the request.                                                         |
| `opts`                                                                                             | [][operations.Option](../../models/operations/option.md)                                           | :heavy_minus_sign:                                                                                 | The options for this request.                                                                      |


### Response

**[*operations.GetBalancesAggregatedResponse](../../models/operations/getbalancesaggregatedresponse.md), error**
| Error Object            | Status Code             | Content Type            |
| ----------------------- | ----------------------- | ----------------------- |
| sdkerrors.ErrorResponse | default                 | application/json        |
| sdkerrors.SDKError      | 4xx-5xx                 | */*                     |

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
    request := operations.ListLogsRequest{
        Ledger: "ledger001",
        PageSize: client.Int64(100),
        After: client.String("1234"),
        Cursor: client.String("aHR0cHM6Ly9nLnBhZ2UvTmVrby1SYW1lbj9zaGFyZQ=="),
    }
    ctx := context.Background()
    res, err := s.Ledger.V1.ListLogs(ctx, request)
    if err != nil {
        log.Fatal(err)
    }
    if res.LogsCursorResponse != nil {
        // handle response
    }
}
```

### Parameters

| Parameter                                                                | Type                                                                     | Required                                                                 | Description                                                              |
| ------------------------------------------------------------------------ | ------------------------------------------------------------------------ | ------------------------------------------------------------------------ | ------------------------------------------------------------------------ |
| `ctx`                                                                    | [context.Context](https://pkg.go.dev/context#Context)                    | :heavy_check_mark:                                                       | The context to use for the request.                                      |
| `request`                                                                | [operations.ListLogsRequest](../../models/operations/listlogsrequest.md) | :heavy_check_mark:                                                       | The request object to use for the request.                               |
| `opts`                                                                   | [][operations.Option](../../models/operations/option.md)                 | :heavy_minus_sign:                                                       | The options for this request.                                            |


### Response

**[*operations.ListLogsResponse](../../models/operations/listlogsresponse.md), error**
| Error Object            | Status Code             | Content Type            |
| ----------------------- | ----------------------- | ----------------------- |
| sdkerrors.ErrorResponse | default                 | application/json        |
| sdkerrors.SDKError      | 4xx-5xx                 | */*                     |
