# Ledgers
(*Ledgers*)

## Overview

### Available Operations

* [ListAllLedgers](#listallledgers) - List all ledgers
* [GetLedger](#getledger) - Get a ledger
* [CreateLedger](#createledger) - Create a new ledger
* [DeleteLedger](#deleteledger) - Delete a ledger
* [GetLedgerRaftState](#getledgerraftstate) - Get ledger Raft cluster state

## ListAllLedgers

Returns a list of all ledgers in the cluster.

### Example Usage

```go
package main

import(
	"context"
	"github.com/formancehq/ledger-v3-poc/pkg/client"
	"log"
)

func main() {
    ctx := context.Background()

    s := client.New()

    res, err := s.Ledgers.ListAllLedgers(ctx)
    if err != nil {
        log.Fatal(err)
    }
    if res.ListAllLedgersResponse != nil {
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

**[*operations.ListAllLedgersResponse](../../models/operations/listallledgersresponse.md), error**

### Errors

| Error Type              | Status Code             | Content Type            |
| ----------------------- | ----------------------- | ----------------------- |
| sdkerrors.ErrorResponse | 500                     | application/json        |
| sdkerrors.SDKError      | 4XX, 5XX                | \*/\*                   |

## GetLedger

Retrieves a ledger by its name.

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

    res, err := s.Ledgers.GetLedger(ctx, operations.GetLedgerRequest{
        LedgerName: "<value>",
    })
    if err != nil {
        log.Fatal(err)
    }
    if res.GetLedgerResponse != nil {
        // handle response
    }
}
```

### Parameters

| Parameter                                                                  | Type                                                                       | Required                                                                   | Description                                                                |
| -------------------------------------------------------------------------- | -------------------------------------------------------------------------- | -------------------------------------------------------------------------- | -------------------------------------------------------------------------- |
| `ctx`                                                                      | [context.Context](https://pkg.go.dev/context#Context)                      | :heavy_check_mark:                                                         | The context to use for the request.                                        |
| `request`                                                                  | [operations.GetLedgerRequest](../../models/operations/getledgerrequest.md) | :heavy_check_mark:                                                         | The request object to use for the request.                                 |
| `opts`                                                                     | [][operations.Option](../../models/operations/option.md)                   | :heavy_minus_sign:                                                         | The options for this request.                                              |

### Response

**[*operations.GetLedgerResponse](../../models/operations/getledgerresponse.md), error**

### Errors

| Error Type              | Status Code             | Content Type            |
| ----------------------- | ----------------------- | ----------------------- |
| sdkerrors.ErrorResponse | 404                     | application/json        |
| sdkerrors.ErrorResponse | 500                     | application/json        |
| sdkerrors.ErrorResponse | 503                     | application/json        |
| sdkerrors.SDKError      | 4XX, 5XX                | \*/\*                   |

## CreateLedger

Creates a new ledger with the specified name, drivers, and configurations. Both logStoreDriver and runtimeStoreDriver are required.
Available drivers: sqlite-mattn (github.com/mattn/go-sqlite3), sqlite-modern (modernc.org/sqlite), and pebble (github.com/cockroachdb/pebble).
Each store (log store and runtime store) can have its own driver and configuration, allowing for flexible storage setups.
Each ledger has its own Raft group for data consistency.


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

    res, err := s.Ledgers.CreateLedger(ctx, operations.CreateLedgerRequest{
        LedgerName: "<value>",
        CreateLedgerRequest: components.CreateLedgerRequest{
            LogStoreDriver: components.CreateLedgerRequestLogStoreDriverPebble,
            RuntimeStoreDriver: components.CreateLedgerRequestRuntimeStoreDriverSqliteMattn,
            LogStoreConfig: client.Pointer(components.CreateCreateLedgerRequestLogStoreConfigSQLiteMattnConfig(
                components.SQLiteMattnConfig{},
            )),
            RuntimeStoreConfig: client.Pointer(components.CreateCreateLedgerRequestRuntimeStoreConfigSQLiteMattnConfig(
                components.SQLiteMattnConfig{},
            )),
        },
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
| sdkerrors.ErrorResponse | 503                     | application/json        |
| sdkerrors.SDKError      | 4XX, 5XX                | \*/\*                   |

## DeleteLedger

Deletes a ledger by its name. This operation removes the ledger and its associated Raft group from the cluster.

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

    res, err := s.Ledgers.DeleteLedger(ctx, operations.DeleteLedgerRequest{
        LedgerName: "<value>",
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
| `request`                                                                        | [operations.DeleteLedgerRequest](../../models/operations/deleteledgerrequest.md) | :heavy_check_mark:                                                               | The request object to use for the request.                                       |
| `opts`                                                                           | [][operations.Option](../../models/operations/option.md)                         | :heavy_minus_sign:                                                               | The options for this request.                                                    |

### Response

**[*operations.DeleteLedgerResponse](../../models/operations/deleteledgerresponse.md), error**

### Errors

| Error Type              | Status Code             | Content Type            |
| ----------------------- | ----------------------- | ----------------------- |
| sdkerrors.ErrorResponse | 404                     | application/json        |
| sdkerrors.ErrorResponse | 500                     | application/json        |
| sdkerrors.ErrorResponse | 503                     | application/json        |
| sdkerrors.SDKError      | 4XX, 5XX                | \*/\*                   |

## GetLedgerRaftState

Returns the current state of the Raft cluster for the specified ledger, including the list of nodes, the current leader, and the ledger state

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

    res, err := s.Ledgers.GetLedgerRaftState(ctx, operations.GetLedgerRaftStateRequest{
        LedgerName: "<value>",
    })
    if err != nil {
        log.Fatal(err)
    }
    if res.LedgerClusterStateResponse != nil {
        // handle response
    }
}
```

### Parameters

| Parameter                                                                                    | Type                                                                                         | Required                                                                                     | Description                                                                                  |
| -------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------- |
| `ctx`                                                                                        | [context.Context](https://pkg.go.dev/context#Context)                                        | :heavy_check_mark:                                                                           | The context to use for the request.                                                          |
| `request`                                                                                    | [operations.GetLedgerRaftStateRequest](../../models/operations/getledgerraftstaterequest.md) | :heavy_check_mark:                                                                           | The request object to use for the request.                                                   |
| `opts`                                                                                       | [][operations.Option](../../models/operations/option.md)                                     | :heavy_minus_sign:                                                                           | The options for this request.                                                                |

### Response

**[*operations.GetLedgerRaftStateResponse](../../models/operations/getledgerraftstateresponse.md), error**

### Errors

| Error Type              | Status Code             | Content Type            |
| ----------------------- | ----------------------- | ----------------------- |
| sdkerrors.ErrorResponse | 404                     | application/json        |
| sdkerrors.ErrorResponse | 500                     | application/json        |
| sdkerrors.ErrorResponse | 503                     | application/json        |
| sdkerrors.SDKError      | 4XX, 5XX                | \*/\*                   |