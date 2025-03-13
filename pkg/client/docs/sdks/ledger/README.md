# Ledger
(*Ledger*)

## Overview

### Available Operations

* [GetInfo](#getinfo) - Show server information
* [GetMetrics](#getmetrics) - Read in memory metrics

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

    res, err := s.Ledger.GetInfo(ctx)
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

    res, err := s.Ledger.GetMetrics(ctx)
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