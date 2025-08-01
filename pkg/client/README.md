# github.com/formancehq/stack/ledger/client

Developer-friendly & type-safe Go SDK specifically catered to leverage *github.com/formancehq/stack/ledger/client* API.

<div align="left">
    <a href="https://www.speakeasy.com/?utm_source=github-com/formancehq/stack/ledger/client&utm_campaign=go"><img src="https://custom-icon-badges.demolab.com/badge/-Built%20By%20Speakeasy-212015?style=for-the-badge&logoColor=FBE331&logo=speakeasy&labelColor=545454" /></a>
    <a href="https://opensource.org/licenses/MIT">
        <img src="https://img.shields.io/badge/License-MIT-blue.svg" style="width: 100px; height: 28px;" />
    </a>
</div>


## 🏗 **Welcome to your new SDK!** 🏗

It has been generated successfully based on your OpenAPI spec. However, it is not yet ready for production use. Here are some next steps:
- [ ] 🛠 Make your SDK feel handcrafted by [customizing it](https://www.speakeasy.com/docs/customize-sdks)
- [ ] ♻️ Refine your SDK quickly by iterating locally with the [Speakeasy CLI](https://github.com/speakeasy-api/speakeasy)
- [ ] 🎁 Publish your SDK to package managers by [configuring automatic publishing](https://www.speakeasy.com/docs/advanced-setup/publish-sdks)
- [ ] ✨ When ready to productionize, delete this section from the README

<!-- Start Summary [summary] -->
## Summary


<!-- End Summary [summary] -->

<!-- Start Table of Contents [toc] -->
## Table of Contents
<!-- $toc-max-depth=2 -->
* [github.com/formancehq/stack/ledger/client](#githubcomformancehqstackledgerclient)
  * [🏗 **Welcome to your new SDK!** 🏗](#welcome-to-your-new-sdk)
  * [SDK Installation](#sdk-installation)
  * [SDK Example Usage](#sdk-example-usage)
  * [Available Resources and Operations](#available-resources-and-operations)
  * [Retries](#retries)
  * [Error Handling](#error-handling)
  * [Server Selection](#server-selection)
  * [Custom HTTP Client](#custom-http-client)
  * [Authentication](#authentication)
* [Development](#development)
  * [Maturity](#maturity)
  * [Contributions](#contributions)

<!-- End Table of Contents [toc] -->

<!-- Start SDK Installation [installation] -->
## SDK Installation

To add the SDK as a dependency to your project:
```bash
go get github.com/formancehq/ledger/pkg/client
```
<!-- End SDK Installation [installation] -->

<!-- Start SDK Example Usage [usage] -->
## SDK Example Usage

### Example

```go
package main

import (
	"context"
	"github.com/formancehq/ledger/pkg/client"
	"github.com/formancehq/ledger/pkg/client/models/components"
	"log"
	"os"
)

func main() {
	ctx := context.Background()

	s := client.New(
		client.WithSecurity(components.Security{
			ClientID:     client.String(os.Getenv("FORMANCE_CLIENT_ID")),
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
<!-- End SDK Example Usage [usage] -->

<!-- Start Available Resources and Operations [operations] -->
## Available Resources and Operations

<details open>
<summary>Available methods</summary>


### [Ledger](docs/sdks/ledger/README.md)

* [GetInfo](docs/sdks/ledger/README.md#getinfo) - Show server information
* [GetMetrics](docs/sdks/ledger/README.md#getmetrics) - Read in memory metrics

#### [Ledger.V1](docs/sdks/v1/README.md)

* [GetInfo](docs/sdks/v1/README.md#getinfo) - Show server information
* [GetLedgerInfo](docs/sdks/v1/README.md#getledgerinfo) - Get information about a ledger
* [CountAccounts](docs/sdks/v1/README.md#countaccounts) - Count the accounts from a ledger
* [ListAccounts](docs/sdks/v1/README.md#listaccounts) - List accounts from a ledger
* [GetAccount](docs/sdks/v1/README.md#getaccount) - Get account by its address
* [AddMetadataToAccount](docs/sdks/v1/README.md#addmetadatatoaccount) - Add metadata to an account
* [GetMapping](docs/sdks/v1/README.md#getmapping) - Get the mapping of a ledger
* [UpdateMapping](docs/sdks/v1/README.md#updatemapping) - Update the mapping of a ledger
* [~~RunScript~~](docs/sdks/v1/README.md#runscript) - Execute a Numscript :warning: **Deprecated**
* [ReadStats](docs/sdks/v1/README.md#readstats) - Get statistics from a ledger
* [CountTransactions](docs/sdks/v1/README.md#counttransactions) - Count the transactions from a ledger
* [ListTransactions](docs/sdks/v1/README.md#listtransactions) - List transactions from a ledger
* [CreateTransaction](docs/sdks/v1/README.md#createtransaction) - Create a new transaction to a ledger
* [GetTransaction](docs/sdks/v1/README.md#gettransaction) - Get transaction from a ledger by its ID
* [AddMetadataOnTransaction](docs/sdks/v1/README.md#addmetadataontransaction) - Set the metadata of a transaction by its ID
* [RevertTransaction](docs/sdks/v1/README.md#reverttransaction) - Revert a ledger transaction by its ID
* [CreateTransactions](docs/sdks/v1/README.md#createtransactions) - Create a new batch of transactions to a ledger
* [GetBalances](docs/sdks/v1/README.md#getbalances) - Get the balances from a ledger's account
* [GetBalancesAggregated](docs/sdks/v1/README.md#getbalancesaggregated) - Get the aggregated balances from selected accounts
* [ListLogs](docs/sdks/v1/README.md#listlogs) - List the logs from a ledger

#### [Ledger.V2](docs/sdks/v2/README.md)

* [ListLedgers](docs/sdks/v2/README.md#listledgers) - List ledgers
* [GetLedger](docs/sdks/v2/README.md#getledger) - Get a ledger
* [CreateLedger](docs/sdks/v2/README.md#createledger) - Create a ledger
* [UpdateLedgerMetadata](docs/sdks/v2/README.md#updateledgermetadata) - Update ledger metadata
* [DeleteLedgerMetadata](docs/sdks/v2/README.md#deleteledgermetadata) - Delete ledger metadata by key
* [GetLedgerInfo](docs/sdks/v2/README.md#getledgerinfo) - Get information about a ledger
* [CreateBulk](docs/sdks/v2/README.md#createbulk) - Bulk request
* [CountAccounts](docs/sdks/v2/README.md#countaccounts) - Count the accounts from a ledger
* [ListAccounts](docs/sdks/v2/README.md#listaccounts) - List accounts from a ledger
* [GetAccount](docs/sdks/v2/README.md#getaccount) - Get account by its address
* [AddMetadataToAccount](docs/sdks/v2/README.md#addmetadatatoaccount) - Add metadata to an account
* [DeleteAccountMetadata](docs/sdks/v2/README.md#deleteaccountmetadata) - Delete metadata by key
* [ReadStats](docs/sdks/v2/README.md#readstats) - Get statistics from a ledger
* [CountTransactions](docs/sdks/v2/README.md#counttransactions) - Count the transactions from a ledger
* [ListTransactions](docs/sdks/v2/README.md#listtransactions) - List transactions from a ledger
* [CreateTransaction](docs/sdks/v2/README.md#createtransaction) - Create a new transaction to a ledger
* [GetTransaction](docs/sdks/v2/README.md#gettransaction) - Get transaction from a ledger by its ID
* [AddMetadataOnTransaction](docs/sdks/v2/README.md#addmetadataontransaction) - Set the metadata of a transaction by its ID
* [DeleteTransactionMetadata](docs/sdks/v2/README.md#deletetransactionmetadata) - Delete metadata by key
* [RevertTransaction](docs/sdks/v2/README.md#reverttransaction) - Revert a ledger transaction by its ID
* [GetBalancesAggregated](docs/sdks/v2/README.md#getbalancesaggregated) - Get the aggregated balances from selected accounts
* [GetVolumesWithBalances](docs/sdks/v2/README.md#getvolumeswithbalances) - Get list of volumes with balances for (account/asset)
* [ListLogs](docs/sdks/v2/README.md#listlogs) - List the logs from a ledger
* [ImportLogs](docs/sdks/v2/README.md#importlogs)
* [ExportLogs](docs/sdks/v2/README.md#exportlogs) - Export logs
* [ListExporters](docs/sdks/v2/README.md#listexporters) - List exporters
* [CreateExporter](docs/sdks/v2/README.md#createexporter) - Create exporter
* [GetExporterState](docs/sdks/v2/README.md#getexporterstate) - Get exporter state
* [DeleteExporter](docs/sdks/v2/README.md#deleteexporter) - Delete exporter
* [ListPipelines](docs/sdks/v2/README.md#listpipelines) - List pipelines
* [CreatePipeline](docs/sdks/v2/README.md#createpipeline) - Create pipeline
* [GetPipelineState](docs/sdks/v2/README.md#getpipelinestate) - Get pipeline state
* [DeletePipeline](docs/sdks/v2/README.md#deletepipeline) - Delete pipeline
* [ResetPipeline](docs/sdks/v2/README.md#resetpipeline) - Reset pipeline
* [StartPipeline](docs/sdks/v2/README.md#startpipeline) - Start pipeline
* [StopPipeline](docs/sdks/v2/README.md#stoppipeline) - Stop pipeline

</details>
<!-- End Available Resources and Operations [operations] -->

<!-- Start Retries [retries] -->
## Retries

Some of the endpoints in this SDK support retries. If you use the SDK without any configuration, it will fall back to the default retry strategy provided by the API. However, the default retry strategy can be overridden on a per-operation basis, or across the entire SDK.

To change the default retry strategy for a single API call, simply provide a `retry.Config` object to the call by using the `WithRetries` option:
```go
package main

import (
	"context"
	"github.com/formancehq/ledger/pkg/client"
	"github.com/formancehq/ledger/pkg/client/models/components"
	"github.com/formancehq/ledger/pkg/client/retry"
	"log"
	"models/operations"
	"os"
)

func main() {
	ctx := context.Background()

	s := client.New(
		client.WithSecurity(components.Security{
			ClientID:     client.String(os.Getenv("FORMANCE_CLIENT_ID")),
			ClientSecret: client.String(os.Getenv("FORMANCE_CLIENT_SECRET")),
		}),
	)

	res, err := s.Ledger.GetInfo(ctx, operations.WithRetries(
		retry.Config{
			Strategy: "backoff",
			Backoff: &retry.BackoffStrategy{
				InitialInterval: 1,
				MaxInterval:     50,
				Exponent:        1.1,
				MaxElapsedTime:  100,
			},
			RetryConnectionErrors: false,
		}))
	if err != nil {
		log.Fatal(err)
	}
	if res.V2ConfigInfoResponse != nil {
		// handle response
	}
}

```

If you'd like to override the default retry strategy for all operations that support retries, you can use the `WithRetryConfig` option at SDK initialization:
```go
package main

import (
	"context"
	"github.com/formancehq/ledger/pkg/client"
	"github.com/formancehq/ledger/pkg/client/models/components"
	"github.com/formancehq/ledger/pkg/client/retry"
	"log"
	"os"
)

func main() {
	ctx := context.Background()

	s := client.New(
		client.WithRetryConfig(
			retry.Config{
				Strategy: "backoff",
				Backoff: &retry.BackoffStrategy{
					InitialInterval: 1,
					MaxInterval:     50,
					Exponent:        1.1,
					MaxElapsedTime:  100,
				},
				RetryConnectionErrors: false,
			}),
		client.WithSecurity(components.Security{
			ClientID:     client.String(os.Getenv("FORMANCE_CLIENT_ID")),
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
<!-- End Retries [retries] -->

<!-- Start Error Handling [errors] -->
## Error Handling

Handling errors in this SDK should largely match your expectations. All operations return a response object or an error, they will never return both.

By Default, an API error will return `sdkerrors.SDKError`. When custom error responses are specified for an operation, the SDK may also return their associated error. You can refer to respective *Errors* tables in SDK docs for more details on possible error types for each operation.

For example, the `GetInfo` function may return the following errors:

| Error Type                | Status Code | Content Type     |
| ------------------------- | ----------- | ---------------- |
| sdkerrors.V2ErrorResponse | default     | application/json |
| sdkerrors.SDKError        | 4XX, 5XX    | \*/\*            |

### Example

```go
package main

import (
	"context"
	"errors"
	"github.com/formancehq/ledger/pkg/client"
	"github.com/formancehq/ledger/pkg/client/models/components"
	"github.com/formancehq/ledger/pkg/client/models/sdkerrors"
	"log"
	"os"
)

func main() {
	ctx := context.Background()

	s := client.New(
		client.WithSecurity(components.Security{
			ClientID:     client.String(os.Getenv("FORMANCE_CLIENT_ID")),
			ClientSecret: client.String(os.Getenv("FORMANCE_CLIENT_SECRET")),
		}),
	)

	res, err := s.Ledger.GetInfo(ctx)
	if err != nil {

		var e *sdkerrors.V2ErrorResponse
		if errors.As(err, &e) {
			// handle error
			log.Fatal(e.Error())
		}

		var e *sdkerrors.SDKError
		if errors.As(err, &e) {
			// handle error
			log.Fatal(e.Error())
		}
	}
}

```
<!-- End Error Handling [errors] -->

<!-- Start Server Selection [server] -->
## Server Selection

### Override Server URL Per-Client

The default server can be overridden globally using the `WithServerURL(serverURL string)` option when initializing the SDK client instance. For example:
```go
package main

import (
	"context"
	"github.com/formancehq/ledger/pkg/client"
	"github.com/formancehq/ledger/pkg/client/models/components"
	"log"
	"os"
)

func main() {
	ctx := context.Background()

	s := client.New(
		client.WithServerURL("http://localhost:8080/"),
		client.WithSecurity(components.Security{
			ClientID:     client.String(os.Getenv("FORMANCE_CLIENT_ID")),
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
<!-- End Server Selection [server] -->

<!-- Start Custom HTTP Client [http-client] -->
## Custom HTTP Client

The Go SDK makes API calls that wrap an internal HTTP client. The requirements for the HTTP client are very simple. It must match this interface:

```go
type HTTPClient interface {
	Do(req *http.Request) (*http.Response, error)
}
```

The built-in `net/http` client satisfies this interface and a default client based on the built-in is provided by default. To replace this default with a client of your own, you can implement this interface yourself or provide your own client configured as desired. Here's a simple example, which adds a client with a 30 second timeout.

```go
import (
	"net/http"
	"time"
	"github.com/myorg/your-go-sdk"
)

var (
	httpClient = &http.Client{Timeout: 30 * time.Second}
	sdkClient  = sdk.New(sdk.WithClient(httpClient))
)
```

This can be a convenient way to configure timeouts, cookies, proxies, custom headers, and other low-level configuration.
<!-- End Custom HTTP Client [http-client] -->

<!-- Start Authentication [security] -->
## Authentication

### Per-Client Security Schemes

This SDK supports the following security scheme globally:

| Name                          | Type   | Scheme                         | Environment Variable                                                       |
| ----------------------------- | ------ | ------------------------------ | -------------------------------------------------------------------------- |
| `ClientID`<br/>`ClientSecret` | oauth2 | OAuth2 Client Credentials Flow | `FORMANCE_CLIENT_ID`<br/>`FORMANCE_CLIENT_SECRET`<br/>`FORMANCE_TOKEN_URL` |

You can configure it using the `WithSecurity` option when initializing the SDK client instance. For example:
```go
package main

import (
	"context"
	"github.com/formancehq/ledger/pkg/client"
	"github.com/formancehq/ledger/pkg/client/models/components"
	"log"
	"os"
)

func main() {
	ctx := context.Background()

	s := client.New(
		client.WithSecurity(components.Security{
			ClientID:     client.String(os.Getenv("FORMANCE_CLIENT_ID")),
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
<!-- End Authentication [security] -->

<!-- Placeholder for Future Speakeasy SDK Sections -->

# Development

## Maturity

This SDK is in beta, and there may be breaking changes between versions without a major version update. Therefore, we recommend pinning usage
to a specific package version. This way, you can install the same version each time without breaking changes unless you are intentionally
looking for the latest version.

## Contributions

While we value open-source contributions to this SDK, this library is generated programmatically. Any manual changes added to internal files will be overwritten on the next generation. 
We look forward to hearing your feedback. Feel free to open a PR or an issue with a proof of concept and we'll do our best to include it in a future release. 

### SDK Created by [Speakeasy](https://www.speakeasy.com/?utm_source=github-com/formancehq/stack/ledger/client&utm_campaign=go)
