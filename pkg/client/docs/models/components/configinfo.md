# ConfigInfo

Configuration and version information for the ledger server


## Fields

| Field                                                  | Type                                                   | Required                                               | Description                                            |
| ------------------------------------------------------ | ------------------------------------------------------ | ------------------------------------------------------ | ------------------------------------------------------ |
| `Config`                                               | [components.Config](../../models/components/config.md) | :heavy_check_mark:                                     | Configuration the ledger server is running with        |
| `Server`                                               | `string`                                               | :heavy_check_mark:                                     | Name of the server serving the API                     |
| `Version`                                              | `string`                                               | :heavy_check_mark:                                     | Version of the ledger service                          |
| `ExperimentalFeatures`                                 | []`string`                                             | :heavy_minus_sign:                                     | Experimental feature flags enabled on this deployment  |