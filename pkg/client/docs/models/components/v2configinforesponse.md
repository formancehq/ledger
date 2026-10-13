# V2ConfigInfoResponse


## Fields

| Field                                                 | Type                                                  | Required                                              | Description                                           |
| ----------------------------------------------------- | ----------------------------------------------------- | ----------------------------------------------------- | ----------------------------------------------------- |
| `Server`                                              | `string`                                              | :heavy_check_mark:                                    | Name of the server serving the API                    |
| `Version`                                             | `string`                                              | :heavy_check_mark:                                    | Version of the ledger service                         |
| `ExperimentalFeatures`                                | []`string`                                            | :heavy_minus_sign:                                    | Experimental feature flags enabled on this deployment |