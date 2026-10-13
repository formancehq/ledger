# V2Ledger

A ledger and its configuration


## Fields

| Field                                                      | Type                                                       | Required                                                   | Description                                                | Example                                                    |
| ---------------------------------------------------------- | ---------------------------------------------------------- | ---------------------------------------------------------- | ---------------------------------------------------------- | ---------------------------------------------------------- |
| `Name`                                                     | `string`                                                   | :heavy_check_mark:                                         | Name of the ledger                                         |                                                            |
| `AddedAt`                                                  | [time.Time](https://pkg.go.dev/time#Time)                  | :heavy_check_mark:                                         | When the ledger was created                                |                                                            |
| `Bucket`                                                   | `string`                                                   | :heavy_check_mark:                                         | Name of the storage bucket backing the ledger              |                                                            |
| `DeletedAt`                                                | [*time.Time](https://pkg.go.dev/time#Time)                 | :heavy_minus_sign:                                         | When the ledger was deleted, absent for active ledgers     |                                                            |
| `Metadata`                                                 | map[string]`string`                                        | :heavy_minus_sign:                                         | Arbitrary key/value pairs attached to the resource         | {<br/>"admin": "true"<br/>}                                |
| `Features`                                                 | map[string]`string`                                        | :heavy_minus_sign:                                         | Feature flags enabled on the ledger, keyed by feature name |                                                            |
| `ID`                                                       | `*int64`                                                   | :heavy_minus_sign:                                         | Unique sequential identifier for the ledger                |                                                            |