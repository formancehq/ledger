# QueryTemplatePlainParams

Parameters without a nested resource; the parent query selects the resource. groupBy and insertionDate apply only to volumes.


## Fields

| Field                                      | Type                                       | Required                                   | Description                                |
| ------------------------------------------ | ------------------------------------------ | ------------------------------------------ | ------------------------------------------ |
| `PageSize`                                 | `*int64`                                   | :heavy_minus_sign:                         | N/A                                        |
| `Sort`                                     | `*string`                                  | :heavy_minus_sign:                         | N/A                                        |
| `Expand`                                   | []`string`                                 | :heavy_minus_sign:                         | N/A                                        |
| `StartTime`                                | [*time.Time](https://pkg.go.dev/time#Time) | :heavy_minus_sign:                         | N/A                                        |
| `EndTime`                                  | [*time.Time](https://pkg.go.dev/time#Time) | :heavy_minus_sign:                         | N/A                                        |
| `InsertionDate`                            | `*bool`                                    | :heavy_minus_sign:                         | N/A                                        |
| `GroupBy`                                  | `*int64`                                   | :heavy_minus_sign:                         | N/A                                        |