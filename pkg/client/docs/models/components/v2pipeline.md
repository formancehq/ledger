# V2Pipeline


## Fields

| Field                                         | Type                                          | Required                                      | Description                                   |
| --------------------------------------------- | --------------------------------------------- | --------------------------------------------- | --------------------------------------------- |
| `Ledger`                                      | `string`                                      | :heavy_check_mark:                            | Name of the ledger the pipeline reads from    |
| `ExporterID`                                  | `string`                                      | :heavy_check_mark:                            | Identifier of the exporter the pipeline feeds |
| `ID`                                          | `string`                                      | :heavy_check_mark:                            | N/A                                           |
| `CreatedAt`                                   | [time.Time](https://pkg.go.dev/time#Time)     | :heavy_check_mark:                            | N/A                                           |
| `LastLogID`                                   | `*int64`                                      | :heavy_minus_sign:                            | N/A                                           |
| `Enabled`                                     | `*bool`                                       | :heavy_minus_sign:                            | N/A                                           |