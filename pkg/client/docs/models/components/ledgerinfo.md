# LedgerInfo


## Fields

| Field                                     | Type                                      | Required                                  | Description                               |
| ----------------------------------------- | ----------------------------------------- | ----------------------------------------- | ----------------------------------------- |
| `ID`                                      | *int64*                                   | :heavy_check_mark:                        | Sequential ID for the ledger              |
| `Name`                                    | *string*                                  | :heavy_check_mark:                        | Name of the ledger                        |
| `Bucket`                                  | *string*                                  | :heavy_check_mark:                        | Name of the bucket containing the ledger  |
| `Metadata`                                | map[string]*string*                       | :heavy_minus_sign:                        | Metadata for the ledger                   |
| `CreatedAt`                               | [time.Time](https://pkg.go.dev/time#Time) | :heavy_check_mark:                        | Creation timestamp (ISO 8601 format)      |
| `LastLogID`                               | **int64*                                  | :heavy_minus_sign:                        | ID of the last log for this ledger        |