# LedgerInfo


## Fields

| Field                                     | Type                                      | Required                                  | Description                               |
| ----------------------------------------- | ----------------------------------------- | ----------------------------------------- | ----------------------------------------- |
| `Name`                                    | *string*                                  | :heavy_check_mark:                        | Name of the ledger                        |
| `Metadata`                                | map[string]*string*                       | :heavy_minus_sign:                        | Metadata for the ledger                   |
| `CreatedAt`                               | [time.Time](https://pkg.go.dev/time#Time) | :heavy_check_mark:                        | Creation timestamp (ISO 8601 format)      |