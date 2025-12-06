# LedgerInfo


## Fields

| Field                                      | Type                                       | Required                                   | Description                                |
| ------------------------------------------ | ------------------------------------------ | ------------------------------------------ | ------------------------------------------ |
| `Name`                                     | **string*                                  | :heavy_minus_sign:                         | Name of the ledger                         |
| `Bucket`                                   | **string*                                  | :heavy_minus_sign:                         | Name of the bucket containing the ledger   |
| `Metadata`                                 | map[string]*string*                        | :heavy_minus_sign:                         | Metadata for the ledger                    |
| `CreatedAt`                                | [*time.Time](https://pkg.go.dev/time#Time) | :heavy_minus_sign:                         | Creation timestamp (ISO 8601 format)       |