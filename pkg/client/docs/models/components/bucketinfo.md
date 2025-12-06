# BucketInfo


## Fields

| Field                                      | Type                                       | Required                                   | Description                                |
| ------------------------------------------ | ------------------------------------------ | ------------------------------------------ | ------------------------------------------ |
| `Name`                                     | **string*                                  | :heavy_minus_sign:                         | Bucket name                                |
| `Driver`                                   | **string*                                  | :heavy_minus_sign:                         | Driver name                                |
| `Config`                                   | map[string]*any*                           | :heavy_minus_sign:                         | Driver-specific configuration              |
| `CreatedAt`                                | [*time.Time](https://pkg.go.dev/time#Time) | :heavy_minus_sign:                         | Creation timestamp (ISO 8601 format)       |