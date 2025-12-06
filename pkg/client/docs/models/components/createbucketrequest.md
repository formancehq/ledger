# CreateBucketRequest


## Fields

| Field                                      | Type                                       | Required                                   | Description                                |
| ------------------------------------------ | ------------------------------------------ | ------------------------------------------ | ------------------------------------------ |
| `Driver`                                   | *string*                                   | :heavy_check_mark:                         | Driver name (e.g., "postgres", "s3", etc.) |
| `Config`                                   | map[string]*any*                           | :heavy_check_mark:                         | Driver-specific configuration              |