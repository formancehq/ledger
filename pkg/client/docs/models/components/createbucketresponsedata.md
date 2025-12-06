# CreateBucketResponseData


## Fields

| Field                         | Type                          | Required                      | Description                   |
| ----------------------------- | ----------------------------- | ----------------------------- | ----------------------------- |
| `ID`                          | **int64*                      | :heavy_minus_sign:            | Sequential bucket ID          |
| `Name`                        | **string*                     | :heavy_minus_sign:            | Name of the created bucket    |
| `Driver`                      | **string*                     | :heavy_minus_sign:            | Driver name                   |
| `Config`                      | map[string]*any*              | :heavy_minus_sign:            | Driver-specific configuration |