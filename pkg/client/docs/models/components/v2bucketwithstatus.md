# V2BucketWithStatus


## Fields

| Field                                                             | Type                                                              | Required                                                          | Description                                                       | Example                                                           |
| ----------------------------------------------------------------- | ----------------------------------------------------------------- | ----------------------------------------------------------------- | ----------------------------------------------------------------- | ----------------------------------------------------------------- |
| `Name`                                                            | *string*                                                          | :heavy_check_mark:                                                | Name of the bucket                                                | default                                                           |
| `DeletedAt`                                                       | [*time.Time](https://pkg.go.dev/time#Time)                        | :heavy_minus_sign:                                                | Timestamp when the bucket was marked for deletion, null if active |                                                                   |