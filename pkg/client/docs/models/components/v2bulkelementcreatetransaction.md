# V2BulkElementCreateTransaction


## Fields

| Field                                                                              | Type                                                                               | Required                                                                           | Description                                                                        |
| ---------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------- |
| `Action`                                                                           | `string`                                                                           | :heavy_check_mark:                                                                 | N/A                                                                                |
| `Ik`                                                                               | `*string`                                                                          | :heavy_minus_sign:                                                                 | N/A                                                                                |
| `Data`                                                                             | [*components.V2PostTransaction](../../models/components/v2posttransaction.md)      | :heavy_minus_sign:                                                                 | Transaction and account metadata are limited to 256 KiB in total for one command.<br/> |