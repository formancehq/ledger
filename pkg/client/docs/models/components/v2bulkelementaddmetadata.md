# V2BulkElementAddMetadata


## Fields

| Field                                                                            | Type                                                                             | Required                                                                         | Description                                                                      |
| -------------------------------------------------------------------------------- | -------------------------------------------------------------------------------- | -------------------------------------------------------------------------------- | -------------------------------------------------------------------------------- |
| `Action`                                                                         | `string`                                                                         | :heavy_check_mark:                                                               | The bulk action this element performs                                            |
| `Ik`                                                                             | `*string`                                                                        | :heavy_minus_sign:                                                               | Idempotency key scoped to this element, making it safe to retry the bulk request |
| `Data`                                                                           | [*components.Data](../../models/components/data.md)                              | :heavy_minus_sign:                                                               | N/A                                                                              |