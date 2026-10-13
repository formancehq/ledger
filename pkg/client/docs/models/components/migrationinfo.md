# MigrationInfo


## Fields

| Field                                                 | Type                                                  | Required                                              | Description                                           | Example                                               |
| ----------------------------------------------------- | ----------------------------------------------------- | ----------------------------------------------------- | ----------------------------------------------------- | ----------------------------------------------------- |
| `Version`                                             | `*string`                                             | :heavy_minus_sign:                                    | Sequence number of the migration                      | 11                                                    |
| `Name`                                                | `*string`                                             | :heavy_minus_sign:                                    | Name of the migration                                 | migrations:001                                        |
| `Date`                                                | [*time.Time](https://pkg.go.dev/time#Time)            | :heavy_minus_sign:                                    | When the migration was applied                        |                                                       |
| `State`                                               | [*components.State](../../models/components/state.md) | :heavy_minus_sign:                                    | Current state of the migration                        |                                                       |