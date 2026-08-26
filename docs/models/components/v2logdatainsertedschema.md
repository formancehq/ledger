# V2LogDataInsertedSchema

Payload for INSERTED_SCHEMA log entries. Contains the schema that was inserted into the ledger.


## Fields

| Field                                                      | Type                                                       | Required                                                   | Description                                                |
| ---------------------------------------------------------- | ---------------------------------------------------------- | ---------------------------------------------------------- | ---------------------------------------------------------- |
| `Schema`                                                   | [components.V2Schema](../../models/components/v2schema.md) | :heavy_check_mark:                                         | Complete schema structure with metadata                    |