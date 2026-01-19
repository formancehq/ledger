# V2LogDataSetMetadata

Payload for SET_METADATA log entries. Contains the target entity and the metadata that was set.


## Fields

| Field                                                          | Type                                                           | Required                                                       | Description                                                    | Example                                                        |
| -------------------------------------------------------------- | -------------------------------------------------------------- | -------------------------------------------------------------- | -------------------------------------------------------------- | -------------------------------------------------------------- |
| `TargetType`                                                   | [components.TargetType](../../models/components/targettype.md) | :heavy_check_mark:                                             | Type of the target entity                                      |                                                                |
| `TargetID`                                                     | [components.TargetID](../../models/components/targetid.md)     | :heavy_check_mark:                                             | N/A                                                            |                                                                |
| `Metadata`                                                     | map[string]*string*                                            | :heavy_check_mark:                                             | N/A                                                            | {<br/>"admin": "true"<br/>}                                    |