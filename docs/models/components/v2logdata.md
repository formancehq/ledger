# V2LogData

The payload of the log entry. Structure depends on the log type:
- NEW_TRANSACTION: V2LogDataNewTransaction
- SET_METADATA: V2LogDataSetMetadata
- REVERTED_TRANSACTION: V2LogDataRevertedTransaction
- DELETE_METADATA: V2LogDataDeleteMetadata
- INSERTED_SCHEMA: V2LogDataInsertedSchema



## Supported Types

### V2LogDataNewTransaction

```go
v2LogData := components.CreateV2LogDataV2LogDataNewTransaction(components.V2LogDataNewTransaction{/* values here */})
```

### V2LogDataSetMetadata

```go
v2LogData := components.CreateV2LogDataV2LogDataSetMetadata(components.V2LogDataSetMetadata{/* values here */})
```

### V2LogDataRevertedTransaction

```go
v2LogData := components.CreateV2LogDataV2LogDataRevertedTransaction(components.V2LogDataRevertedTransaction{/* values here */})
```

### V2LogDataDeleteMetadata

```go
v2LogData := components.CreateV2LogDataV2LogDataDeleteMetadata(components.V2LogDataDeleteMetadata{/* values here */})
```

### V2LogDataInsertedSchema

```go
v2LogData := components.CreateV2LogDataV2LogDataInsertedSchema(components.V2LogDataInsertedSchema{/* values here */})
```

## Union Discrimination

Use the `Type` field to determine which variant is active, then access the corresponding field:

```go
switch v2LogData.Type {
	case components.V2LogDataTypeV2LogDataNewTransaction:
		// v2LogData.V2LogDataNewTransaction is populated
	case components.V2LogDataTypeV2LogDataSetMetadata:
		// v2LogData.V2LogDataSetMetadata is populated
	case components.V2LogDataTypeV2LogDataRevertedTransaction:
		// v2LogData.V2LogDataRevertedTransaction is populated
	case components.V2LogDataTypeV2LogDataDeleteMetadata:
		// v2LogData.V2LogDataDeleteMetadata is populated
	case components.V2LogDataTypeV2LogDataInsertedSchema:
		// v2LogData.V2LogDataInsertedSchema is populated
}
```
