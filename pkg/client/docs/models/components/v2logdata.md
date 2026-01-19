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

