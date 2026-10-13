# V2LogDataDeleteMetadataTargetID

Identifier of the entity the metadata was deleted from, either an account address or a transaction ID


## Supported Types

### 

```go
v2LogDataDeleteMetadataTargetID := components.CreateV2LogDataDeleteMetadataTargetIDStr(string{/* values here */})
```

### 

```go
v2LogDataDeleteMetadataTargetID := components.CreateV2LogDataDeleteMetadataTargetIDBigint(*big.Int{/* values here */})
```

## Union Discrimination

Use the `Type` field to determine which variant is active, then access the corresponding field:

```go
switch v2LogDataDeleteMetadataTargetID.Type {
	case components.V2LogDataDeleteMetadataTargetIDTypeStr:
		// v2LogDataDeleteMetadataTargetID.Str is populated
	case components.V2LogDataDeleteMetadataTargetIDTypeBigint:
		// v2LogDataDeleteMetadataTargetID.Bigint is populated
}
```
