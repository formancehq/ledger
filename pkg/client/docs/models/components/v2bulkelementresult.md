# V2BulkElementResult


## Supported Types

### V2BulkElementResultCreateTransaction

```go
v2BulkElementResult := components.CreateV2BulkElementResultCreateTransaction(components.V2BulkElementResultCreateTransaction{/* values here */})
```

### V2BulkElementResultAddMetadata

```go
v2BulkElementResult := components.CreateV2BulkElementResultAddMetadata(components.V2BulkElementResultAddMetadata{/* values here */})
```

### V2BulkElementResultRevertTransaction

```go
v2BulkElementResult := components.CreateV2BulkElementResultRevertTransaction(components.V2BulkElementResultRevertTransaction{/* values here */})
```

### V2BulkElementResultDeleteMetadata

```go
v2BulkElementResult := components.CreateV2BulkElementResultDeleteMetadata(components.V2BulkElementResultDeleteMetadata{/* values here */})
```

### V2BulkElementResultError

```go
v2BulkElementResult := components.CreateV2BulkElementResultError(components.V2BulkElementResultError{/* values here */})
```

## Union Discrimination

Use the `Type` field to determine which variant is active, then access the corresponding field:

```go
switch v2BulkElementResult.Type {
	case components.V2BulkElementResultTypeCreateTransaction:
		// v2BulkElementResult.V2BulkElementResultCreateTransaction is populated
	case components.V2BulkElementResultTypeAddMetadata:
		// v2BulkElementResult.V2BulkElementResultAddMetadata is populated
	case components.V2BulkElementResultTypeRevertTransaction:
		// v2BulkElementResult.V2BulkElementResultRevertTransaction is populated
	case components.V2BulkElementResultTypeDeleteMetadata:
		// v2BulkElementResult.V2BulkElementResultDeleteMetadata is populated
	case components.V2BulkElementResultTypeError:
		// v2BulkElementResult.V2BulkElementResultError is populated
}
```
