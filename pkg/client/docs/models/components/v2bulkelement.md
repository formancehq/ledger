# V2BulkElement


## Supported Types

### V2BulkElementCreateTransaction

```go
v2BulkElement := components.CreateV2BulkElementCreateTransaction(components.V2BulkElementCreateTransaction{/* values here */})
```

### V2BulkElementAddMetadata

```go
v2BulkElement := components.CreateV2BulkElementAddMetadata(components.V2BulkElementAddMetadata{/* values here */})
```

### V2BulkElementRevertTransaction

```go
v2BulkElement := components.CreateV2BulkElementRevertTransaction(components.V2BulkElementRevertTransaction{/* values here */})
```

### V2BulkElementDeleteMetadata

```go
v2BulkElement := components.CreateV2BulkElementDeleteMetadata(components.V2BulkElementDeleteMetadata{/* values here */})
```

## Union Discrimination

Use the `Type` field to determine which variant is active, then access the corresponding field:

```go
switch v2BulkElement.Type {
	case components.V2BulkElementTypeCreateTransaction:
		// v2BulkElement.V2BulkElementCreateTransaction is populated
	case components.V2BulkElementTypeAddMetadata:
		// v2BulkElement.V2BulkElementAddMetadata is populated
	case components.V2BulkElementTypeRevertTransaction:
		// v2BulkElement.V2BulkElementRevertTransaction is populated
	case components.V2BulkElementTypeDeleteMetadata:
		// v2BulkElement.V2BulkElementDeleteMetadata is populated
}
```
