# V2QueryTemplate

A query template. `resource` selects which parameters apply.


## Supported Types

### V2AccountQueryTemplate

```go
v2QueryTemplate := components.CreateV2QueryTemplateAccounts(components.V2AccountQueryTemplate{/* values here */})
```

### V2TransactionQueryTemplate

```go
v2QueryTemplate := components.CreateV2QueryTemplateTransactions(components.V2TransactionQueryTemplate{/* values here */})
```

### V2LogQueryTemplate

```go
v2QueryTemplate := components.CreateV2QueryTemplateLogs(components.V2LogQueryTemplate{/* values here */})
```

### V2VolumeQueryTemplate

```go
v2QueryTemplate := components.CreateV2QueryTemplateVolumes(components.V2VolumeQueryTemplate{/* values here */})
```

## Union Discrimination

Use the `Type` field to determine which variant is active, then access the corresponding field:

```go
switch v2QueryTemplate.Type {
	case components.V2QueryTemplateTypeAccounts:
		// v2QueryTemplate.V2AccountQueryTemplate is populated
	case components.V2QueryTemplateTypeTransactions:
		// v2QueryTemplate.V2TransactionQueryTemplate is populated
	case components.V2QueryTemplateTypeLogs:
		// v2QueryTemplate.V2LogQueryTemplate is populated
	case components.V2QueryTemplateTypeVolumes:
		// v2QueryTemplate.V2VolumeQueryTemplate is populated
}
```
