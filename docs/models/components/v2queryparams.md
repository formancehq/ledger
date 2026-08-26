# V2QueryParams


## Supported Types

### QueryTemplateAccountParams

```go
v2QueryParams := components.CreateV2QueryParamsQueryTemplateAccountParams(components.QueryTemplateAccountParams{/* values here */})
```

### QueryTemplateTransactionParams

```go
v2QueryParams := components.CreateV2QueryParamsQueryTemplateTransactionParams(components.QueryTemplateTransactionParams{/* values here */})
```

### QueryTemplateLogParams

```go
v2QueryParams := components.CreateV2QueryParamsQueryTemplateLogParams(components.QueryTemplateLogParams{/* values here */})
```

### QueryTemplateVolumeParams

```go
v2QueryParams := components.CreateV2QueryParamsQueryTemplateVolumeParams(components.QueryTemplateVolumeParams{/* values here */})
```

## Union Discrimination

Use the `Type` field to determine which variant is active, then access the corresponding field:

```go
switch v2QueryParams.Type {
	case components.V2QueryParamsTypeQueryTemplateAccountParams:
		// v2QueryParams.QueryTemplateAccountParams is populated
	case components.V2QueryParamsTypeQueryTemplateTransactionParams:
		// v2QueryParams.QueryTemplateTransactionParams is populated
	case components.V2QueryParamsTypeQueryTemplateLogParams:
		// v2QueryParams.QueryTemplateLogParams is populated
	case components.V2QueryParamsTypeQueryTemplateVolumeParams:
		// v2QueryParams.QueryTemplateVolumeParams is populated
}
```
