# TargetID


## Supported Types

### 

```go
targetID := components.CreateTargetIDStr(string{/* values here */})
```

### 

```go
targetID := components.CreateTargetIDBigint(*big.Int{/* values here */})
```

## Union Discrimination

Use the `Type` field to determine which variant is active, then access the corresponding field:

```go
switch targetID.Type {
	case components.TargetIDTypeStr:
		// targetID.Str is populated
	case components.TargetIDTypeBigint:
		// targetID.Bigint is populated
}
```
