package queries

import (
	"fmt"
)

type ResourceKind string

const (
	ResourceKindTransaction ResourceKind = "transactions"
	ResourceKindAccount     ResourceKind = "accounts"
	ResourceKindLog         ResourceKind = "logs"
	ResourceKindVolume      ResourceKind = "volumes"
)

var Resources []ResourceKind = []ResourceKind{
	ResourceKindTransaction,
	ResourceKindAccount,
	ResourceKindLog,
	ResourceKindVolume,
}

var AccountSchema EntitySchema = EntitySchema{
	Fields: map[string]Field{
		"address":        NewStringField().Paginated(),
		"first_usage":    NewDateField().Paginated(),
		"balance":        NewNumericMapField(),
		"metadata":       NewStringMapField(),
		"insertion_date": NewDateField().Paginated(),
		"updated_at":     NewDateField().Paginated(),
	},
}

var AggregatedBalanceSchema EntitySchema = EntitySchema{
	Fields: map[string]Field{
		"address":  NewStringField().Paginated(),
		"metadata": NewStringMapField(),
	},
}

var LogSchema EntitySchema = EntitySchema{
	Fields: map[string]Field{
		"date": NewDateField().Paginated(),
		"id":   NewNumericField().Paginated(),
		"type": NewStringField(),
	},
}

var SchemaSchema EntitySchema = EntitySchema{
	Fields: map[string]Field{
		"version":    NewStringField().Paginated(),
		"created_at": NewDateField().Paginated(),
	},
}

var TransactionSchema EntitySchema = EntitySchema{
	Fields: map[string]Field{
		"reverted":    NewBooleanField(),
		"account":     NewStringField(),
		"source":      NewStringField(),
		"destination": NewStringField(),
		"timestamp":   NewDateField().Paginated(),
		"metadata":    NewStringMapField(),
		"id":          NewNumericField().Paginated(),
		"reference":   NewStringField(),
		"inserted_at": NewDateField().Paginated(),
		"updated_at":  NewDateField().Paginated(),
		"reverted_at": NewDateField().Paginated(),
	},
}

var VolumeSchema EntitySchema = EntitySchema{
	Fields: map[string]Field{
		"address": NewStringField().
			WithAliases("account").
			Paginated(),
		"balance":     NewNumericMapField(),
		"first_usage": NewDateField(),
		"metadata":    NewStringMapField(),
	},
}

func GetResourceSchema(kind ResourceKind) (*EntitySchema, error) {
	switch kind {
	case ResourceKindAccount:
		return &AccountSchema, nil
	case ResourceKindLog:
		return &LogSchema, nil
	case ResourceKindTransaction:
		return &TransactionSchema, nil
	case ResourceKindVolume:
		return &VolumeSchema, nil
	default:
		return nil, fmt.Errorf("unexpected resources.ResourceKind: %#v", kind)
	}
}
