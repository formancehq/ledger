package kafka

import (
	"context"
	"time"
)

type contextKey int

const (
	_ contextKey = iota
	partitionContextKey
	partitionOffsetContextKey
	timestampContextKey
	keyContextKey
)

func setPartitionToCtx(ctx context.Context, partition int32) context.Context {
	return context.WithValue(ctx, partitionContextKey, partition)
}

// MessagePartitionFromCtx returns Kafka partition of the consumed message
func MessagePartitionFromCtx(ctx context.Context) (int32, bool) {
	partition, ok := ctx.Value(partitionContextKey).(int32)
	return partition, ok
}

func setPartitionOffsetToCtx(ctx context.Context, offset int64) context.Context {
	return context.WithValue(ctx, partitionOffsetContextKey, offset)
}

// MessagePartitionOffsetFromCtx returns Kafka partition offset of the consumed message
func MessagePartitionOffsetFromCtx(ctx context.Context) (int64, bool) {
	offset, ok := ctx.Value(partitionOffsetContextKey).(int64)
	return offset, ok
}

func setMessageTimestampToCtx(ctx context.Context, timestamp time.Time) context.Context {
	return context.WithValue(ctx, timestampContextKey, timestamp)
}

// MessageTimestampFromCtx returns Kafka internal timestamp of the consumed message
func MessageTimestampFromCtx(ctx context.Context) (time.Time, bool) {
	timestamp, ok := ctx.Value(timestampContextKey).(time.Time)
	return timestamp, ok
}

func setMessageKeyToCtx(ctx context.Context, key []byte) context.Context {
	return context.WithValue(ctx, keyContextKey, key)
}

// MessageKeyFromCtx returns Kafka internal key of the consumed message
func MessageKeyFromCtx(ctx context.Context) ([]byte, bool) {
	key, ok := ctx.Value(keyContextKey).([]byte)
	return key, ok
}
