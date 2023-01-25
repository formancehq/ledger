package otlp

import (
	"context"
)

func RecordErrorOnRecover(ctx context.Context, forwardPanic bool) func() {
	return func() {
		if e := recover(); e != nil {
			RecordAsError(ctx, e)
			if forwardPanic {
				panic(e)
			}
		}
	}
}
