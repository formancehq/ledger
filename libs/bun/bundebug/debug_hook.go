package bundebug

import (
	"context"
	"fmt"
	"time"

	"github.com/formancehq/stack/libs/go-libs/logging"

	"github.com/uptrace/bun"
)

type QueryHook struct{}

var _ bun.QueryHook = (*QueryHook)(nil)

func NewQueryHook() *QueryHook {
	return &QueryHook{}
}

func (h *QueryHook) BeforeQuery(
	ctx context.Context, event *bun.QueryEvent,
) context.Context {
	return ctx
}

func (h *QueryHook) AfterQuery(ctx context.Context, event *bun.QueryEvent) {
	dur := time.Since(event.StartTime)

	fields := map[string]any{
		"component": "bun",
		"operation": event.Operation(),
		"duration":  fmt.Sprintf("%s", dur.Round(time.Microsecond)),
	}

	if event.Err != nil {
		fields["err"] = event.Err.Error()
	}

	logging.FromContext(ctx).WithFields(fields).Debug(event.Query)
}
