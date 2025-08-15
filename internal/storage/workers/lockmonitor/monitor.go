package lockmonitor

import (
	"context"
)

type Monitor interface {
	Accept(ctx context.Context, locks []Lock)
}
