package ledger

import (
	"context"

	"github.com/numary/ledger/pkg/core"
)

func (l *Ledger) Logs(ctx context.Context) ([]core.Log, error) {
	return []core.Log{}, nil
}
