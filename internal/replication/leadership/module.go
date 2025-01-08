package leadership

import (
	"github.com/formancehq/ledger/internal/utils"
	"go.uber.org/fx"
)

func NewFXModule() fx.Option {
	return fx.Options(
		fx.Provide(NewLeadership),
		fx.Invoke(utils.StartRunner[*Leadership]()),
	)
}
