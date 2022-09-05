package analytics

import (
	sharedanalytics "github.com/numary/go-libs/sharedanalytics/pkg"
	"go.uber.org/fx"
)

func CustomizeAnalyticsModule() fx.Option {
	return fx.Options(
		fx.Decorate(FromStorageAppIdProvider),
		fx.Provide(fx.Annotate(NewLedgerStatsPropertiesProvider, fx.ResultTags(sharedanalytics.FXTagPropertiesEnrichers),
			fx.As(new(sharedanalytics.PropertiesEnricher)))),
	)
}
