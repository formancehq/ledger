package licence

import (
	"context"
	"time"

	"github.com/formancehq/stack/libs/go-libs/errorsutils"
	"github.com/formancehq/stack/libs/go-libs/logging"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.uber.org/fx"
)

const (
	LicenceEnabled            = "licence-enabled"
	LicenceTokenFlag          = "licence-token"
	LicenceValidateTickFlag   = "licence-validate-tick"
	LicenceClusterIDFlag      = "licence-cluster-id"
	LicenceExpectedIssuerFlag = "licence-issuer"
)

func InitCLIFlags(cmd *cobra.Command) {
	cmd.PersistentFlags().Bool(LicenceEnabled, false, "Enable licence check")
	cmd.PersistentFlags().String(LicenceTokenFlag, "", "Licence token")
	cmd.PersistentFlags().Duration(LicenceValidateTickFlag, 2*time.Minute, "Licence validate tick")
	cmd.PersistentFlags().String(LicenceClusterIDFlag, "", "Licence cluster ID")
	cmd.PersistentFlags().String(LicenceExpectedIssuerFlag, "", "Licence expected issuer")
}

func CLIModule(
	serviceName string,
) fx.Option {
	options := make([]fx.Option, 0)

	licenceChanError := make(chan error, 1)

	if viper.GetBool(LicenceEnabled) {
		options = append(options,
			fx.Provide(func(logger logging.Logger) *Licence {
				return NewLicence(
					logger,
					viper.GetString(LicenceTokenFlag),
					viper.GetDuration(LicenceValidateTickFlag),
					serviceName,
					viper.GetString(LicenceClusterIDFlag),
					viper.GetString(LicenceExpectedIssuerFlag),
				)

			}),
			fx.Invoke(func(lc fx.Lifecycle, l *Licence, shutdowner fx.Shutdowner) {
				lc.Append(fx.Hook{
					OnStart: func(ctx context.Context) error {
						if err := l.Start(licenceChanError); err != nil {
							return errorsutils.NewErrorWithExitCode(err, 126)
						}

						go waitLicenceError(licenceChanError, shutdowner)

						return nil
					},
					OnStop: func(ctx context.Context) error {
						l.Stop()
						close(licenceChanError)
						return nil
					},
				})
			}),
		)
	}

	return fx.Options(options...)
}

func waitLicenceError(
	licenceErrorChan chan error,
	shutdowner fx.Shutdowner,
) {
	for err := range licenceErrorChan {
		if err != nil {
			shutdowner.Shutdown(fx.ExitCode(126))
			return
		}
	}
}
