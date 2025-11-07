//go:build ee

package ee

import (
	"github.com/formancehq/go-libs/v3/audit"
	"github.com/formancehq/go-libs/v3/logging"
	"github.com/go-chi/chi/v5"
	"github.com/spf13/cobra"
	"go.uber.org/fx"
)

// ModuleParams contains dependencies for EE module
type ModuleParams struct {
	fx.In

	Router      chi.Router
	AuditClient *audit.PublisherClient `optional:"true"`
	Logger      logging.Logger
}

// AddFlags adds all EE-related flags to the command
func AddFlags(cmd *cobra.Command) {
	// Add audit flags
	audit.AddFlags(cmd.Flags())
}

// Module returns the Enterprise Edition Fx module
func Module(cobraCmd *cobra.Command) fx.Option {
	return fx.Options(
		// Provide audit module
		AuditModule(cobraCmd),

		// Decorate router to add audit middleware
		fx.Decorate(func(params ModuleParams) chi.Router {
			if params.AuditClient != nil {
				params.Logger.Infof("Adding audit middleware to router (EE)")
				params.Router.Use(audit.HTTPMiddlewareWithPublisher(params.AuditClient))
			}
			return params.Router
		}),
	)
}
