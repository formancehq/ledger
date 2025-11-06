//go:build ee

package cmd

import (
	"github.com/formancehq/ledger/ee"
	eeAudit "github.com/formancehq/ledger/ee/audit"
	"github.com/go-chi/chi/v5"
	"github.com/spf13/cobra"
	"go.uber.org/fx"
)

// getEEModules returns EE-specific FX modules (EE override)
func getEEModules(cmd *cobra.Command) ([]fx.Option, error) {
	// Load EE configuration
	eeCfg, err := ee.LoadConfig(cmd)
	if err != nil {
		return nil, err
	}

	var modules []fx.Option

	// Only add audit module if enabled
	if eeCfg.Audit.Enabled {
		// Auto-detect audit topic from wildcard mapping
		// Example: "*:stack.ledger" → audit topic = "stack.audit"
		auditTopic := eeAudit.BuildAuditTopic(cmd)

		// Audit will reuse existing publisher from FX
		modules = append(modules, eeAudit.NewFXModule(eeAudit.Config{
			Topic:            auditTopic,
			AppName:          ServiceName,
			MaxBodySize:      eeCfg.Audit.MaxBodySize,
			ExcludedPaths:    eeCfg.Audit.ExcludedPaths,
			SensitiveHeaders: eeCfg.Audit.SensitiveHeaders,
		}))
	}

	return modules, nil
}

// injectEEMiddleware injects EE-specific middleware (EE override)
func injectEEMiddleware() fx.Option {
	return fx.Decorate(func(
		params struct {
			fx.In
			Router      chi.Router
			AuditClient *eeAudit.Client `optional:"true"`
		},
	) chi.Router {
		// Only inject audit middleware if EE and client is available
		if ee.IsEnterpriseEdition && params.AuditClient != nil {
			return wrapRouterWithAuditMiddleware(params.Router, params.AuditClient)
		}
		return params.Router
	})
}

// wrapRouterWithAuditMiddleware wraps the router with audit middleware
func wrapRouterWithAuditMiddleware(router chi.Router, client *eeAudit.Client) chi.Router {
	newRouter := chi.NewRouter()
	newRouter.Use(eeAudit.Middleware(client))
	newRouter.Mount("/", router)
	return newRouter
}
