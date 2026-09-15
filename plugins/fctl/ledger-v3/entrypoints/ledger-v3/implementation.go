//go:build fctl_component_guest

package export_formance_fctl_plugin_lifecycle

import (
	"fmt"

	"github.com/formancehq/fctl-v2-poc/pkg/plugin/sdk/portable/component"
	ledgerv3 "github.com/formancehq/ledger/v3/plugins/fctl/ledger-v3"
	ledgercomponent "github.com/formancehq/ledger/v3/plugins/fctl/ledger-v3/component"
)

var lifecycle = mustLifecycle()

func Describe() []byte                                 { return lifecycle.Describe() }
func Start(executionID string, input []byte) [][]byte  { return lifecycle.Start(executionID, input) }
func Resume(executionID string, input []byte) [][]byte { return lifecycle.Resume(executionID, input) }
func Cancel(executionID string) [][]byte               { return lifecycle.Cancel(executionID) }
func Close(executionID string)                         { lifecycle.Close(executionID) }

func mustLifecycle() *component.Component {
	plugin := ledgerv3.Plugin{}
	configured, err := component.NewCommand(plugin, ledgercomponent.Descriptor())
	if err != nil {
		panic(fmt.Sprintf("configure Ledger v3 portable component: %v", err))
	}
	return configured
}
