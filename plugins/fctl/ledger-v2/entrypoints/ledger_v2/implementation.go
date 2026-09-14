//go:build fctl_component_guest

package export_formance_fctl_plugin_lifecycle

import (
	"fmt"

	"github.com/formancehq/fctl-v2-poc/pkg/plugin/sdk/portable/component"
	ledgerv2 "github.com/formancehq/ledger/plugins/fctl/ledger-v2"
	ledgercomponent "github.com/formancehq/ledger/plugins/fctl/ledger-v2/component"
)

var lifecycle = mustLifecycle()

func Describe() []byte                                 { return lifecycle.Describe() }
func Start(executionID string, input []byte) [][]byte  { return lifecycle.Start(executionID, input) }
func Resume(executionID string, input []byte) [][]byte { return lifecycle.Resume(executionID, input) }
func Cancel(executionID string) [][]byte               { return lifecycle.Cancel(executionID) }
func Close(executionID string)                         { lifecycle.Close(executionID) }

func mustLifecycle() *component.Component {
	plugin := ledgerv2.Plugin{}
	configured, err := component.NewCommand(plugin, ledgercomponent.Descriptor())
	if err != nil {
		panic(fmt.Sprintf("configure ledger-v2 portable component: %v", err))
	}
	return configured
}
