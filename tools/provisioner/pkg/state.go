package provisionner

import . "github.com/formancehq/go-libs/v3/collectionutils"

type ConnectorState struct {
	ID     string          `yaml:"id"`
	Config ConnectorConfig `yaml:"config"`
}

type LedgerState struct {
	Config LedgerConfig `yaml:"config"`

	// Map the connector name to the pipeline id
	Connectors map[string]string `yaml:"connectors"`
}

func (state *LedgerState) removeConnectorBinding(connectorName string) {
	delete(state.Connectors, connectorName)
	state.Config.Connectors = Filter(state.Config.Connectors, FilterNot(FilterEq(connectorName)))
}

type State struct {
	Ledgers    map[string]*LedgerState    `yaml:"ledgers"`
	Connectors map[string]*ConnectorState `yaml:"connectors"`
}

func (s *State) setDefaults() {
	if s.Ledgers == nil {
		s.Ledgers = make(map[string]*LedgerState)
	}
	if s.Connectors == nil {
		s.Connectors = make(map[string]*ConnectorState)
	}
}

func (s *State) removeConnector(name string) {
	delete(s.Connectors, name)
	for _, ledger := range s.Ledgers {
		ledger.removeConnectorBinding(name)
	}
}

func newState() State {
	return State{
		Ledgers:    make(map[string]*LedgerState),
		Connectors: make(map[string]*ConnectorState),
	}
}
