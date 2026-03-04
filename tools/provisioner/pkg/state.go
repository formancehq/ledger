package provisionner

import . "github.com/formancehq/go-libs/v3/collectionutils"

type ExporterState struct {
	ID     string         `yaml:"id"`
	Config ExporterConfig `yaml:"config"`
}

type LedgerState struct {
	Config LedgerConfig `yaml:"config"`

	// Map the exporter name to the pipeline id
	Exporters map[string]string `yaml:"exporters"`
}

func (state *LedgerState) removeExporterBinding(exporterName string) {
	delete(state.Exporters, exporterName)
	state.Config.Exporters = Filter(state.Config.Exporters, FilterNot(FilterEq(exporterName)))
}

type State struct {
	Ledgers   map[string]*LedgerState   `yaml:"ledgers"`
	Exporters map[string]*ExporterState `yaml:"exporters"`
}

func (s *State) setDefaults() {
	if s.Ledgers == nil {
		s.Ledgers = make(map[string]*LedgerState)
	}
	if s.Exporters == nil {
		s.Exporters = make(map[string]*ExporterState)
	}
}

func (s *State) removeExporter(name string) {
	delete(s.Exporters, name)
	for _, ledger := range s.Ledgers {
		ledger.removeExporterBinding(name)
	}
}

func newState() State {
	return State{
		Ledgers:   make(map[string]*LedgerState),
		Exporters: make(map[string]*ExporterState),
	}
}
