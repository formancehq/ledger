package provisionner

type LedgerConfig struct {
	Bucket   string            `yaml:"bucket"`
	Features map[string]string `yaml:"features"`
	Metadata map[string]string `yaml:"metadata"`
}

type Config struct {
	Ledgers map[string]LedgerConfig `yaml:"ledgers"`
}

func newState() State {
	return State{
		Ledgers: make(map[string]LedgerConfig),
	}
}
