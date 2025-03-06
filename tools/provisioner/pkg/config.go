package provisionner

type LedgerCreateConfig struct {
	Bucket   string            `yaml:"bucket"`
	Features map[string]string `yaml:"features"`
	Metadata map[string]string `yaml:"metadata"`
}

type LedgerConfig struct {
	LedgerCreateConfig `yaml:",inline"`
	Connectors         []string `yaml:"connectors"`
}

type ConnectorConfig struct {
	Driver string         `yaml:"driver"`
	Config map[string]any `yaml:"config"`
}

type Config struct {
	Ledgers    map[string]LedgerConfig    `yaml:"ledgers"`
	Connectors map[string]ConnectorConfig `yaml:"connectors"`
}

func (cfg *Config) setDefaults() {
	if cfg.Ledgers == nil {
		cfg.Ledgers = map[string]LedgerConfig{}
	}
	if cfg.Connectors == nil {
		cfg.Connectors = map[string]ConnectorConfig{}
	}
}
