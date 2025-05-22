package provisionner

type LedgerCreateConfig struct {
	Bucket   string            `yaml:"bucket"`
	Features map[string]string `yaml:"features"`
	Metadata map[string]string `yaml:"metadata"`
}

type LedgerConfig struct {
	LedgerCreateConfig `yaml:",inline"`
	Exporters          []string `yaml:"exporters"`
}

type ExporterConfig struct {
	Driver string         `yaml:"driver"`
	Config map[string]any `yaml:"config"`
}

type Config struct {
	Ledgers   map[string]LedgerConfig   `yaml:"ledgers"`
	Exporters map[string]ExporterConfig `yaml:"exporters"`
}

func (cfg *Config) setDefaults() {
	if cfg.Ledgers == nil {
		cfg.Ledgers = map[string]LedgerConfig{}
	}
	if cfg.Exporters == nil {
		cfg.Exporters = map[string]ExporterConfig{}
	}
}
