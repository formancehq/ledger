package configtemplate

type Configs map[string]Config

type Config map[string]Parameter

type Parameter struct {
	DataType Type `json:"dataType"`
	Required bool `json:"required"`
}

type TemplateBuilder interface {
	BuildTemplate() (string, Config)
}

func BuildConfigs(builders ...TemplateBuilder) Configs {
	configs := make(map[string]Config)
	for _, builder := range builders {
		name, config := builder.BuildTemplate()
		configs[name] = config
	}

	return configs
}

func NewConfig() Config {
	return make(map[string]Parameter)
}

func (c *Config) AddParameter(name string, dataType Type, required bool) {
	(*c)[name] = Parameter{
		DataType: dataType,
		Required: required,
	}
}
