package drivers

type ServiceConfig struct {
	Stack string
	Debug bool
}

func NewServiceConfig(stack string, debug bool) ServiceConfig {
	return ServiceConfig{
		Stack: stack,
		Debug: debug,
	}
}
