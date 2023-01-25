package workflow

type Input struct {
	Config    Config            `json:"config"`
	Variables map[string]string `json:"variables"`
}
