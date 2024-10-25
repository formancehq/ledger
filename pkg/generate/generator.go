package generate

import (
	"github.com/dop251/goja"
	"github.com/google/uuid"
)

type Result struct {
	Script    string            `json:"script"`
	Variables map[string]string `json:"variables"`
}

type Generator struct {
	next func(int) Result
}

func (g *Generator) Next(iteration int) Result {
	return g.next(iteration)
}

func NewGenerator(script string) (*Generator, error) {
	runtime := goja.New()
	_, err := runtime.RunString(script)
	if err != nil {
		return nil, err
	}
	runtime.SetFieldNameMapper(goja.TagFieldNameMapper("json", true))
	err = runtime.Set("uuid", uuid.NewString)
	if err != nil {
		return nil, err
	}

	var next func(int) Result
	err = runtime.ExportTo(runtime.Get("next"), &next)
	if err != nil {
		panic(err)
	}

	return &Generator{
		next: next,
	}, nil
}