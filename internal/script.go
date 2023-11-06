package ledger

import (
	"fmt"

	"github.com/formancehq/stack/libs/go-libs/metadata"
)

type RunScript struct {
	Script
	Timestamp Time              `json:"timestamp"`
	Metadata  metadata.Metadata `json:"metadata"`
	Reference string            `json:"reference"`
}

type Script struct {
	Plain string            `json:"plain"`
	Vars  map[string]string `json:"vars" swaggertype:"object"`
}

type ScriptV1 struct {
	Script
	Vars map[string]any `json:"vars"`
}

func (s ScriptV1) ToCore() Script {
	s.Script.Vars = map[string]string{}
	for k, v := range s.Vars {
		switch v := v.(type) {
		case string:
			s.Script.Vars[k] = v
		case map[string]any:
			s.Script.Vars[k] = fmt.Sprintf("%s %v", v["asset"], v["amount"])
		default:
			s.Script.Vars[k] = fmt.Sprint(v)
		}
	}
	return s.Script
}
