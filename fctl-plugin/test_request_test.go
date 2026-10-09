package ledger

import (
	"encoding/json"
	"strings"

	"github.com/formancehq/fctl/pkg/pluginsdk"
)

// testRequest expresses the previous command fixtures as SDK requests. It only
// separates their tokens; validation and defaults belong to NormalizeRequest.
// File and stdin inputs are supplied as already-read JSON by the calling test.
func testRequest(manifest pluginsdk.Manifest, tokens []string, body string) pluginsdk.ExecuteRequest {
	kinds := make(map[string]string)
	collectFlags(manifest.Root, kinds)
	req := pluginsdk.ExecuteRequest{CommandPath: []string{"ledger"}, Flags: make(map[string]string), ChangedFlags: make(map[string]bool)}
	current := manifest.Root
	for i := 0; i < len(tokens); i++ {
		token := tokens[i]
		if name, ok := strings.CutPrefix(token, "--"); ok {
			flag, value, consumed := testFlag(name, tokens[i+1:], kinds)
			i += consumed
			req.Flags[flag], req.ChangedFlags[flag] = value, true

			continue
		}
		if child, found := testChild(current, token); found && len(req.Args) == 0 {
			current = child
			req.CommandPath = append(req.CommandPath, token)
		} else {
			req.Args = append(req.Args, token)
		}
	}
	req.Body = testBody(req.Flags, body)

	return req
}

func collectFlags(spec pluginsdk.CommandSpec, kinds map[string]string) {
	for _, flag := range spec.Flags {
		kinds[flag.Name] = flag.Type
	}
	for _, child := range spec.Subcommands {
		collectFlags(child, kinds)
	}
}

func testChild(spec pluginsdk.CommandSpec, name string) (pluginsdk.CommandSpec, bool) {
	for _, child := range spec.Subcommands {
		if strings.Fields(child.Use)[0] == name {
			return child, true
		}
	}

	return pluginsdk.CommandSpec{}, false
}

func testFlag(name string, remaining []string, kinds map[string]string) (string, string, int) {
	flag, value, inline := strings.Cut(name, "=")
	if inline {
		return flag, value, 0
	}
	if kinds[flag] != "bool" && len(remaining) > 0 {
		return flag, remaining[0], 1
	}

	return flag, "true", 0
}

func testBody(flags map[string]string, body string) json.RawMessage {
	if body != "" {
		return json.RawMessage(body)
	}
	if data, changed := flags["data"]; changed && !strings.HasPrefix(data, "@") && data != "-" {
		return json.RawMessage(data)
	}

	return nil
}
