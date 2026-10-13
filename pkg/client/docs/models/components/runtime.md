# Runtime

The numscript runtime used to execute the script. Uses "machine" by default, unless the "--experimental-numscript-interpreter" feature flag is passed.

## Example Usage

```go
import (
	"github.com/formancehq/ledger/pkg/client/models/components"
)

value := components.RuntimeExperimentalInterpreter
```


## Values

| Name                             | Value                            |
| -------------------------------- | -------------------------------- |
| `RuntimeExperimentalInterpreter` | experimental-interpreter         |
| `RuntimeMachine`                 | machine                          |