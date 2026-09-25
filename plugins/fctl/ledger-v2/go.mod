module github.com/formancehq/ledger/plugins/fctl/ledger-v2

go 1.26.0

require (
	github.com/formancehq/fctl-v2-poc/pkg/plugin v0.0.0
	github.com/formancehq/ledger/pkg/client v0.0.0
	google.golang.org/protobuf v1.36.12
	gopkg.in/yaml.v3 v3.0.1
)

require golang.org/x/sync v0.8.0 // indirect

replace github.com/formancehq/ledger/pkg/client => ../../../pkg/client
