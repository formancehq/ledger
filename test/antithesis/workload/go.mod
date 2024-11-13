module github.com/formancehq/ledger/test/antithesis

go 1.22.0

toolchain go1.23.2

replace github.com/formancehq/ledger/pkg/client => ../../../pkg/client

require (
	github.com/alitto/pond v1.8.3
	github.com/antithesishq/antithesis-sdk-go v0.4.2
	github.com/formancehq/go-libs/v2 v2.0.1-0.20241114125605-4a3e447246a9
	github.com/formancehq/ledger/pkg/client v0.0.0-00010101000000-000000000000
	go.uber.org/atomic v1.10.0
)

require (
	github.com/bahlo/generic-list-go v0.2.0 // indirect
	github.com/buger/jsonparser v1.1.1 // indirect
	github.com/cenkalti/backoff/v4 v4.3.0 // indirect
	github.com/ericlagergren/decimal v0.0.0-20240411145413-00de7ca16731 // indirect
	github.com/invopop/jsonschema v0.12.0 // indirect
	github.com/mailru/easyjson v0.7.7 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/wk8/go-ordered-map/v2 v2.1.8 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)
