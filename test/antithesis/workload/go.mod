module github.com/formancehq/ledger/test/antithesis

go 1.21

toolchain go1.22.0

replace github.com/formancehq/ledger/pkg/client => ../../../pkg/client

require (
	github.com/alitto/pond v1.8.3
	github.com/antithesishq/antithesis-sdk-go v0.3.8
	github.com/formancehq/formance-sdk-go/v2 v2.1.3
	github.com/formancehq/ledger/pkg/client v0.0.0-00010101000000-000000000000
	github.com/formancehq/stack/libs/go-libs v0.0.0-20240412081813-558ce638a33b
	go.uber.org/atomic v1.10.0
)

require (
	github.com/cenkalti/backoff/v4 v4.3.0 // indirect
	github.com/ericlagergren/decimal v0.0.0-20240411145413-00de7ca16731 // indirect
	github.com/inconshreveable/mousetrap v1.1.0 // indirect
	github.com/spf13/cobra v1.8.1 // indirect
	github.com/spf13/pflag v1.0.5 // indirect
)
