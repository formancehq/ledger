package node

//go:generate go tool mockgen -typed -write_source_comment=false -write_package_comment=false -destination notifier_generated_test.go -package node github.com/formancehq/ledger/v3/internal/infra/state Notifier
